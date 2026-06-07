from asyncio import (
    CancelledError,
    Queue,
    create_task,
    ensure_future,
    gather,
    sleep,
    wait,
    wait_for,
    TimeoutError as AsyncTimeoutError,
    Event,
    FIRST_COMPLETED,
)
from datetime import datetime
from mimetypes import guess_extension
from os import path as ospath, remove as os_remove, sendfile as os_sendfile, fstat
from pathlib import Path
from re import sub
from sys import argv
from time import time

from aiofiles import open as aiopen
from aiofiles.os import makedirs
from aioshutil import move
from pyrogram import StopTransmission, raw, utils
from pyrogram.errors import (
    AuthBytesInvalid,
    FloodWait,
    FloodPremiumWait,
    FileMigrate,
    FileReferenceExpired,
    FileReferenceInvalid,
    PeerIdInvalid,
    ChannelInvalid,
)
from pyrogram.file_id import PHOTO_TYPES, FileId, FileType, ThumbnailSource
from pyrogram.session import Auth, Session
from pyrogram.session.internals import MsgId

from ... import LOGGER
from ...core.config_manager import Config
from ...core.tg_client import TgClient

CHUNK_SIZE = 1024 * 1024
WRITE_BUF = 32 * 1024 * 1024


def _pick_clients(wl, num, count):
    return sorted(range(num), key=lambda i: wl.get(i, 0))[:count]


class HyperTGDownload:

    def __init__(self):
        self.clients = TgClient.helper_bots
        self.work_loads = TgClient.helper_loads
        self.num_clients = len(self.clients)
        self.num_parts = Config.HYPER_THREADS or max(8, self.num_clients)
        self.pipeline_depth = getattr(Config, "HYPER_PIPELINE", 1)
        self.message = None
        self.dump_chat = None
        self.directory = None
        self.file_name = ""
        self.file_size = 0
        self.download_dir = "downloads/"
        self._ref_cache = {}
        self._sessions = {}
        self._cancel = Event()
        self._tasks = []
        self._prog_task = None
        self._processed_bytes = 0
        self._write_buf = getattr(Config, "HYPER_WRITE_BUFFER", WRITE_BUF)

    @staticmethod
    def _media_of(message):
        for attr in ("audio", "document", "photo", "sticker", "animation",
                      "video", "voice", "video_note", "new_chat_photo"):
            if m := getattr(message, attr, None):
                return m
        raise ValueError("No downloadable media")

    async def _fetch_ref(self, idx, client):
        msg = await client.get_messages(self.dump_chat, self.message.id)
        fid = FileId.decode(getattr(await self._media_of(msg), "file_id", ""))
        self._ref_cache[idx] = fid
        return fid

    async def _mk_session(self, client, dc_id):
        tm = await client.storage.test_mode()
        if dc_id != await client.storage.dc_id():
            ak = await Auth(client, dc_id, tm).create()
            s = Session(client, dc_id, ak, tm, is_media=True)
            await s.start()
            for _ in range(6):
                try:
                    e = await client.invoke(raw.functions.auth.ExportAuthorization(dc_id=dc_id))
                    await s.invoke(raw.functions.auth.ImportAuthorization(id=e.id, bytes=e.bytes))
                    return s
                except AuthBytesInvalid:
                    await sleep(1)
            await s.stop()
            raise AuthBytesInvalid
        ak = await client.storage.auth_key()
        s = Session(client, dc_id, ak, tm, is_media=True)
        await s.start()
        return s

    async def _get_session(self, idx, dc_id, force=False):
        s = self._sessions.get(idx)
        if s and not force:
            if s.is_connected and s.dc_id == dc_id:
                return s
            try:
                await s.stop()
            except Exception:
                pass
        s = await self._mk_session(self.clients[idx], dc_id)
        self._sessions[idx] = s
        return s

    async def _warmup(self, indices, dc_id):
        async def _w(i):
            try:
                await self._get_session(i, dc_id)
            except Exception as e:
                LOGGER.warning(f"HyperDL warmup fail client {i}: {e}")
        await gather(*[_w(i) for i in indices])

    async def _close_all(self):
        for s in self._sessions.values():
            try:
                if s.is_connected:
                    await s.stop()
            except Exception:
                pass
        self._sessions.clear()

    @staticmethod
    def _location(fid):
        ft = fid.file_type
        if ft == FileType.CHAT_PHOTO:
            if fid.chat_id > 0:
                peer = raw.types.InputPeerUser(user_id=fid.chat_id, access_hash=fid.chat_access_hash)
            elif fid.chat_access_hash == 0:
                peer = raw.types.InputPeerChat(chat_id=-fid.chat_id)
            else:
                peer = raw.types.InputPeerChannel(
                    channel_id=utils.get_channel_id(fid.chat_id), access_hash=fid.chat_access_hash
                )
            return raw.types.InputPeerPhotoFileLocation(
                peer=peer, volume_id=fid.volume_id, local_id=fid.local_id,
                big=fid.thumbnail_source == ThumbnailSource.CHAT_PHOTO_BIG,
            )
        if ft == FileType.PHOTO:
            return raw.types.InputPhotoFileLocation(
                id=fid.media_id, access_hash=fid.access_hash,
                file_reference=fid.file_reference, thumb_size=fid.thumbnail_size,
            )
        return raw.types.InputDocumentFileLocation(
            id=fid.media_id, access_hash=fid.access_hash,
            file_reference=fid.file_reference, thumb_size=fid.thumbnail_size,
        )

    async def _do_req(self, sess, location, off, attempt=0):
        try:
            r = await wait_for(
                sess.invoke(raw.functions.upload.GetFile(
                    precise=True, cdn_supported=False,
                    location=location, offset=off, limit=CHUNK_SIZE,
                )),
                timeout=30,
            )
            if isinstance(r, raw.types.upload.File):
                return r.bytes
            raise ValueError(f"Unexpected response type: {type(r)}")
        except FileMigrate as e:
            dc = e.value if hasattr(e, "value") else int(str(e).split()[-1])
            if attempt < 3:
                return None, dc
            raise
        except (FileReferenceExpired, FileReferenceInvalid):
            if attempt < 3:
                return None, -1
            raise

    async def _pipeline_fetch(self, idx, location, start, end, fid, queue):
        sess = await self._get_session(idx, fid.dc_id)
        loc = location
        first_chunk_off = start - (start % CHUNK_SIZE)
        first_trim = start - first_chunk_off
        last_byte = end - 1
        window = self.pipeline_depth
        inflight = set()
        cur_off = first_chunk_off
        seq = 0

        async def _req(off, s):
            nonlocal sess, loc
            for attempt in range(3):
                try:
                    result = await self._do_req(sess, loc, off, attempt)
                    if isinstance(result, tuple):
                        _, dc_or_ref = result
                        if dc_or_ref == -1:
                            fid_new = await self._fetch_ref(idx, self.clients[idx])
                            loc = self._location(fid_new)
                        else:
                            sess = await self._get_session(idx, dc_or_ref, force=True)
                        continue
                    return s, off, result
                except (FloodWait, FloodPremiumWait) as e:
                    await sleep(e.value + 1 if hasattr(e, "value") else 5)
                except CancelledError:
                    raise
            raise RuntimeError(f"Failed after 3 attempts at offset {off}")

        try:
            while cur_off <= last_byte or inflight:
                while len(inflight) < window and cur_off <= last_byte:
                    if self._cancel.is_set():
                        raise CancelledError
                    inflight.add(ensure_future(_req(cur_off, seq)))
                    cur_off += CHUNK_SIZE
                    seq += 1
                if not inflight:
                    break
                done_set, inflight = await wait(inflight, return_when=FIRST_COMPLETED)
                for f in done_set:
                    s, roff, chunk = f.result()
                    if not chunk:
                        continue
                    clen = len(chunk)
                    if roff == first_chunk_off and roff + CHUNK_SIZE >= end:
                        chunk = chunk[first_trim:last_byte - roff + 1]
                    elif roff == first_chunk_off:
                        chunk = chunk[first_trim:]
                    elif roff + CHUNK_SIZE > end:
                        chunk = chunk[:end - roff]
                    await queue.put(chunk)
                    self._processed_bytes += len(chunk)
        except CancelledError:
            raise
        except Exception as e:
            LOGGER.error(f"HyperDL pipeline err: {e}")
            raise
        finally:
            for f in inflight:
                if not f.done():
                    f.cancel()

    async def _part(self, start, end, pi, ci, fid):
        ppath = ospath.join(self.directory, f"{self.file_name}.p{pi:02d}")
        q = Queue(maxsize=self.pipeline_depth + 1)
        error_holder = [None]

        async def _producer():
            try:
                await self._pipeline_fetch(ci, self._location(fid), start, end, fid, q)
            except CancelledError:
                pass
            except Exception as e:
                error_holder[0] = e
            finally:
                await q.put(None)

        prod = ensure_future(_producer())
        buf = bytearray()
        try:
            async with aiopen(ppath, "wb") as f:
                while True:
                    chunk = await q.get()
                    if chunk is None:
                        break
                    buf.extend(chunk)
                    if len(buf) >= self._write_buf:
                        await f.write(buf)
                        buf = bytearray()
                if buf:
                    await f.write(buf)
            if error_holder[0] is not None:
                raise error_holder[0]
        except CancelledError:
            prod.cancel()
            raise
        except Exception:
            prod.cancel()
            raise
        finally:
            if not prod.done():
                prod.cancel()
        return pi, ppath

    async def _assemble(self, parts, dest):
        ordered = [p for _, p in sorted(parts)]
        try:
            with open(dest, "wb") as dst:
                for part in ordered:
                    with open(part, "rb") as src:
                        sfd, dfd = src.fileno(), dst.fileno()
                        st = fstat(sfd)
                        off = 0
                        while off < st.st_size:
                            sent = os_sendfile(dfd, sfd, off, st.st_size - off)
                            if sent == 0:
                                break
                            off += sent
        except (AttributeError, OSError, ValueError):
            async with aiopen(dest, "wb") as dst:
                for part in ordered:
                    async with aiopen(part, "rb") as src:
                        while True:
                            c = await src.read(8 * 1024 * 1024)
                            if not c:
                                break
                            await dst.write(c)
        for part in ordered:
            try:
                os_remove(part)
            except Exception:
                pass
        return dest

    async def _progress(self, cb, args):
        if not cb:
            return
        last = 0
        while not self._cancel.is_set():
            try:
                cur = self._processed_bytes
                if cur != last:
                    await cb(cur, self.file_size, *args)
                    last = cur
                await sleep(1)
            except (CancelledError, StopTransmission):
                break
            except Exception:
                await sleep(1)

    def _drop_parts(self, n):
        for i in range(n):
            try:
                os_remove(ospath.join(self.directory, f"{self.file_name}.p{i:02d}"))
            except Exception:
                pass

    async def handle_download(self, progress, progress_args):
        self._cancel.clear()
        self._processed_bytes = 0
        await makedirs(self.directory, exist_ok=True)
        final = ospath.abspath(sub("\\\\", "/", ospath.join(self.directory, self.file_name)))

        n_use = min(self.num_parts, self.num_clients)
        cidx = _pick_clients(self.work_loads, self.num_clients, n_use)

        min_part = 10 * 1024 * 1024
        n_parts = min(n_use, max(1, self.file_size // min_part)) if self.file_size >= min_part else 1
        psz = self.file_size // n_parts if n_parts > 0 else self.file_size
        ranges = [(i * psz, min((i + 1) * psz, self.file_size)) for i in range(n_parts)]
        assigns = [cidx[i % n_use] for i in range(n_parts)]

        try:
            fid = await self._fetch_ref(cidx[0], self.clients[cidx[0]])
        except Exception as e:
            LOGGER.error(f"HyperDL ref fail: {e}")
            return None

        try:
            await self._warmup(range(n_parts), fid.dc_id)
        except Exception as e:
            LOGGER.warning(f"HyperDL warmup err: {e}")

        self._tasks = []
        self._prog_task = None

        try:
            for i, (s, e) in enumerate(ranges):
                self._tasks.append(create_task(self._part(s, e, i, assigns[i], fid)))
            if progress:
                self._prog_task = create_task(self._progress(progress, progress_args))
            parts = list(await gather(*self._tasks))
            tmp = final + ".parts"
            await self._assemble(parts, tmp)
            await move(tmp, final)
            LOGGER.info(
                f"HyperDL done {self.file_name} "
                f"({self.file_size / 1048576:.1f}MB {n_parts}p {n_use}c pipe={self.pipeline_depth})"
            )
            return final
        except FloodWait:
            raise
        except (CancelledError, StopTransmission):
            return None
        except Exception as e:
            LOGGER.error(f"HyperDL: {e}")
            return None
        finally:
            self._cancel.set()
            if self._prog_task and not self._prog_task.done():
                self._prog_task.cancel()
            for t in self._tasks:
                if not t.done():
                    t.cancel()
            self._drop_parts(len(ranges))
            await self._close_all()

    async def download_media(self, message, file_name="downloads/",
                             progress=None, progress_args=(), dump_chat=None):
        try:
            if dump_chat:
                try:
                    self.message = await TgClient.bot.copy_message(
                        chat_id=dump_chat, from_chat_id=message.chat.id,
                        message_id=message.id, disable_notification=True,
                    )
                except (PeerIdInvalid, ChannelInvalid) as e:
                    LOGGER.warning(f"HyperDL dump copy fail: {e}")
                    dump_chat = None
            self.dump_chat = dump_chat or message.chat.id
            self.message = self.message or message
            media = await self._media_of(self.message)
            fid_str = media if isinstance(media, str) else media.file_id
            fid_obj = FileId.decode(fid_str)
            ftype = fid_obj.file_type
            mname = getattr(media, "file_name", "")
            self.file_size = getattr(media, "file_size", 0)
            mime = getattr(media, "mime_type", "image/jpeg")
            dt = getattr(media, "date", None)
            self.directory, self.file_name = ospath.split(file_name)
            self.file_name = self.file_name or mname or ""
            if not ospath.isabs(self.file_name):
                self.directory = Path(argv[0]).parent / (self.directory or self.download_dir)
            if not self.file_name:
                ext = self._ext(ftype, mime)
                self.file_name = f"{FileType(ftype).name.lower()}_{(dt or datetime.now()).strftime('%Y-%m-%d_%H-%M-%S')}_{MsgId()}{ext}"
            return await self.handle_download(progress, progress_args)
        except Exception as e:
            LOGGER.error(f"HyperDL download_media: {e}")
            raise

    @staticmethod
    def _ext(ft, mime):
        if ft in PHOTO_TYPES:
            return ".jpg"
        if mime:
            e = guess_extension(mime)
            if e:
                return e
        return {
            FileType.VOICE: ".ogg", FileType.VIDEO: ".mp4",
            FileType.ANIMATION: ".mp4", FileType.VIDEO_NOTE: ".mp4",
            FileType.AUDIO: ".mp3", FileType.STICKER: ".webp",
        }.get(ft, ".bin")

    async def cancel(self):
        self._cancel.set()
        for t in self._tasks:
            if not t.done():
                t.cancel()
        if self._prog_task and not self._prog_task.done():
            self._prog_task.cancel()
        await self._close_all()
