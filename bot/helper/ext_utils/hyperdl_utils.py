import os
from asyncio import (
    FIRST_COMPLETED,
    CancelledError,
    Event,
    Queue,
    create_task,
    ensure_future,
    gather,
    sleep,
    to_thread,
    wait,
    wait_for,
)
from datetime import datetime
from mimetypes import guess_extension
from pathlib import Path
from re import sub
from sys import argv
from time import time

from aiofiles.os import makedirs
from pyrogram import StopTransmission, raw, utils
from pyrogram.errors import (
    AuthBytesInvalid,
    FileMigrate,
    FileReferenceExpired,
    FileReferenceInvalid,
    FloodPremiumWait,
    FloodWait,
)
from pyrogram.file_id import PHOTO_TYPES, FileId, FileType, ThumbnailSource
from pyrogram.session import Auth, Session
from pyrogram.session.internals import MsgId

from ... import LOGGER
from ...core.config_manager import Config
from ...core.tg_client import TgClient

def _chunk_size():
    return max(getattr(Config, "HYPER_CHUNK", 256 * 1024), 64 * 1024)


def _pick_clients(wl, clients, count):
    keys = list(clients.keys())
    return sorted(keys, key=lambda i: wl.get(i, 0))[:count]


class HyperTGDownload:

    def __init__(self):
        self.clients = TgClient.helper_bots
        self.work_loads = TgClient.helper_loads
        self.num_clients = len(self.clients)
        self.num_parts = Config.HYPER_THREADS or max(8, self.num_clients)
        self.pipeline_depth = max(getattr(Config, "HYPER_PIPELINE", 32), 16)
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

    @staticmethod
    def _media_of(message):
        for attr in ("audio", "document", "photo", "sticker", "animation",
                      "video", "voice", "video_note", "new_chat_photo"):
            if m := getattr(message, attr, None):
                return m
        raise ValueError("No downloadable media")

    async def _fetch_ref(self, idx, client, max_retries=3):
        last_error = None
        for attempt in range(max_retries):
            try:
                msg = await client.get_messages(self.dump_chat, self.message.id)
                if msg is None:
                    raise ValueError(
                        f"msg {self.message.id} not found in {self.dump_chat}"
                    )
                media = self._media_of(msg)
                fid_str = getattr(media, "file_id", None)
                if not fid_str:
                    raise ValueError(
                        f"no file_id in media from msg {self.message.id}"
                    )
                fid = FileId.decode(fid_str)
                self._ref_cache[idx] = fid
                return fid
            except Exception as e:
                last_error = e
                LOGGER.warning(
                    f"HyperDL _fetch_ref attempt {attempt + 1}/{max_retries} "
                    f"fail: {e} (client={client.me.username} "
                    f"chat={self.dump_chat} msg={self.message.id})"
                )
                if attempt < max_retries - 1:
                    await sleep(1 * (attempt + 1))
        raise ValueError(
            f"Failed to get file ref from {self.dump_chat} msg "
            f"{self.message.id} with {client.me.username}: {last_error}"
        )

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
        sessions = list(self._sessions.values())
        self._sessions.clear()
        for s in sessions:
            try:
                if s.is_connected:
                    await s.stop()
            except Exception:
                pass

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
                    location=location, offset=off, limit=_chunk_size(),
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
        first_chunk_off = start - (start % _chunk_size())
        first_trim = start - first_chunk_off
        last_byte = end - 1
        window = self.pipeline_depth
        min_window = 2
        max_window = window * 4
        inflight = set()
        cur_off = first_chunk_off
        seq = 0
        consecutive_ok = 0
        flood_count = 0

        async def _req(off, s):
            nonlocal sess, loc, window, consecutive_ok, flood_count
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
                    flood_count += 1
                    wait_val = e.value if hasattr(e, "value") else 5
                    if wait_val > 10 or flood_count >= 3:
                        window = max(min_window, window - max(1, window // 4))
                        consecutive_ok = 0
                        flood_count = 0
                    await sleep(wait_val + 1)
                except CancelledError:
                    raise
            raise RuntimeError(f"Failed after 3 attempts at offset {off}")

        try:
            while cur_off <= last_byte or inflight:
                while len(inflight) < window and cur_off <= last_byte:
                    if self._cancel.is_set():
                        raise CancelledError
                    inflight.add(ensure_future(_req(cur_off, seq)))
                    cur_off += _chunk_size()
                    seq += 1
                if not inflight:
                    break
                done_set, inflight = await wait(inflight, return_when=FIRST_COMPLETED)
                consecutive_ok += len(done_set)
                if consecutive_ok >= window:
                    window = min(window + 2, max_window)
                    consecutive_ok = 0
                for f in done_set:
                    s, roff, chunk = f.result()
                    if not chunk:
                        continue
                    if roff == first_chunk_off and roff + _chunk_size() >= end:
                        chunk = chunk[first_trim:last_byte - roff + 1]
                    elif roff == first_chunk_off:
                        chunk = chunk[first_trim:]
                    elif roff + _chunk_size() > end:
                        chunk = chunk[:end - roff]
                    await queue.put((roff, chunk))
        except CancelledError:
            raise
        except Exception as e:
            if "FloodWait" in type(e).__name__ or "Flood" in str(type(e).__name__):
                window = max(min_window, window // 2)
                consecutive_ok = 0
            LOGGER.error(f"HyperDL pipeline err: {e}")
            raise
        finally:
            for f in inflight:
                if not f.done():
                    f.cancel()

    async def _part(self, start, end, final_path, ci, fid):
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

        async def _consumer():
            fd = await to_thread(os.open, final_path, os.O_WRONLY)
            try:
                while True:
                    item = await q.get()
                    if item is None:
                        break
                    roff, chunk = item
                    await self._async_pwrite(fd, chunk, roff)
                    self._processed_bytes += len(chunk)
            finally:
                await to_thread(os.close, fd)

        prod = ensure_future(_producer())
        try:
            await _consumer()
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

    @staticmethod
    async def _async_pwrite(fd, data, offset):
        total = len(data)
        written = 0
        while written < total:
            n = await to_thread(os.pwrite, fd, data[written:], offset + written)
            if n == 0:
                raise OSError(f"pwrite returned 0 at offset {offset + written}")
            written += n

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

    async def handle_download(self, progress, progress_args):
        self._cancel.clear()
        self._processed_bytes = 0
        dl_start = time()
        await makedirs(self.directory, exist_ok=True)
        final = os.path.abspath(sub("\\\\", "/", os.path.join(self.directory, self.file_name)))

        n_use = min(self.num_parts, self.num_clients)
        cidx = _pick_clients(self.work_loads, self.clients, n_use)

        min_part = 1 * 1024 * 1024
        n_parts = min(n_use, max(1, self.file_size // min_part)) if self.file_size >= min_part else 1
        psz = self.file_size // n_parts if n_parts > 0 else self.file_size
        ranges = [(i * psz, min((i + 1) * psz, self.file_size)) for i in range(n_parts)]
        assigns = [cidx[i % n_use] for i in range(n_parts)]

        unique_clients = set(assigns)
        fid_map = {}
        try:
            for ci in unique_clients:
                fid_map[ci] = await self._fetch_ref(ci, self.clients[ci])
        except Exception as e:
            LOGGER.error(f"HyperDL ref fail: {e}")
            return None

        first_fid = fid_map[assigns[0]]
        try:
            await self._warmup(unique_clients, first_fid.dc_id)
        except Exception as e:
            LOGGER.warning(f"HyperDL warmup err: {e}")

        self._tasks = []
        self._prog_task = None

        try:
            fd = await to_thread(os.open, final, os.O_WRONLY | os.O_CREAT)
            try:
                await to_thread(os.ftruncate, fd, self.file_size)
            finally:
                await to_thread(os.close, fd)

            for i, (s, e) in enumerate(ranges):
                self._tasks.append(
                    create_task(self._part(s, e, final, assigns[i], fid_map[assigns[i]]))
                )
            if progress:
                self._prog_task = create_task(self._progress(progress, progress_args))
            await gather(*self._tasks)
            dl_elapsed = time() - dl_start
            dl_speed = self.file_size / dl_elapsed / 1048576 if dl_elapsed > 0 else 0
            wl_str = " ".join(f"c{k}:{v}" for k, v in sorted(self.work_loads.items()))
            LOGGER.info(
                f"HyperDL done {self.file_name} "
                f"({self.file_size / 1048576:.1f}MB {n_parts}p {n_use}c "
                f"pipe={self.pipeline_depth} wl=[{wl_str}] {dl_speed:.1f}MB/s)"
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
            if self._tasks:
                await gather(*self._tasks, return_exceptions=True)
            await self._close_all()

    async def download_media(self, message, file_name="downloads/",
                             progress=None, progress_args=(), dump_chat=None):
        try:
            if dump_chat and not isinstance(dump_chat, int):
                try:
                    dump_chat = int(dump_chat)
                except (ValueError, TypeError):
                    dump_chat = None
            if dump_chat:
                try:
                    self.message = await TgClient.bot.copy_message(
                        chat_id=dump_chat, from_chat_id=message.chat.id,
                        message_id=message.id, disable_notification=True,
                    )
                except Exception as e:
                    LOGGER.warning(
                        f"HyperDL copy fail: {e} "
                        f"(from={message.chat.id} to={dump_chat})"
                    )
                    raise RuntimeError(
                        f"Cannot copy to dump chat: {e}"
                    ) from e
            self.dump_chat = dump_chat or message.chat.id
            self.message = self.message or message
            LOGGER.info(
                f"HyperDL init dump={self.dump_chat} "
                f"msg_id={self.message.id} msg_type={type(self.message).__name__}"
            )
            media = self._media_of(self.message)
            fid_str = media if isinstance(media, str) else media.file_id
            fid_obj = FileId.decode(fid_str)
            ftype = fid_obj.file_type
            mname = getattr(media, "file_name", "")
            self.file_size = getattr(media, "file_size", 0)
            mime = getattr(media, "mime_type", "image/jpeg")
            dt = getattr(media, "date", None)
            self.directory, self.file_name = os.path.split(file_name)
            self.file_name = self.file_name or mname or ""
            if not os.path.isabs(self.file_name):
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
        if self._tasks:
            await gather(*self._tasks, return_exceptions=True)
        await self._close_all()
