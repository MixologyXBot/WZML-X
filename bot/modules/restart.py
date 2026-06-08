from asyncio import create_subprocess_exec, gather
from datetime import datetime
from os import execl as osexecl
from sys import executable

from aiofiles import open as aiopen
from aiofiles.os import path as aiopath, remove
from pytz import timezone
from pyrogram.enums import ButtonStyle

from bot.version import get_version

from .. import LOGGER, intervals, sabnzbd_client, scheduler
from ..core.config_manager import Config, BinConfig
from ..core.jdownloader_booter import jdownloader
from ..core.tg_client import TgClient
from ..core.torrent_manager import TorrentManager
from ..helper.ext_utils.bot_utils import new_task, resolve_command
from ..helper.ext_utils.db_handler import database
from ..helper.ext_utils.files_utils import clean_all
from ..helper.listeners.mega_listener import mega_cleanup
from ..helper.telegram_helper import button_build
from ..helper.telegram_helper.message_utils import (
    delete_message,
    send_message,
)


@new_task
async def restart_bot(_, message):
    buttons = button_build.ButtonMaker()
    buttons.data_button("Yes!", "botrestart confirm", style=ButtonStyle.SUCCESS)
    buttons.data_button("No!", "botrestart cancel", style=ButtonStyle.DANGER)
    button = buttons.build_menu(2)
    await send_message(
        message, "<i>Are you really sure you want to restart the bot ?</i>", button
    )


@new_task
async def restart_sessions(_, message):
    buttons = button_build.ButtonMaker()
    buttons.data_button("Yes!", "sessionrestart confirm", style=ButtonStyle.SUCCESS)
    buttons.data_button("No!", "sessionrestart cancel", style=ButtonStyle.DANGER)
    button = buttons.build_menu(2)
    await send_message(
        message,
        "<i>Are you really sure you want to restart the session(s) ?!</i>",
        button,
    )


def _restart_header(now, is_restart_chat=False):
    title = "Restarted Successfully!" if is_restart_chat else "Bot Restarted!"
    return (
        f"⌬ <b><i>{title}</i></b>\n"
        f"┟ <b>Date:</b> {now.strftime('%d/%m/%y')}\n"
        f"┠ <b>Time:</b> {now.strftime('%I:%M:%S %p')}\n"
        f"┠ <b>TimeZone:</b> {Config.TIMEZONE}\n"
        f"┠ <b>Branch:</b> {Config.UPSTREAM_BRANCH}\n"
        f"┖ <b>Version:</b> {get_version()}"
    )


async def _send_msg(cid, msg):
    try:
        await TgClient.bot.send_message(
            chat_id=cid,
            text=msg,
            disable_web_page_preview=True,
            disable_notification=True,
        )
    except Exception as e:
        LOGGER.error(e)


async def restart_notification():
    if await aiopath.isfile(".restartmsg"):
        with open(".restartmsg") as f:
            chat_id, msg_id = map(int, f)
    else:
        chat_id, msg_id = 0, 0

    now = datetime.now(timezone(Config.TIMEZONE))

    if Config.DATABASE_URL and (Config.INC_TASK_NOTIFY or Config.INC_TASK_RESUME):
        if notifier_dict := await database.get_incomplete_tasks():
            if Config.INC_TASK_RESUME:
                await _resume_tasks(notifier_dict)
            if Config.INC_TASK_NOTIFY:
                await _notify_tasks(notifier_dict, chat_id, now)

    if await aiopath.isfile(".restartmsg"):
        try:
            await TgClient.bot.edit_message_text(
                chat_id=chat_id,
                message_id=msg_id,
                text=_restart_header(now, is_restart_chat=True),
                disable_web_page_preview=True,
            )
        except Exception as e:
            LOGGER.error(e)
        await remove(".restartmsg")


async def _notify_tasks(notifier_dict, restart_chat_id, now):
    for cid, data in notifier_dict.items():
        is_restart_chat = cid == restart_chat_id
        header = _restart_header(now, is_restart_chat)
        msg = header + "\n\n⌬ <b><i>Incomplete Tasks!</i></b>"
        for tag, tasks in data.items():
            entry = f"\n➲ <b>User:</b> {tag}\n┖ <b>Tasks:</b>"
            for index, task in enumerate(tasks, start=1):
                link = task.get("link", "")
                entry += f" {index}. <a href='{link}'>L</a> |"
            if len((msg + entry).encode()) > 4000:
                await _send_msg(cid, msg)
                msg = header
            msg += entry
        if msg:
            await _send_msg(cid, msg)


async def _resume_tasks(notifier_dict):
    for cid, data in notifier_dict.items():
        for tag, tasks in data.items():
            for task in tasks:
                command = task.get("command", "")
                user_id = task.get("user_id", 0)
                if not command or not user_id:
                    continue
                try:
                    user = await TgClient.bot.get_users(user_id)
                except Exception as e:
                    LOGGER.warning(f"Resume: cannot get user {user_id}: {e}")
                    continue
                handler = resolve_command(command)
                if handler is None:
                    continue
                try:
                    msg = await TgClient.bot.send_message(
                        chat_id=cid,
                        text=command,
                        disable_notification=True,
                    )
                    msg.text = command
                    msg.from_user = user
                    await handler(TgClient.bot, msg)
                    await delete_message(msg)
                except Exception as e:
                    LOGGER.error(f"Resume: failed for '{command}' in {cid}: {e}")


@new_task
async def confirm_restart(_, query):
    await query.answer()
    data = query.data.split()
    message = query.message
    reply_to = message.reply_to_message
    await delete_message(message)
    if data[1] == "confirm":
        intervals["stopAll"] = True
        restart_message = await send_message(reply_to, "<i>Restarting...</i>")
        await delete_message(message)
        await TgClient.stop()
        if scheduler.running:
            scheduler.shutdown(wait=False)
        if qb := intervals["qb"]:
            qb.cancel()
        if jd := intervals["jd"]:
            jd.cancel()
        if nzb := intervals["nzb"]:
            nzb.cancel()
        if st := intervals["status"]:
            for intvl in list(st.values()):
                intvl.cancel()
        await mega_cleanup()
        await clean_all()
        await TorrentManager.close_all()
        if sabnzbd_client.LOGGED_IN:
            await gather(
                sabnzbd_client.pause_all(),
                sabnzbd_client.delete_job("all", True),
                sabnzbd_client.purge_all(True),
                sabnzbd_client.delete_history("all", delete_files=True),
            )
            await sabnzbd_client.close()
        if jdownloader.is_connected:
            await gather(
                jdownloader.device.downloadcontroller.stop_downloads(),
                jdownloader.device.linkgrabber.clear_list(),
                jdownloader.device.downloads.cleanup(
                    "DELETE_ALL",
                    "REMOVE_LINKS_AND_DELETE_FILES",
                    "ALL",
                ),
            )
            await jdownloader.close()
        proc1 = await create_subprocess_exec(
            "pkill",
            "-9",
            "-f",
            f"gunicorn|{BinConfig.ARIA2_NAME}|{BinConfig.QBIT_NAME}|{BinConfig.FFMPEG_NAME}|{BinConfig.RCLONE_NAME}|java|{BinConfig.SABNZBD_NAME}|7z|split",
        )
        proc2 = await create_subprocess_exec("python3", "update.py")
        await gather(proc1.wait(), proc2.wait())
        async with aiopen(".restartmsg", "w") as f:
            await f.write(f"{restart_message.chat.id}\n{restart_message.id}\n")
        osexecl(executable, executable, "-m", "bot")
    else:
        await delete_message(message, reply_to)
