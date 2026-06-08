from asyncio import Event, wait_for, TimeoutError as AsyncTimeout
from os.path import exists as path_exists

from aiofiles.os import remove as aioremove
from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.filters import create, user, text, private
from pyrogram.handlers import CallbackQueryHandler, MessageHandler
from pyrogram.errors import (
    SessionPasswordNeeded,
    FloodWait,
    PhoneNumberInvalid,
    ApiIdInvalid,
    PhoneCodeInvalid,
    PhoneCodeExpired,
)

from ..core.tg_client import TgClient
from ..helper.ext_utils.bot_utils import new_task
from ..helper.telegram_helper.button_build import ButtonMaker
from ..helper.telegram_helper.message_utils import send_message, edit_message, delete_message

_STOP = "gensess_stop"
_TIMEOUT = 120


def _stop_filter(uid):
    async def _check(_, __, update):
        return update.data == _STOP and update.from_user.id == uid
    return create(_check)


async def _safe_disconnect(client):
    try:
        await client.disconnect()
    except ConnectionError:
        pass


def _timeout_str(secs):
    m, s = divmod(int(secs), 60)
    return f"{m}m {s}s" if m else f"{s}s"


def _stop_btns():
    btns = ButtonMaker()
    btns.data_button("Stop Process", data=_STOP)
    return btns.build_menu(1)


def _build_header(user_name):
    return (
        "⌬ <u><i><b>Pyrogram String Session Generator</b></i></u>\n\n"
        f"│ Hello <b>{user_name}</b>!"
    )


def _build_lines(api_id=None, api_hash=None, phone=None):
    lines = []
    if api_id is not None:
        lines.append(f"│\n│ <b>API_ID:</b> <code>{api_id}</code>")
    if api_hash is not None:
        masked = api_hash[:4] + "*" * (len(api_hash) - 4)
        lines.append(f"│ <b>API_HASH:</b> <code>{masked}</code>")
    if phone is not None:
        lines.append(f"│ <b>Phone:</b> <code>{phone}</code>")
    return "\n".join(lines) if lines else ""


async def _invoke(user_id, timeout=_TIMEOUT):
    event = Event()
    result = [None]

    async def _on_text(_, message):
        await delete_message(message)
        result[0] = message.text or ""
        event.set()

    async def _on_stop(_, query):
        await query.answer("Process Stopped.", show_alert=True)
        result[0] = _STOP
        event.set()

    h1 = TgClient.bot.add_handler(
        MessageHandler(_on_text, filters=user(user_id) & text & private),
        group=-1,
    )
    h2 = TgClient.bot.add_handler(
        CallbackQueryHandler(_on_stop, filters=_stop_filter(user_id)),
        group=-1,
    )
    try:
        await wait_for(event.wait(), timeout)
    except AsyncTimeout:
        result[0] = None
    finally:
        TgClient.bot.remove_handler(*h1)
        TgClient.bot.remove_handler(*h2)

    return result[0]


async def _stop_or_timeout(value, msg, pyro_client=None):
    if value is None:
        await edit_message(msg, "│ <b>Timed Out!</b>\n│\n│ <i>Process Stopped.</i>")
        if pyro_client:
            await _safe_disconnect(pyro_client)
        return True
    if value == _STOP:
        await edit_message(msg, "│ <b>Process Stopped.</b>")
        if pyro_client:
            await _safe_disconnect(pyro_client)
        return True
    return False


@new_task
async def gen_pyro_string(_, message):
    if message.chat.type != ChatType.PRIVATE:
        return

    user_id = message.from_user.id
    user_name = message.from_user.first_name or "User"
    btns = _stop_btns()

    header = _build_header(user_name)

    sess_msg = await send_message(
        message,
        f"{header}\n\n"
        "│ <i>Send your <code>API_ID</code> (also known as <code>APP_ID</code>).</i>\n"
        "│ <i>Get it from <a href='https://my.telegram.org'>my.telegram.org</a>.</i>\n"
        "│\n"
        f"┖ <b>Timeout:</b> <code>{_timeout_str(_TIMEOUT)}</code>",
        btns,
    )

    api_id = await _invoke(user_id)
    if await _stop_or_timeout(api_id, sess_msg):
        return

    try:
        api_id = int(api_id)
    except ValueError:
        return await edit_message(
            sess_msg,
            f"{header}\n\n"
            "│ <i><code>APP_ID</code> is Invalid.</i>\n│\n│ <b>Process Stopped.</b>",
        )

    await edit_message(
        sess_msg,
        f"{header}\n\n"
        f"{_build_lines(api_id=api_id)}\n\n"
        "│ <i>Send your <code>API_HASH</code>.</i>\n"
        "│ <i>Get it from <a href='https://my.telegram.org'>my.telegram.org</a>.</i>\n"
        "│\n"
        f"┖ <b>Timeout:</b> <code>{_timeout_str(_TIMEOUT)}</code>",
        btns,
    )

    api_hash = await _invoke(user_id)
    if await _stop_or_timeout(api_hash, sess_msg):
        return
    if len(api_hash) <= 30:
        return await edit_message(
            sess_msg,
            f"{header}\n\n"
            f"{_build_lines(api_id=api_id)}\n\n"
            "│ <i><code>API_HASH</code> is Invalid.</i>\n│\n│ <b>Process Stopped.</b>",
        )

    while True:
        await edit_message(
            sess_msg,
            f"{header}\n\n"
            f"{_build_lines(api_id=api_id, api_hash=api_hash)}\n\n"
            "│ <i>Send your phone number in International Format.</i>\n"
            "┖ <b>Example:</b> <code>+14154566376</code>",
            btns,
        )

        phone_no = await _invoke(user_id)
        if await _stop_or_timeout(phone_no, sess_msg):
            return

        await edit_message(
            sess_msg,
            f"{header}\n\n"
            f"{_build_lines(api_id=api_id, api_hash=api_hash, phone=phone_no)}\n\n"
            f"│ Is <code>{phone_no}</code> correct?\n"
            "┖ <b>Send:</b> <code>y</code> / <code>yes</code> | <code>n</code> / <code>no</code>",
            btns,
        )

        confirm = await _invoke(user_id)
        if await _stop_or_timeout(confirm, sess_msg):
            return
        if confirm.lower() in ("y", "yes"):
            break

    try:
        pyro_client = Client(
            f"WZML-X-{user_id}",
            api_id=api_id,
            api_hash=api_hash,
            workdir="/usr/src/app",
        )
    except Exception as e:
        return await edit_message(sess_msg, f"│ <b>Client Error:</b> <i>{e}</i>")

    try:
        await pyro_client.connect()
    except ConnectionError:
        await _safe_disconnect(pyro_client)
        await pyro_client.connect()

    try:
        user_code = await pyro_client.send_code(phone_no)
    except FloodWait as e:
        await _safe_disconnect(pyro_client)
        return await edit_message(
            sess_msg,
            f"{header}\n\n"
            f"{_build_lines(api_id=api_id, api_hash=api_hash, phone=phone_no)}\n\n"
            f"│ <b>FloodWait:</b> <i>Retry after {_timeout_str(e.value)}.</i>",
        )
    except ApiIdInvalid:
        await _safe_disconnect(pyro_client)
        return await edit_message(
            sess_msg,
            f"{header}\n\n"
            f"{_build_lines(api_id=api_id, api_hash=api_hash, phone=phone_no)}\n\n"
            "│ <i><code>API_ID</code> and <code>API_HASH</code> are Invalid.</i>",
        )
    except PhoneNumberInvalid:
        await _safe_disconnect(pyro_client)
        return await edit_message(
            sess_msg,
            f"{header}\n\n"
            f"{_build_lines(api_id=api_id, api_hash=api_hash, phone=phone_no)}\n\n"
            "│ <i>Phone Number is Invalid.</i>",
        )

    await edit_message(
        sess_msg,
        f"{header}\n\n"
        f"{_build_lines(api_id=api_id, api_hash=api_hash, phone=phone_no)}\n\n"
        "│ <i>OTP sent to your Phone Number.</i>\n"
        "│ <i>Enter in <code>1 2 3 4 5</code> format.</i>\n"
        "│\n"
        f"┖ <b>Timeout:</b> <code>{_timeout_str(_TIMEOUT)}</code>",
        btns,
    )

    otp_str = await _invoke(user_id)
    if await _stop_or_timeout(otp_str, sess_msg, pyro_client):
        return

    otp = " ".join(str(otp_str).split())

    try:
        if not pyro_client.is_connected:
            await pyro_client.connect()
        await pyro_client.sign_in(phone_no, user_code.phone_code_hash, phone_code=otp)
    except PhoneCodeInvalid:
        await _safe_disconnect(pyro_client)
        return await edit_message(
            sess_msg,
            f"{header}\n\n"
            f"{_build_lines(api_id=api_id, api_hash=api_hash, phone=phone_no)}\n\n"
            "│ <i>OTP is Invalid.</i>",
        )
    except PhoneCodeExpired:
        await _safe_disconnect(pyro_client)
        return await edit_message(
            sess_msg,
            f"{header}\n\n"
            f"{_build_lines(api_id=api_id, api_hash=api_hash, phone=phone_no)}\n\n"
            "│ <i>OTP has Expired.</i>",
        )
    except SessionPasswordNeeded:
        hint = await pyro_client.get_password_hint()
        await edit_message(
            sess_msg,
            f"{header}\n\n"
            f"{_build_lines(api_id=api_id, api_hash=api_hash, phone=phone_no)}\n\n"
            "│ <i>Account is protected with <b>Two-Step Verification</b>.</i>\n"
            f"│ <b>Hint:</b> <i>{hint}</i>\n"
            "│\n"
            "┖ <i>Send your Password.</i>",
            btns,
        )

        password = await _invoke(user_id)
        if await _stop_or_timeout(password, sess_msg, pyro_client):
            return

        try:
            await pyro_client.check_password(password.strip())
        except Exception as e:
            await _safe_disconnect(pyro_client)
            return await edit_message(
                sess_msg,
                f"{header}\n\n"
                f"{_build_lines(api_id=api_id, api_hash=api_hash, phone=phone_no)}\n\n"
                f"│ <b>Password Error:</b> <i>{e}</i>",
            )
    except Exception as e:
        await _safe_disconnect(pyro_client)
        return await edit_message(
            sess_msg,
            f"{header}\n\n"
            f"{_build_lines(api_id=api_id, api_hash=api_hash, phone=phone_no)}\n\n"
            f"│ <b>Sign In Error:</b> <i>{e}</i>",
        )

    try:
        session_string = await pyro_client.export_session_string()
        await pyro_client.send_message(
            "me",
            f"⌬ <b><u>Pyrogram Session Generated</u></b>\n\n"
            f"<code>{session_string}</code>\n\n"
            f"<b>Via <a href='https://github.com/weebzone/WZML-X'>WZML-X</a> [ @WZML_X ]</b>",
            disable_web_page_preview=True,
        )
        await _safe_disconnect(pyro_client)
        await edit_message(
            sess_msg,
            f"{header}\n\n"
            f"{_build_lines(api_id=api_id, api_hash=api_hash, phone=phone_no)}\n\n"
            "│ <b>String Session Generated Successfully!</b>\n"
            "│\n"
            "┖ <i>Check your <b>Saved Messages</b>.</i>",
        )
    except Exception as e:
        await _safe_disconnect(pyro_client)
        return await edit_message(
            sess_msg,
            f"{header}\n\n"
            f"{_build_lines(api_id=api_id, api_hash=api_hash, phone=phone_no)}\n\n"
            f"│ <b>Export Error:</b> <i>{e}</i>",
        )

    for ext in ("session", "session-journal"):
        path = f"WZML-X-{user_id}.{ext}"
        if path_exists(path):
            try:
                await aioremove(path)
            except Exception:
                pass
