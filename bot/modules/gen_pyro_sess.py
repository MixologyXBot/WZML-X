from asyncio import Event, wait_for, TimeoutError as AsyncTimeout
from os.path import exists as path_exists

from aiofiles.os import remove as aioremove
from pyrogram import Client
from pyrogram.enums import ChatType
from pyrogram.filters import user, text, private, create
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


def _stop_filter(uid):
    return create(lambda _, q: q.data == _STOP and q.from_user.id == uid)


async def _safe_disconnect(client):
    try:
        await client.disconnect()
    except ConnectionError:
        pass


async def _invoke(user_id, timeout=120):
    event = Event()
    result = [None]

    async def _on_text(_, msg):
        await delete_message(msg)
        result[0] = msg.text or ""
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


def _stop_msg():
    btns = ButtonMaker()
    btns.data_button("Stop Process", data=_STOP)
    return btns.build_menu(1)


@new_task
async def gen_pyro_string(_, message):
    if message.chat.type != ChatType.PRIVATE:
        return

    user_id = message.from_user.id
    user_name = message.from_user.first_name or "User"
    STOP = _stop_msg()

    sess_msg = await send_message(
        message,
        f"⌬ <u><i><b>Pyrogram String Session Generator</b></i></u>\n\n"
        f"Hello <b>{user_name}</b>!\n\n"
        f"<i>Send your <code>API_ID</code> (also known as <code>APP_ID</code>).</i>\n"
        f"<i>Get it from <a href='https://my.telegram.org'>my.telegram.org</a>.</i>\n\n"
        f"<b>Timeout:</b> 120s",
        STOP,
    )

    api_id_str = await _invoke(user_id)
    if api_id_str is None:
        return await edit_message(sess_msg, "⌬ <b>Timed Out!</b>\n\n<i>Process Stopped.</i>")
    if api_id_str == _STOP:
        return await edit_message(sess_msg, "⌬ <b>Process Stopped.</b>")

    try:
        api_id = int(api_id_str)
    except ValueError:
        return await edit_message(
            sess_msg,
            "⌬ <i><code>APP_ID</code> is Invalid.</i>\n\n<b>Process Stopped.</b>",
        )

    await edit_message(
        sess_msg,
        "⌬ <u><i><b>Pyrogram String Session Generator</b></i></u>\n\n"
        "<i>Send your <code>API_HASH</code>.</i>\n"
        "<i>Get it from <a href='https://my.telegram.org'>my.telegram.org</a>.</i>\n\n"
        "<b>Timeout:</b> 120s",
        STOP,
    )

    api_hash = await _invoke(user_id)
    if api_hash is None:
        return await edit_message(sess_msg, "⌬ <b>Timed Out!</b>\n\n<i>Process Stopped.</i>")
    if api_hash == _STOP:
        return await edit_message(sess_msg, "⌬ <b>Process Stopped.</b>")
    if len(api_hash) <= 30:
        return await edit_message(
            sess_msg,
            "⌬ <i><code>API_HASH</code> is Invalid.</i>\n\n<b>Process Stopped.</b>",
        )

    while True:
        await edit_message(
            sess_msg,
            "⌬ <u><i><b>Pyrogram String Session Generator</b></i></u>\n\n"
            "<i>Send your Telegram account's phone number in International Format "
            "(including country code).</i>\n\n"
            "<b>Example:</b> <code>+14154566376</code>\n\n"
            "<b>Timeout:</b> 120s",
            STOP,
        )

        phone_no = await _invoke(user_id)
        if phone_no is None:
            return await edit_message(sess_msg, "⌬ <b>Timed Out!</b>\n\n<i>Process Stopped.</i>")
        if phone_no == _STOP:
            return await edit_message(sess_msg, "⌬ <b>Process Stopped.</b>")

        await edit_message(
            sess_msg,
            f"⌬ <b>Verification Confirmation:</b>\n\n"
            f"<i>Is <code>{phone_no}</code> correct?</i>\n\n"
            f"<b>Send:</b> <code>y</code> / <code>yes</code> (Confirm) | <code>n</code> / <code>no</code> (Retry)",
            STOP,
        )

        confirm = await _invoke(user_id)
        if confirm is None:
            return await edit_message(sess_msg, "⌬ <b>Timed Out!</b>\n\n<i>Process Stopped.</i>")
        if confirm == _STOP:
            return await edit_message(sess_msg, "⌬ <b>Process Stopped.</b>")
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
        return await edit_message(sess_msg, f"⌬ <b>Client Error:</b> <i>{e}</i>")

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
            f"⌬ <b>FloodWait:</b> <i>Retry after {e.value} seconds.</i>",
        )
    except ApiIdInvalid:
        await _safe_disconnect(pyro_client)
        return await edit_message(
            sess_msg,
            "⌬ <i><code>API_ID</code> and <code>API_HASH</code> are Invalid.</i>",
        )
    except PhoneNumberInvalid:
        await _safe_disconnect(pyro_client)
        return await edit_message(
            sess_msg, "⌬ <i>Phone Number is Invalid.</i>"
        )

    await edit_message(
        sess_msg,
        "⌬ <u><i><b>Pyrogram String Session Generator</b></i></u>\n\n"
        "<i>OTP has been sent to your Phone Number.</i>\n\n"
        "<i>Enter OTP in <code>1 2 3 4 5</code> format (space between each digit).</i>\n\n"
        "<b>Timeout:</b> 120s",
        STOP,
    )

    otp_str = await _invoke(user_id)
    if otp_str is None:
        await _safe_disconnect(pyro_client)
        return await edit_message(sess_msg, "⌬ <b>Timed Out!</b>\n\n<i>Process Stopped.</i>")
    if otp_str == _STOP:
        await _safe_disconnect(pyro_client)
        return await edit_message(sess_msg, "⌬ <b>Process Stopped.</b>")

    otp = " ".join(str(otp_str).split())

    try:
        if not pyro_client.is_connected:
            await pyro_client.connect()
        await pyro_client.sign_in(phone_no, user_code.phone_code_hash, phone_code=otp)
    except PhoneCodeInvalid:
        await _safe_disconnect(pyro_client)
        return await edit_message(
            sess_msg, "⌬ <i>OTP is Invalid.</i>"
        )
    except PhoneCodeExpired:
        await _safe_disconnect(pyro_client)
        return await edit_message(
            sess_msg, "⌬ <i>OTP has Expired.</i>"
        )
    except SessionPasswordNeeded:
        await edit_message(
            sess_msg,
            "⌬ <u><i><b>Pyrogram String Session Generator</b></i></u>\n\n"
            "<i>Account is protected with <b>Two-Step Verification.</b></i>\n\n"
            "<i>Send your Password below.</i>\n\n"
            f"<b>Password Hint:</b> <i>{await pyro_client.get_password_hint()}</i>\n\n"
            "<b>Timeout:</b> 120s",
            STOP,
        )

        password = await _invoke(user_id)
        if password is None:
            await _safe_disconnect(pyro_client)
            return await edit_message(sess_msg, "⌬ <b>Timed Out!</b>\n\n<i>Process Stopped.</i>")
        if password == _STOP:
            await _safe_disconnect(pyro_client)
            return await edit_message(sess_msg, "⌬ <b>Process Stopped.</b>")

        try:
            await pyro_client.check_password(password.strip())
        except Exception as e:
            await _safe_disconnect(pyro_client)
            return await edit_message(
                sess_msg, f"⌬ <b>Password Check Error:</b> <i>{e}</i>"
            )
    except Exception as e:
        await _safe_disconnect(pyro_client)
        return await edit_message(
            sess_msg, f"⌬ <b>Sign In Error:</b> <i>{e}</i>"
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
            "⌬ <u><i><b>Pyrogram String Session Generator</b></i></u>\n\n"
            "➲ <b>String Session Generated Successfully!</b>\n\n"
            "<i>Check your <b>Saved Messages</b>.</i>",
        )
    except Exception as e:
        await _safe_disconnect(pyro_client)
        return await edit_message(sess_msg, f"⌬ <b>Export Error:</b> <i>{e}</i>")

    for ext in ("session", "session-journal"):
        path = f"WZML-X-{user_id}.{ext}"
        if path_exists(path):
            try:
                await aioremove(path)
            except Exception:
                pass
