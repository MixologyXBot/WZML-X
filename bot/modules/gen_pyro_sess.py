from asyncio import Event, wait_for, TimeoutError as AsyncTimeout
from os.path import exists as path_exists

from aiofiles.os import remove as aioremove
from pyrogram import Client
from pyrogram.filters import user, text, private
from pyrogram.handlers import MessageHandler
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
from ..helper.telegram_helper.message_utils import send_message, edit_message, delete_message


async def _invoke(user_id, timeout=120):
    event = Event()
    result = [None]

    async def _handler(_, msg):
        value = msg.text
        await delete_message(msg)
        if value:
            result[0] = value
        event.set()

    handler = TgClient.bot.add_handler(
        MessageHandler(
            _handler,
            filters=user(user_id) & text & private,
        ),
        group=-1,
    )
    try:
        await wait_for(event.wait(), timeout)
    except AsyncTimeout:
        result[0] = None
    finally:
        TgClient.bot.remove_handler(*handler)

    return result[0]


async def _stop_or_timeout(value, sess_msg, pyro_client=None):
    if pyro_client:
        await pyro_client.disconnect()
    if value is None:
        await edit_message(
            sess_msg,
            "⌬ <b>Timed Out!</b>\n\n<i>No response received. Process Stopped.</i>",
        )
        return True
    if value.lower() == "/stop":
        await edit_message(sess_msg, "⌬ <b>Process Stopped.</b>")
        return True
    return False


@new_task
async def gen_pyro_string(_, message):
    user_id = message.from_user.id
    user_name = message.from_user.first_name or "User"

    sess_msg = await send_message(
        message,
        f"⌬ <u><i><b>Pyrogram String Session Generator</b></i></u>\n\n"
        f"Hello <b>{user_name}</b>!\n\n"
        f"<i>Send your <code>API_ID</code> (also known as <code>APP_ID</code>).</i>\n"
        f"<i>Get it from <a href='https://my.telegram.org'>my.telegram.org</a>.</i>\n\n"
        f"<b>Timeout:</b> 120s\n\n"
        f"<i>Send /stop to cancel the process.</i>",
    )

    api_id_str = await _invoke(user_id)
    if await _stop_or_timeout(api_id_str, sess_msg):
        return

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
        "<b>Timeout:</b> 120s\n\n"
        "<i>Send /stop to cancel the process.</i>",
    )

    api_hash = await _invoke(user_id)
    if await _stop_or_timeout(api_hash, sess_msg):
        return
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
            "<b>Timeout:</b> 120s\n\n"
            "<i>Send /stop to cancel the process.</i>",
        )

        phone_no = await _invoke(user_id)
        if await _stop_or_timeout(phone_no, sess_msg):
            return

        await edit_message(
            sess_msg,
            f"⌬ <b>Verification Confirmation:</b>\n\n"
            f"<i>Is <code>{phone_no}</code> correct?</i>\n\n"
            f"<b>Send:</b> <code>y</code> / <code>yes</code> (Confirm) | <code>n</code> / <code>no</code> (Retry)",
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
        return await edit_message(sess_msg, f"⌬ <b>Client Error:</b> <i>{e}</i>")

    try:
        await pyro_client.connect()
    except ConnectionError:
        await pyro_client.disconnect()
        await pyro_client.connect()

    try:
        user_code = await pyro_client.send_code(phone_no)
    except FloodWait as e:
        await pyro_client.disconnect()
        return await edit_message(
            sess_msg,
            f"⌬ <b>FloodWait:</b> <i>Retry after {e.value} seconds.</i>\n\n"
            "<b>Process Stopped.</b>",
        )
    except ApiIdInvalid:
        await pyro_client.disconnect()
        return await edit_message(
            sess_msg,
            "⌬ <i><code>API_ID</code> and <code>API_HASH</code> are Invalid.</i>\n\n"
            "<b>Process Stopped.</b>",
        )
    except PhoneNumberInvalid:
        await pyro_client.disconnect()
        return await edit_message(
            sess_msg,
            "⌬ <i>Phone Number is Invalid.</i>\n\n<b>Process Stopped.</b>",
        )

    await edit_message(
        sess_msg,
        "⌬ <u><i><b>Pyrogram String Session Generator</b></i></u>\n\n"
        "<i>OTP has been sent to your Phone Number.</i>\n\n"
        "<i>Enter OTP in <code>1 2 3 4 5</code> format (space between each digit).</i>\n\n"
        "<b>Timeout:</b> 120s\n\n"
        "<i>Send /stop to cancel the process.</i>",
    )

    otp_str = await _invoke(user_id)
    if await _stop_or_timeout(otp_str, sess_msg, pyro_client):
        return

    otp = " ".join(str(otp_str).split())

    try:
        await pyro_client.sign_in(phone_no, user_code.phone_code_hash, phone_code=otp)
    except PhoneCodeInvalid:
        await pyro_client.disconnect()
        return await edit_message(
            sess_msg,
            "⌬ <i>OTP is Invalid.</i>\n\n<b>Process Stopped.</b>",
        )
    except PhoneCodeExpired:
        await pyro_client.disconnect()
        return await edit_message(
            sess_msg,
            "⌬ <i>OTP has Expired.</i>\n\n<b>Process Stopped.</b>",
        )
    except SessionPasswordNeeded:
        await edit_message(
            sess_msg,
            "⌬ <u><i><b>Pyrogram String Session Generator</b></i></u>\n\n"
            "<i>Account is protected with <b>Two-Step Verification.</b></i>\n\n"
            "<i>Send your Password below.</i>\n\n"
            f"<b>Password Hint:</b> <i>{await pyro_client.get_password_hint()}</i>\n\n"
            "<b>Timeout:</b> 120s\n\n"
            "<i>Send /stop to cancel the process.</i>",
        )

        password = await _invoke(user_id)
        if await _stop_or_timeout(password, sess_msg, pyro_client):
            return

        try:
            await pyro_client.check_password(password.strip())
        except Exception as e:
            await pyro_client.disconnect()
            return await edit_message(
                sess_msg,
                f"⌬ <b>Password Check Error:</b> <i>{e}</i>",
            )
    except Exception as e:
        await pyro_client.disconnect()
        return await edit_message(
            sess_msg, f"⌬ <b>Sign In Error:</b> <i>{e}</i>"
        )

    try:
        session_string = await pyro_client.export_session_string()
        await pyro_client.send_message(
            "me",
            f"⌬ <b><u>Pyrogram Session Generated</u></b>\n\n"
            f"<code>{session_string}</code>\n\n"
            f"<b>Via <a href='https://github.com/SilentDemonSD/WZML-X'>WZML-X</a> [ @WZML_X ]</b>",
            disable_web_page_preview=True,
        )
        await pyro_client.disconnect()
        await edit_message(
            sess_msg,
            "⌬ <u><i><b>Pyrogram String Session Generator</b></i></u>\n\n"
            "➲ <b>String Session Generated Successfully!</b>\n\n"
            "<i>Check your <b>Saved Messages</b>.</i>",
        )
    except Exception as e:
        await pyro_client.disconnect()
        return await edit_message(
            sess_msg,
            f"⌬ <b>Export Error:</b> <i>{e}</i>",
        )

    for ext in ("session", "session-journal"):
        path = f"WZML-X-{user_id}.{ext}"
        if path_exists(path):
            try:
                await aioremove(path)
            except Exception:
                pass
