from ..helper.ext_utils.bot_utils import COMMAND_USAGE, new_task
from ..helper.ext_utils.help_messages import (
    YT_HELP_DICT,
    MIRROR_HELP_DICT,
    CLONE_HELP_DICT,
)
from ..helper.telegram_helper.button_build import ButtonMaker
from ..helper.telegram_helper.message_utils import (
    edit_message,
    delete_message,
    send_message,
)
from ..helper.ext_utils.help_messages import help_string


@new_task
async def arg_usage(_, query):
    data = query.data.split()
    message = query.message
    await query.answer()
    if data[1] == "close":
        return await delete_message(message, message.reply_to_message)
    pg_no = int(data[3])
    key = {"m": "mirror", "y": "yt", "c": "clone"}.get(data[2], data[2])

    if data[1] in ("nex", "pre", "back"):
        pages = COMMAND_USAGE.get(key)
        if not pages:
            return
        btn_idx = pg_no + 1
        if 1 <= btn_idx < len(pages):
            await edit_message(message, pages[0], pages[btn_idx])
    elif data[1] in COMMAND_USAGE:
        info = {
            "mirror": ("m", MIRROR_HELP_DICT),
            "yt": ("y", YT_HELP_DICT),
            "clone": ("c", CLONE_HELP_DICT),
        }
        back_key, help_dict = info[data[1]]
        button = ButtonMaker()
        button.data_button("Back", f"help back {back_key} {pg_no}")
        await edit_message(message, help_dict[data[2]], button.build_menu())


@new_task
async def bot_help(_, message):
    await send_message(message, help_string)
