from asyncio import run
from hashlib import sha256
from importlib import import_module
from logging import (
    FileHandler,
    StreamHandler,
    INFO,
    basicConfig,
    getLogger,
    ERROR,
)
from os import path, remove, environ
from pymongo import AsyncMongoClient
from pymongo.errors import PyMongoError
from pymongo.server_api import ServerApi
from subprocess import run as srun, call as scall
from sys import exit

getLogger("pymongo").setLevel(ERROR)

_LOGGER = getLogger("update")

_DB_PARTITION_SALT = b"wzmlx_v3_db_partition_salt"
_VAR_LIST = [
    "BOT_TOKEN",
    "TELEGRAM_API",
    "TELEGRAM_HASH",
    "OWNER_ID",
    "DATABASE_URL",
    "BASE_URL",
    "UPSTREAM_REPO",
    "UPSTREAM_BRANCH",
    "UPDATE_PKGS",
]

def _get_version():
    try:
        version = import_module("bot.version")
        return version.get_version()
    except Exception:
        return "unknown"


def _setup_logging():
    if path.exists("log.txt"):
        with open("log.txt", "r+") as f:
            f.truncate(0)
    if path.exists("rlog.txt"):
        remove("rlog.txt")
    basicConfig(
        format="[%(asctime)s] [%(levelname)s] - %(message)s",
        datefmt="%d-%b-%y %I:%M:%S %p",
        handlers=[FileHandler("log.txt"), StreamHandler()],
        level=INFO,
    )


def _load_config():
    try:
        settings = import_module("config")
        config_file = {
            key: value.strip() if isinstance(value, str) else value
            for key, value in vars(settings).items()
            if not key.startswith("__")
        }
    except ModuleNotFoundError:
        _LOGGER.info("Config.py file is not Added! Checking ENVs..")
        config_file = {}

    env_updates = {
        key: value.strip() if isinstance(value, str) else value
        for key, value in environ.items()
        if key in _VAR_LIST
    }
    if env_updates:
        _LOGGER.info("Config data is updated with ENVs!")
        config_file.update(env_updates)
    return config_file


def _db_partition_id(bot_id):
    raw = sha256(_DB_PARTITION_SALT + str(bot_id).encode("utf-8")).hexdigest()
    return f"p_{raw[:24]}"


async def _fetch_db_config(database_url, db_part):
    conn = AsyncMongoClient(database_url, server_api=ServerApi("1"))
    try:
        db = conn.wzmlx
        return await db.settings.config.find_one({"_id": db_part}, {"_id": 0})
    except PyMongoError as e:
        _LOGGER.error(f"Database ERROR: {e}")
        return None
    finally:
        await conn.close()


def _fetch_config_from_db(config_file, db_part):
    database_url = config_file.get("DATABASE_URL", "").strip()
    if not database_url:
        return
    db_config = run(_fetch_db_config(database_url, db_part))
    if db_config is not None:
        for key, value in db_config.items():
            if key not in config_file or config_file[key] is None:
                config_file[key] = value
        _LOGGER.info("Config imported from MongoDB")
    else:
        _LOGGER.warning("No saved config found in MongoDB, using defaults")


def _run_update(upstream_repo, upstream_branch, version):
    if not upstream_repo:
        _LOGGER.info("No UPSTREAM_REPO set, skipping git update")
        return

    if path.exists(".git"):
        srun(["rm", "-rf", ".git"])

    result = srun(
        [
            "bash",
            "-c",
            f"git init -q"
            f" && git config --global user.email 105407900+SilentDemonSD@users.noreply.github.com"
            f" && git config --global user.name SilentDemonSD"
            f" && git add ."
            f" && git commit -sm update -q"
            f" && git remote add origin {upstream_repo}"
            f" && git fetch origin -q"
            f" && git reset --hard origin/{upstream_branch} -q",
        ],
    )

    display_repo = "/".join(upstream_repo.split("/")[-2:])
    if result.returncode == 0:
        _LOGGER.info("Successfully updated with Latest Updates!")
    else:
        _LOGGER.error("Something went Wrong! Recheck your details or Ask Support!")
    _LOGGER.info(f"UPSTREAM_REPO: {display_repo} | UPSTREAM_BRANCH: {upstream_branch} | VERSION: {version}")


def _update_packages(update_pkgs):
    if (isinstance(update_pkgs, str) and update_pkgs.lower() == "true") or update_pkgs:
        scall("uv pip install -U -r requirements.txt", shell=True)
        _LOGGER.info("Successfully Updated all the Packages!")


def main():
    _setup_logging()
    config_file = _load_config()
    version = _get_version()

    bot_token = config_file.get("BOT_TOKEN", "")
    if not bot_token:
        _LOGGER.error("BOT_TOKEN variable is missing! Exiting now")
        exit(1)

    bot_id = bot_token.split(":", 1)[0]
    db_part = _db_partition_id(bot_id)

    _fetch_config_from_db(config_file, db_part)

    upstream_repo = config_file.get("UPSTREAM_REPO", "").strip()
    upstream_branch = config_file.get("UPSTREAM_BRANCH", "").strip() or "wzv3"

    _run_update(upstream_repo, upstream_branch, version)

    update_pkgs = config_file.get("UPDATE_PKGS", "True")
    _update_packages(update_pkgs)


if __name__ == "__main__":
    main()
