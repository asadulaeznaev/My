import os

class Config:
    BOT_TOKEN = "7669343401:AAGlmpa_R6qlLgUSbkzFwaauVehMiv2nePY"
    API_ID = 2040
    API_HASH = "b18441a1ff607e10a989891a5462e627"

    PARSER_MESSAGE_LIMIT = 5000

    HELP_MESSAGE = (
        "**Dominatus Lite v1.0**\n\n"
        "🔍 `/search <link> <q>` - Поиск в чате в реальном времени.\n"
        "Например: `/search https://t.me/some_chat важное слово`\n\n"
        "🆔 `/my_id` - Ваш Telegram ID\n\n"
        "**Внимание:** Поиск может занять несколько минут."
    )

config = Config()
