import os

class Config:
    BOT_TOKEN = "7669343401:AAGlmpa_R6qlLgUSbkzFwaauVehMiv2nePY"
    API_ID = 2040
    API_HASH = "b18441a1ff607e10a989891a5462e627"
    
    DATABASE_URL = os.environ.get('DATABASE_URL')
    REDIS_URL = os.environ.get('REDIS_URL')
    WEBAPP_URL = os.environ.get('WEBAPP_URL')

    PARSER_MESSAGE_LIMIT = int(os.environ.get('PARSER_MESSAGE_LIMIT', 10000))

    HELP_MESSAGE = (
        "**Dominatus v3.4**\n\n"
        "📝 `/add <link>` - Добавить чат в очередь\n"
        "🔍 `/search <link> <q>` - Поиск в чате\n"
        "📊 `/status <link>` - Статус сканирования\n"
        "📈 `/stats` - Общая статистика\n"
        "🆔 `/my_id` - Ваш Telegram ID\n\n"
        "Для глобального поиска просто отправьте текст, ID или @username."
    )

config = Config()
