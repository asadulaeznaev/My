import os
import sqlite3
import logging
import threading
import time
import asyncio
import requests
from flask import Flask, request
from gunicorn.app.base import BaseApplication
from telebot import TeleBot, types
from telebot.apihelper import ApiTelegramException
from telethon import TelegramClient
from telethon.tl.types import User

# ==============================================================================
# 1. КОНФИГУРАЦИЯ И СЕКРЕТНЫЕ ДАННЫЕ (УСТАНОВЛЕНЫ ПО ПРИКАЗУ)
# ==============================================================================
BOT_TOKEN = "8124170502:AAGu0S-gdIJa8Mk-TXa74pIs6_aG8FyWS_E"
API_ID = 2040
API_HASH = "b18441a1ff607e10a989891a5462e627"
ADMIN_ID = 7926898132 # ЗАМЕНИТЕ НА ВАШ РЕАЛЬНЫЙ TELEGRAM ID

# Конфигурация парсера
PARSER_MESSAGE_LIMIT = 300
PARSER_CHAT_BLACKLIST = ['новости', 'ставки', 'крипто', 'news', 'crypto', 'bets']
DB_PATH = 'data/citadel_monolith.db'

# ==============================================================================
# 2. КЛАСС УПРАВЛЕНИЯ БАЗОЙ ДАННЫХ
# ==============================================================================
class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)

    def _get_connection(self):
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        return sqlite3.connect(self.db_path, check_same_thread=False)

    def init_db(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
        CREATE TABLE IF NOT EXISTS messages (
            id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
            first_name TEXT, last_name TEXT, username TEXT, phone TEXT,
            message_date TEXT NOT NULL, message_link TEXT NOT NULL UNIQUE,
            message_content TEXT, media_count INTEGER DEFAULT 0,
            chat_name TEXT, chat_id INTEGER
        )''')
        cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON messages (user_id);')
        conn.commit()
        conn.close()
        self.logger.info("База данных и индекс успешно инициализированы.")

    def save_message(self, data):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute('''
        INSERT OR IGNORE INTO messages (
            user_id, first_name, last_name, username, phone, message_date, 
            message_link, message_content, media_count, chat_name, chat_id
        ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        ''', (
            data.get('user_id'), data.get('first_name'), data.get('last_name'),
            data.get('username'), data.get('phone'), data.get('message_date'),
            data.get('message_link'), data.get('message_content'),
            data.get('media_count', 0), data.get('chat_name'), data.get('chat_id')
        ))
        conn.commit()
        conn.close()

    def search_user(self, user_id):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT * FROM messages WHERE user_id = ? ORDER BY message_date DESC", (user_id,))
        columns = [description[0] for description in cursor.description]
        rows = cursor.fetchall()
        return [dict(zip(columns, row)) for row in rows]

    def get_stats(self):
        conn = self._get_connection()
        cursor = conn.cursor()
        cursor.execute("SELECT COUNT(*), COUNT(DISTINCT user_id) FROM messages")
        stats = cursor.fetchone()
        conn.close()
        return stats or (0, 0)

# ==============================================================================
# 3. КЛАСС ПАРСЕРА
# ==============================================================================
class Parser:
    def __init__(self, api_id, api_hash, db_manager):
        self.client = TelegramClient('data/parser_session', api_id, api_hash)
        self.db_manager = db_manager
        self.logger = logging.getLogger(__name__)
    
    async def _scan_loop(self):
        async with self.client:
            self.logger.info("Клиент Telethon запущен.")
            while True:
                try:
                    self.logger.info("Начинаю новый цикл сканирования...")
                    dialogs = await self.client.get_dialogs()
                    for dialog in dialogs:
                        if any(word in dialog.name.lower() for word in PARSER_CHAT_BLACKLIST): continue
                        if not (dialog.is_group or dialog.is_channel): continue

                        async for message in self.client.iter_messages(dialog.id, limit=PARSER_MESSAGE_LIMIT):
                            if not message.sender or not isinstance(message.sender, User) or message.sender.bot: continue
                            
                            user, msg_link = message.sender, f"https://t.me/c/{dialog.id}/{message.id}"
                            self.db_manager.save_message({
                                "user_id": user.id, "first_name": user.first_name, "last_name": user.last_name,
                                "username": user.username, "phone": user.phone, "message_date": message.date.isoformat(),
                                "message_link": msg_link, "message_content": message.text,
                                "media_count": 1 if message.media else 0, "chat_name": dialog.name, "chat_id": dialog.id
                            })
                except Exception as e:
                    self.logger.error(f"Ошибка в цикле парсера: {e}")
                
                self.logger.info("Цикл парсинга завершен. Пауза 15 минут.")
                await asyncio.sleep(15 * 60)

    def _run_in_new_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._scan_loop())

    def start(self):
        threading.Thread(target=self._run_in_new_loop, daemon=True).start()

# ==============================================================================
# 4. КЛАСС ОБРАБОТЧИКА БОТА
# ==============================================================================
class BotHandler:
    def __init__(self, bot, db_manager):
        self.bot = bot
        self.db = db_manager
        self.logger = logging.getLogger(__name__)

    def _is_admin(self, message):
        return message.from_user.id == ADMIN_ID

    def _format_message(self, data, page, total):
        return (
            f"**Запись {page + 1} из {total}**\n\n"
            f"👤 **Пользователь:** `{data.get('first_name') or ''} {data.get('last_name') or ''}`\n"
            f"**ID:** `{data.get('user_id')}` | **Юзернейм:** `@{data.get('username') or 'N/A'}`\n\n"
            f"📅 **Дата:** `{data.get('message_date')}`\n"
            f"🏛 **Чат:** `{data.get('chat_name')}`\n\n"
            f"📜 **Сообщение:**\n"
            f"```\n{data.get('message_content') or 'Нет текста'}\n```"
        )

    def _create_navigation_markup(self, page, total, user_id):
        markup = types.InlineKeyboardMarkup()
        row = [
            types.InlineKeyboardButton("◀️", callback_data=f"page_{page - 1}_{user_id}") if page > 0 else types.InlineKeyboardButton(" ", callback_data="page_noop"),
            types.InlineKeyboardButton(f"{page + 1}/{total}", callback_data="page_noop"),
            types.InlineKeyboardButton("▶️", callback_data=f"page_{page + 1}_{user_id}") if page < total - 1 else types.InlineKeyboardButton(" ", callback_data="page_noop"),
            types.InlineKeyboardButton("❌", callback_data="page_close")
        ]
        markup.row(*row)
        return markup

    def register_handlers(self):
        @self.bot.message_handler(commands=['start'], func=self._is_admin)
        def send_welcome(message): self.bot.reply_to(message, "Протокол Монолит активен. Введите ID цели.")

        @self.bot.message_handler(commands=['stats'], func=self._is_admin)
        def send_stats(message):
            total, unique = self.db.get_stats()
            self.bot.send_message(message.chat.id, f"**Статистика БД**\n- Всего сообщений: `{total}`\n- Уникальных пользователей: `{unique}`", parse_mode="Markdown")

        @self.bot.message_handler(func=lambda msg: self._is_admin(msg) and msg.text and msg.text.isdigit())
        def handle_user_id(message):
            results = self.db.search_user(int(message.text))
            if not results:
                self.bot.reply_to(message, "Цель не найдена в базе данных.")
                return
            page, total = 0, len(results)
            self.bot.send_message(message.chat.id, self._format_message(results[page], page, total), 
                                  reply_markup=self._create_navigation_markup(page, total, int(message.text)), parse_mode="Markdown")

        @self.bot.callback_query_handler(func=lambda call: self._is_admin(call))
        def handle_pagination(call):
            try:
                action = call.data.split("_")[1]
                if action == "noop": self.bot.answer_callback_query(call.id); return
                if action == "close": self.bot.delete_message(call.message.chat.id, call.message.message_id); return
                
                page, user_id = int(action), int(call.data.split("_")[2])
                results = self.db.search_user(user_id)
                if not results: self.bot.answer_callback_query(call.id, text="Данные не найдены."); return
                
                total = len(results)
                self.bot.edit_message_text(self._format_message(results[page], page, total), 
                                           call.message.chat.id, call.message.message_id, 
                                           reply_markup=self._create_navigation_markup(page, total, user_id), parse_mode="Markdown")
            except ApiTelegramException as e:
                self.logger.error(f"Ошибка API Telegram: {e}")
            except Exception as e:
                self.logger.error(f"Общая ошибка пагинации: {e}")

# ==============================================================================
# 5. ГЛАВНЫЙ КЛАСС ПРИЛОЖЕНИЯ И ЗАПУСК
# ==============================================================================
class App:
    def __init__(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.db = DatabaseManager(DB_PATH)
        self.bot = TeleBot(BOT_TOKEN)
        self.bot_handler = BotHandler(self.bot, self.db)
        self.parser = Parser(API_ID, API_HASH, self.db)
        self.flask_app = Flask(__name__)
        self.setup_routes()

    def setup_routes(self):
        self.flask_app.add_url_rule(f'/{BOT_TOKEN}', 'process_webhook', self.process_webhook, methods=['POST'])
        self.flask_app.add_url_rule('/', 'index', lambda: "Экзекутор Воли: Протокол Монолит активен.")

    def process_webhook(self):
        update = types.Update.de_json(request.get_data().decode('utf-8'))
        self.bot.process_new_updates([update])
        return '', 200
    
    def guardian_thread_func(self):
        render_url = os.environ.get('RENDER_EXTERNAL_URL')
        if not render_url:
            self.logger.warning("'Страж' неактивен: RENDER_EXTERNAL_URL не найдена.")
            return
        
        while True:
            time.sleep(15)
            try: requests.get(render_url, timeout=10)
            except Exception: pass
            time.sleep(10 * 60)

    def run(self):
        self.db.init_db()
        self.bot_handler.register_handlers()
        self.parser.start()
        threading.Thread(target=self.guardian_thread_func, daemon=True).start()
        
        self.logger.info("Веб-сервер готов к запуску.")
        # Для локального теста раскомментируйте строку ниже и закомментируйте Gunicorn
        # self.flask_app.run(host='0.0.0.0', port=5000)

class StandaloneGunicorn(BaseApplication):
    def __init__(self, app, options=None):
        self.options = options or {}
        self.application = app
        super().__init__()

    def load_config(self):
        for key, value in self.options.items():
            self.cfg.set(key.lower(), value)

    def load(self):
        return self.application

if __name__ == '__main__':
    app_instance = App()
    
    # Запуск через Gunicorn для Render. Для локального теста закомментируйте эти строки.
    options = {
        'bind': f'0.0.0.0:{os.environ.get("PORT", 5000)}',
        'workers': 3,
        'threads': 2,
    }
    StandaloneGunicorn(app_instance.flask_app, options).run()
