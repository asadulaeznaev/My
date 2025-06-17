import os
import sqlite3
import logging
import threading
import time
import asyncio
import requests
import io
from flask import Flask, request
from telebot import TeleBot, types
from telebot.apihelper import ApiTelegramException
from telethon import TelegramClient
from telethon.tl.types import User

class Config:
    BOT_TOKEN = "8124170502:AAGu0S-gdIJa8Mk-TXa74pIs6_aG8FyWS_E"
    API_ID = 2040
    API_HASH = "b18441a1ff607e10a989891a5462e627"
    PARSER_MESSAGE_LIMIT = int(os.environ.get('PARSER_MESSAGE_LIMIT', 10000))
    DB_PATH = 'data/dominatus_protocol.db'
    HELP_MESSAGE = (
        "**Протокол 'Dominatus'**\n\n"
        "**1. Добавление чата на сканирование:**\n"
        "`/add_chat <ссылка_на_чат_или_@username>`\n\n"
        "**2. Глобальный поиск (по всем чатам):**\n"
        "Просто отправьте ID, `@username` или `текст`.\n\n"
        "**3. Поиск в конкретном чате:**\n"
        "`/search <ссылка_на_чат> <ID, @user, текст>`\n\n"
        "**4. Утилиты:**\n"
        "`/status <ссылка>` - узнать статус сканирования\n"
        "`/chat_info <ссылка>` - статистика по чату\n"
        "`/stats` - общая статистика\n"
        "`/my_id` - ваш Telegram ID"
    )

class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        os.makedirs(os.path.dirname(self.db_path), exist_ok=True)
        self.init_db()

    def _get_connection(self):
        return sqlite3.connect(self.db_path, check_same_thread=False, timeout=10)

    def init_db(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS messages (
                id INTEGER PRIMARY KEY AUTOINCREMENT, user_id INTEGER NOT NULL,
                first_name TEXT, last_name TEXT, username TEXT,
                message_date TEXT NOT NULL, message_link TEXT NOT NULL UNIQUE,
                message_content TEXT, chat_id INTEGER NOT NULL
            )''')
            cursor.execute('''
            CREATE TABLE IF NOT EXISTS scanned_chats (
                id INTEGER PRIMARY KEY AUTOINCREMENT, chat_link TEXT NOT NULL UNIQUE,
                chat_id INTEGER, chat_title TEXT, submitter_id INTEGER NOT NULL, 
                status TEXT NOT NULL, message_count INTEGER DEFAULT 0, last_scanned TEXT
            )''')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_user_id ON messages (user_id);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_username ON messages (username);')
            cursor.execute('CREATE INDEX IF NOT EXISTS idx_chat_id ON messages (chat_id);')
            cursor.execute('CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(message_content, content=`messages`, content_rowid=`id`);')
            cursor.execute('''
            CREATE TRIGGER IF NOT EXISTS messages_after_insert AFTER INSERT ON messages BEGIN
                INSERT INTO messages_fts(rowid, message_content) VALUES (new.id, new.message_content);
            END;
            ''')

    def add_chat_to_queue(self, chat_link, submitter_id):
        with self._get_connection() as conn:
            conn.execute("INSERT OR IGNORE INTO scanned_chats (chat_link, submitter_id, status) VALUES (?, ?, ?)", 
                         (chat_link, submitter_id, 'PENDING'))

    def get_next_pending_chat(self):
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM scanned_chats WHERE status = 'PENDING' ORDER BY id LIMIT 1")
            return dict(cursor.fetchone()) if cursor.fetchone() else None

    def update_chat_status(self, chat_id, status, msg_count=None, title=None, real_id=None):
        with self._get_connection() as conn:
            query = "UPDATE scanned_chats SET status = ?, last_scanned = datetime('now') WHERE id = ?"
            params = [status, chat_id]
            if msg_count is not None:
                query = "UPDATE scanned_chats SET status = ?, message_count = ?, last_scanned = datetime('now') WHERE id = ?"
                params = [status, msg_count, chat_id]
            if title:
                conn.execute("UPDATE scanned_chats SET chat_title = ? WHERE id = ?", (title, chat_id))
            if real_id:
                conn.execute("UPDATE scanned_chats SET chat_id = ? WHERE id = ?", (real_id, chat_id))
            conn.execute(query, params)
    
    def get_chat_info(self, chat_link):
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute("SELECT * FROM scanned_chats WHERE chat_link = ?", (chat_link,))
            return dict(cursor.fetchone()) if cursor.fetchone() else None

    def save_message_batch(self, messages):
        with self._get_connection() as conn:
            conn.executemany('''
            INSERT OR IGNORE INTO messages (user_id, first_name, last_name, username, message_date, 
            message_link, message_content, chat_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)''', messages)

    def search_globally(self, query):
        if query.isdigit(): return self._execute_search("SELECT m.*, sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id = sc.chat_id WHERE m.user_id = ? ORDER BY m.message_date DESC", (int(query),))
        if query.startswith('@'): return self._execute_search("SELECT m.*, sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id = sc.chat_id WHERE m.username = ? ORDER BY m.message_date DESC", (query.lstrip('@'),))
        return self._execute_search("SELECT m.*, sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id = sc.chat_id JOIN messages_fts fts ON m.id = fts.rowid WHERE fts.message_content MATCH ? ORDER BY m.message_date DESC LIMIT 200", (query,))

    def search_in_chat(self, chat_id, query):
        if query.isdigit(): return self._execute_search("SELECT m.*, sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id = sc.chat_id WHERE m.chat_id = ? AND m.user_id = ? ORDER BY m.message_date DESC", (chat_id, int(query)))
        if query.startswith('@'): return self._execute_search("SELECT m.*, sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id = sc.chat_id WHERE m.chat_id = ? AND m.username = ? ORDER BY m.message_date DESC", (chat_id, query.lstrip('@')))
        return self._execute_search("SELECT m.*, sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id = sc.chat_id JOIN messages_fts fts ON m.id = fts.rowid WHERE m.chat_id = ? AND fts.message_content MATCH ? ORDER BY m.message_date DESC LIMIT 200", (chat_id, query))
    
    def _execute_search(self, query, params):
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            cursor = conn.cursor()
            cursor.execute(query, params)
            return [dict(row) for row in cursor.fetchall()]

    def get_main_stats(self):
        with self._get_connection() as conn:
            cursor = conn.cursor()
            cursor.execute("SELECT COUNT(*), COUNT(DISTINCT user_id) FROM messages")
            msg_stats = cursor.fetchone() or (0, 0)
            cursor.execute("SELECT COUNT(*), COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) FROM scanned_chats")
            chat_stats = cursor.fetchone() or (0, 0)
            return msg_stats, chat_stats

class ChatScanner:
    def __init__(self, config, db_manager):
        self.client = TelegramClient('data/parser_session', config.API_ID, config.API_HASH)
        self.db = db_manager
        self.config = config
    
    async def _worker_loop(self):
        await self.client.connect()
        if not await self.client.is_user_authorized():
            logger.critical("АККАУНТ ПАРСЕРА НЕ АВТОРИЗОВАН. СОЗДАЙТЕ СЕССИЮ ЛОКАЛЬНО.")
            return

        logger.info("Воркер-сканер запущен и готов к задачам.")
        while True:
            task = self.db.get_next_pending_chat()
            if not task:
                await asyncio.sleep(30)
                continue
            
            try:
                self.db.update_chat_status(task['id'], 'SCANNING')
                logger.info(f"Начинаю сканирование задачи #{task['id']}: {task['chat_link']}")
                entity = await self.client.get_entity(task['chat_link'])
                self.db.update_chat_status(task['id'], 'SCANNING', title=entity.title, real_id=entity.id)
                await self.client.join_channel(entity)

                message_batch = []
                msg_count = 0
                async for message in self.client.iter_messages(entity, limit=self.config.PARSER_MESSAGE_LIMIT):
                    if not hasattr(message, 'sender') or not message.sender or not isinstance(message.sender, User) or message.sender.bot: continue
                    user = message.sender
                    message_batch.append((user.id, user.first_name, user.last_name, user.username, message.date.isoformat(),
                                          f"https://t.me/c/{entity.id}/{message.id}", message.text, entity.id))
                    msg_count += 1
                    if len(message_batch) >= 100:
                        self.db.save_message_batch(message_batch)
                        message_batch = []

                if message_batch: self.db.save_message_batch(message_batch)
                
                await self.client.leave_channel(entity)
                self.db.update_chat_status(task['id'], 'COMPLETED', msg_count=msg_count)
                logger.info(f"Задача #{task['id']} успешно завершена. Собрано {msg_count} сообщений.")
            except Exception as e:
                logger.error(f"Ошибка при обработке задачи #{task['id']}: {e}")
                self.db.update_chat_status(task['id'], 'FAILED')

    def start(self):
        threading.Thread(target=lambda: asyncio.run(self._worker_loop()), daemon=True).start()

class BotController:
    def __init__(self, bot, db, config):
        self.bot = bot
        self.db = db
        self.config = config

    def _format_message(self, data, page, total):
        return (f"**Запись {page + 1} из {total}**\n\n"
                f"👤 `{data.get('first_name') or ''} {data.get('last_name') or ''}` (`{data.get('user_id')}`)\n"
                f"🏛 `{data.get('chat_title')}` | `@{data.get('username') or 'N/A'}`\n\n"
                f"```{data.get('message_content') or 'Нет текста'}```")

    def _create_nav_markup(self, page, total, search_type, query):
        row = [
            types.InlineKeyboardButton("◀️", callback_data=f"nav_{search_type}_{page - 1}_{query}") if page > 0 else types.InlineKeyboardButton(" ", callback_data="noop"),
            types.InlineKeyboardButton(f"{page + 1}/{total}", callback_data="noop"),
            types.InlineKeyboardButton("▶️", callback_data=f"nav_{search_type}_{page + 1}_{query}") if page < total - 1 else types.InlineKeyboardButton(" ", callback_data="noop"),
            types.InlineKeyboardButton("❌", callback_data="close")
        ]
        return types.InlineKeyboardMarkup(keyboard=[row])
    
    def _handle_search_results(self, message, results, search_type, query):
        if not results:
            self.bot.reply_to(message, "По вашему запросу ничего не найдено.")
            return
        
        page, total = 0, len(results)
        text = self._format_message(results[page], page, total)
        markup = self._create_nav_markup(page, total, search_type, query)
        self.bot.send_message(message.chat.id, text, reply_markup=markup, parse_mode="Markdown")

    def register_handlers(self):
        @self.bot.message_handler(commands=['start', 'help'])
        def help(message): self.bot.reply_to(message, self.config.HELP_MESSAGE, parse_mode="Markdown")

        @self.bot.message_handler(commands=['my_id'])
        def my_id(message): self.bot.reply_to(message, f"Ваш Telegram ID: `{message.from_user.id}`", parse_mode="Markdown")
        
        @self.bot.message_handler(commands=['stats'])
        def stats(message):
            msg_stats, chat_stats = self.db.get_main_stats()
            self.bot.send_message(message.chat.id, f"**Общая статистика 'Dominatus'**\n\n"
                                  f"▫️ Всего сообщений: `{msg_stats[0]}`\n"
                                  f"▫️ Уникальных пользователей: `{msg_stats[1]}`\n"
                                  f"▫️ Просканировано чатов: `{chat_stats[1]}`\n"
                                  f"▫️ Всего в очереди: `{chat_stats[0]}`", parse_mode="Markdown")

        @self.bot.message_handler(commands=['add_chat'])
        def add_chat(message):
            try:
                chat_link = message.text.split(maxsplit=1)[1]
                if not (chat_link.startswith('@') or 't.me/' in chat_link):
                    self.bot.reply_to(message, "Неверный формат. Укажите ссылку или @username чата.")
                    return
                self.db.add_chat_to_queue(chat_link, message.from_user.id)
                self.bot.reply_to(message, f"Чат `{chat_link}` добавлен в очередь на сканирование.", parse_mode="Markdown")
            except IndexError:
                self.bot.reply_to(message, "Формат команды: `/add_chat <ссылка_на_чат>`")

        @self.bot.message_handler(commands=['status', 'chat_info'])
        def chat_status(message):
            try:
                chat_link = message.text.split(maxsplit=1)[1]
                info = self.db.get_chat_info(chat_link)
                if not info:
                    self.bot.reply_to(message, "Этот чат еще не добавлялся в очередь.")
                    return
                self.bot.reply_to(message, f"**Информация о чате:** `{info['chat_link']}`\n\n"
                                  f"**Название:** `{info['chat_title'] or 'Не определено'}`\n"
                                  f"**Статус:** `{info['status']}`\n"
                                  f"**Найдено сообщений:** `{info['message_count']}`\n"
                                  f"**Последнее сканирование:** `{info['last_scanned'] or 'N/A'}`", parse_mode="Markdown")
            except IndexError:
                self.bot.reply_to(message, "Формат команды: `/status <ссылка_на_чат>`")

        @self.bot.message_handler(commands=['search'])
        def specific_search(message):
            try:
                _, chat_link, query = message.text.split(maxsplit=2)
                chat_info = self.db.get_chat_info(chat_link)
                if not chat_info or not chat_info.get('chat_id'):
                    self.bot.reply_to(message, "Этот чат не был просканирован или ссылка неверна.")
                    return
                results = self.db.search_in_chat(chat_info['chat_id'], query)
                self._handle_search_results(message, results, f"schat_{chat_info['chat_id']}", query)
            except ValueError:
                self.bot.reply_to(message, "Формат: `/search <ссылка_на_чат> <запрос>`")

        @self.bot.message_handler(func=lambda msg: not msg.text.startswith('/'))
        def global_search(message):
            results = self.db.search_globally(message.text)
            self._handle_search_results(message, results, "global", message.text)

        @self.bot.callback_query_handler(func=lambda call: True)
        def pagination(call):
            try:
                cmd, search_type, page_str, query = call.data.split('_', 3)
                if cmd != "nav": return
                page = int(page_str)
                
                if search_type.startswith("schat"):
                    chat_id = int(search_type.split("-")[1])
                    results = self.db.search_in_chat(chat_id, query)
                else:
                    results = self.db.search_globally(query)

                if not results: self.bot.answer_callback_query(call.id, text="Данные не найдены."); return
                
                self.bot.edit_message_text(self._format_message(results[page], page, len(results)), 
                                           call.message.chat.id, call.message.message_id, 
                                           reply_markup=self._create_nav_markup(page, len(results), search_type, query), parse_mode="Markdown")
            except (ValueError, IndexError):
                if call.data == "close": self.bot.delete_message(call.message.chat.id, call.message.message_id)
                elif call.data != "noop": logger.warning(f"Некорректный callback: {call.data}")
                self.bot.answer_callback_query(call.id)
            except Exception as e:
                logger.error(f"Общая ошибка пагинации: {e}")

class DominatusApp:
    def __init__(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s')
        self.logger = logging.getLogger(__name__)
        self.config = Config()
        self.db = DatabaseManager(self.config.DB_PATH)
        self.bot = TeleBot(self.config.BOT_TOKEN)
        self.scanner = ChatScanner(self.config, self.db)
        self.controller = BotController(self.bot, self.db, self.config)
        self.server = Flask(__name__)
    
    def _setup_web_routes(self):
        @self.server.route(f'/{self.config.BOT_TOKEN}', methods=['POST'])
        def process_webhook():
            self.controller.bot.process_new_updates([types.Update.de_json(request.get_data().decode('utf-8'))])
            return '', 200
        @self.server.route('/')
        def index(): return "Протокол 'Dominatus' активен."

    def _startup_background_tasks(self):
        self.logger.info("Запуск фоновых задач...")
        self.scanner.start()
        render_url = os.environ.get('RENDER_EXTERNAL_URL')
        if render_url:
            def guardian():
                while True: 
                    time.sleep(600)
                    try: requests.get(render_url, timeout=10)
                    except Exception: pass
            threading.Thread(target=guardian, daemon=True).start()

    def run(self):
        self.logger.info("Инициализация протокола 'Dominatus'...")
        self.controller.register_handlers()
        self._setup_web_routes()
        threading.Timer(5.0, self._startup_background_tasks).start()
        self.logger.info("Протокол готов. Gunicorn принимает управление.")
        return self.server

if __name__ == '__main__':
    app = DominatusApp()
    server = app.run()
