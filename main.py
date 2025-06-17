import os
import sqlite3
import logging
import threading
import time
import asyncio
import requests
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
    DB_PATH = 'data/dominatus_public.db'
    HELP_MESSAGE = (
        "**Протокол 'Dominatus'**\n\n"
        "**1. Добавление чата на сканирование:**\n`/add_chat <ссылка_на_чат_или_@username>`\n\n"
        "**2. Глобальный поиск (по всем чатам):**\nПросто отправьте ID, `@username` или `текст`.\n\n"
        "**3. Поиск в конкретном чате:**\n`/search <ссылка_на_чат> <запрос>`\n\n"
        "**4. Утилиты:**\n`/status <ссылка>` - узнать статус сканирования\n`/chat_info <ссылка>` - статистика по чату\n`/stats` - общая статистика\n`/my_id` - ваш Telegram ID"
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
            cursor.executescript('''
                CREATE TABLE IF NOT EXISTS messages (
                    id INTEGER PRIMARY KEY, user_id INTEGER NOT NULL,
                    first_name TEXT, last_name TEXT, username TEXT,
                    message_date TEXT NOT NULL, message_link TEXT NOT NULL UNIQUE,
                    message_content TEXT, chat_id INTEGER NOT NULL
                );
                CREATE TABLE IF NOT EXISTS scanned_chats (
                    id INTEGER PRIMARY KEY, chat_link TEXT NOT NULL UNIQUE,
                    chat_id INTEGER, chat_title TEXT, submitter_id INTEGER NOT NULL, 
                    status TEXT NOT NULL, message_count INTEGER DEFAULT 0, last_scanned TEXT
                );
                CREATE INDEX IF NOT EXISTS idx_user_id ON messages (user_id);
                CREATE INDEX IF NOT EXISTS idx_chat_id ON messages (chat_id);
                CREATE VIRTUAL TABLE IF NOT EXISTS messages_fts USING fts5(message_content, content="messages", content_rowid="id");
                CREATE TRIGGER IF NOT EXISTS messages_after_insert AFTER INSERT ON messages BEGIN
                    INSERT INTO messages_fts(rowid, message_content) VALUES (new.id, new.message_content);
                END;
            ''')

    def add_chat_to_queue(self, chat_link, submitter_id):
        with self._get_connection() as conn:
            conn.execute("INSERT OR IGNORE INTO scanned_chats (chat_link, submitter_id, status) VALUES (?, ?, ?)", (chat_link, submitter_id, 'PENDING'))

    def get_next_pending_chat(self):
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute("SELECT * FROM scanned_chats WHERE status = 'PENDING' ORDER BY id LIMIT 1").fetchone()

    def update_chat_status(self, task_id, status, **kwargs):
        with self._get_connection() as conn:
            updates = [f"{key} = ?" for key in kwargs]
            params = list(kwargs.values())
            query = f"UPDATE scanned_chats SET status = ?, last_scanned = datetime('now')"
            if updates:
                query += ", " + ", ".join(updates)
            query += " WHERE id = ?"
            params = [status] + params + [task_id]
            conn.execute(query, params)
    
    def get_chat_info(self, chat_link):
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            return conn.execute("SELECT * FROM scanned_chats WHERE chat_link = ?", (chat_link,)).fetchone()

    def save_message_batch(self, messages):
        with self._get_connection() as conn:
            conn.executemany('INSERT OR IGNORE INTO messages (user_id, first_name, last_name, username, message_date, message_link, message_content, chat_id) VALUES (?, ?, ?, ?, ?, ?, ?, ?)', messages)

    def search_globally(self, query):
        if query.isdigit(): return self._execute_search("SELECT m.*, sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id = sc.chat_id WHERE m.user_id = ? ORDER BY m.message_date DESC", (int(query),))
        if query.startswith('@'): return self._execute_search("SELECT m.*, sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id = sc.chat_id WHERE m.username = ? ORDER BY m.message_date DESC", (query.lstrip('@'),))
        return self._execute_search("SELECT m.*, sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id = sc.chat_id JOIN messages_fts fts ON m.id = fts.rowid WHERE fts.message_content MATCH ? ORDER BY m.message_date DESC LIMIT 200", (f'"{query}"',))

    def search_in_chat(self, chat_id, query):
        if query.isdigit(): return self._execute_search("SELECT m.*, sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id = sc.chat_id WHERE m.chat_id = ? AND m.user_id = ? ORDER BY m.message_date DESC", (chat_id, int(query)))
        if query.startswith('@'): return self._execute_search("SELECT m.*, sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id = sc.chat_id WHERE m.chat_id = ? AND m.username = ? ORDER BY m.message_date DESC", (chat_id, query.lstrip('@')))
        return self._execute_search("SELECT m.*, sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id = sc.chat_id JOIN messages_fts fts ON m.id = fts.rowid WHERE m.chat_id = ? AND fts.message_content MATCH ? ORDER BY m.message_date DESC LIMIT 200", (chat_id, f'"{query}"'))
    
    def _execute_search(self, query, params):
        with self._get_connection() as conn:
            conn.row_factory = sqlite3.Row
            return [dict(row) for row in conn.execute(query, params).fetchall()]

    def get_main_stats(self):
        with self._get_connection() as conn:
            msg_stats = conn.execute("SELECT COUNT(*), COUNT(DISTINCT user_id) FROM messages").fetchone() or (0, 0)
            chat_stats = conn.execute("SELECT COUNT(*), COUNT(CASE WHEN status = 'COMPLETED' THEN 1 END) FROM scanned_chats").fetchone() or (0, 0)
            return msg_stats, chat_stats

class ChatScanner:
    def __init__(self, config, db_manager):
        self.client = TelegramClient('data/parser_session', config.API_ID, config.API_HASH)
        self.db = db_manager
        self.config = config
    
    async def _worker_loop(self):
        await self.client.connect()
        if not await self.client.is_user_authorized():
            logging.critical("АККАУНТ ПАРСЕРА НЕ АВТОРИЗОВАН.")
            return

        logging.info("Воркер-сканер запущен.")
        while True:
            task_row = self.db.get_next_pending_chat()
            if not task_row:
                await asyncio.sleep(30); continue
            
            task = dict(task_row)
            try:
                self.db.update_chat_status(task['id'], 'SCANNING')
                logging.info(f"Сканирую задачу #{task['id']}: {task['chat_link']}")
                entity = await self.client.get_entity(task['chat_link'])
                self.db.update_chat_status(task['id'], 'SCANNING', chat_title=getattr(entity, 'title', 'N/A'), chat_id=entity.id)
                await self.client.join_channel(entity)

                message_batch, msg_count = [], 0
                async for message in self.client.iter_messages(entity, limit=self.config.PARSER_MESSAGE_LIMIT):
                    if not hasattr(message, 'sender') or not message.sender or not isinstance(message.sender, User) or message.sender.bot: continue
                    user = message.sender
                    message_batch.append((user.id, user.first_name, user.last_name, user.username, message.date.isoformat(),
                                          f"https://t.me/c/{entity.id}/{message.id}", message.text, entity.id))
                    msg_count += 1
                    if len(message_batch) >= 100:
                        self.db.save_message_batch(message_batch); message_batch = []
                if message_batch: self.db.save_message_batch(message_batch)
                self.db.update_chat_status(task['id'], 'COMPLETED', message_count=msg_count)
                logging.info(f"Задача #{task['id']} завершена. Собрано {msg_count} сообщений.")
            except Exception as e:
                logging.error(f"Ошибка задачи #{task['id']}: {e}")
                self.db.update_chat_status(task['id'], 'FAILED')

    def start(self):
        threading.Thread(target=lambda: asyncio.run(self._worker_loop()), daemon=True).start()

class BotController:
    def __init__(self, bot, db, config):
        self.bot, self.db, self.config = bot, db, config
        self.register_handlers()

    def _format_message(self, data, page, total):
        return (f"**Запись {page + 1} из {total}** | `{data.get('chat_title')}`\n\n"
                f"👤 `{data.get('first_name') or ''} {data.get('last_name') or ''}` (`{data.get('user_id')}`)\n\n"
                f"```{data.get('message_content') or 'Нет текста'}```")

    def _create_nav_markup(self, page, total, search_type, query):
        row = [types.InlineKeyboardButton("◀️", callback_data=f"nav_{search_type}_{page - 1}_{query}") if page > 0 else types.InlineKeyboardButton(" ", callback_data="noop"),
               types.InlineKeyboardButton(f"{page + 1}/{total}", callback_data="noop"),
               types.InlineKeyboardButton("▶️", callback_data=f"nav_{search_type}_{page + 1}_{query}") if page < total - 1 else types.InlineKeyboardButton(" ", callback_data="noop"),
               types.InlineKeyboardButton("❌", callback_data="close")]
        return types.InlineKeyboardMarkup(keyboard=[row])
    
    def _handle_search_results(self, message, results, search_type, query):
        if not results: self.bot.reply_to(message, "Ничего не найдено."); return
        page, total = 0, len(results)
        self.bot.send_message(message.chat.id, self._format_message(results[page], page, total), 
                              reply_markup=self._create_nav_markup(page, total, search_type, query), parse_mode="Markdown")

    def register_handlers(self):
        @self.bot.message_handler(commands=['start', 'help'])
        def help(message): self.bot.reply_to(message, self.config.HELP_MESSAGE, parse_mode="Markdown")

        @self.bot.message_handler(commands=['my_id'])
        def my_id(message): self.bot.reply_to(message, f"`{message.from_user.id}`", parse_mode="Markdown")
        
        @self.bot.message_handler(commands=['stats'])
        def stats(message):
            msg_stats, chat_stats = self.db.get_main_stats()
            self.bot.send_message(message.chat.id, f"**Статистика 'Dominatus'**\n\n- Сообщений: `{msg_stats[0]}`\n- Пользователей: `{msg_stats[1]}`\n- Чатов (завершено/всего): `{chat_stats[1]}/{chat_stats[0]}`", parse_mode="Markdown")

        @self.bot.message_handler(commands=['add_chat'])
        def add_chat(message):
            try:
                chat_link = message.text.split(maxsplit=1)[1]
                if not (chat_link.startswith('@') or 't.me/' in chat_link):
                    self.bot.reply_to(message, "Неверный формат. Укажите ссылку или @username чата.")
                    return
                self.db.add_chat_to_queue(chat_link, message.from_user.id)
                self.bot.reply_to(message, f"`{chat_link}` добавлен в очередь на сканирование.", parse_mode="Markdown")
            except IndexError:
                self.bot.reply_to(message, "Формат команды: `/add_chat <ссылка_на_чат>`")

        @self.bot.message_handler(commands=['status', 'chat_info'])
        def chat_status(message):
            try:
                chat_link = message.text.split(maxsplit=1)[1]
                info = self.db.get_chat_info(chat_link)
                if not info: self.bot.reply_to(message, "Этот чат еще не добавлялся в очередь."); return
                info_dict = dict(info)
                self.bot.reply_to(message, f"**Инфо:** `{info_dict.get('chat_link')}`\n**Статус:** `{info_dict.get('status')}`\n**Сообщений:** `{info_dict.get('message_count')}`", parse_mode="Markdown")
            except IndexError:
                self.bot.reply_to(message, "Формат команды: `/status <ссылка_на_чат>`")

        @self.bot.message_handler(commands=['search'])
        def specific_search(message):
            try:
                _, chat_link, query = message.text.split(maxsplit=2)
                chat_info = self.db.get_chat_info(chat_link)
                if not chat_info or not chat_info.get('chat_id'): self.bot.reply_to(message, "Чат не просканирован."); return
                results = self.db.search_in_chat(chat_info['chat_id'], query)
                self._handle_search_results(message, results, f"schat_{chat_info['chat_id']}", query)
            except ValueError:
                self.bot.reply_to(message, "Формат: `/search <ссылка_на_чат> <запрос>`")

        @self.bot.message_handler(func=lambda msg: msg.text and not msg.text.startswith('/'))
        def global_search(message):
            self._handle_search_results(message, self.db.search_globally(message.text), "global", message.text)

        @self.bot.callback_query_handler(func=lambda call: True)
        def pagination(call):
            if call.data == "close": self.bot.delete_message(call.message.chat.id, call.message.message_id); return
            if call.data == "noop": self.bot.answer_callback_query(call.id); return
            try:
                cmd, search_type, page_str, query = call.data.split('_', 3)
                page = int(page_str)
                results = self.db.search_in_chat(int(search_type.split("-")[1]), query) if search_type.startswith("schat") else self.db.search_globally(query)
                if not results: self.bot.answer_callback_query(call.id, text="Данные не найдены."); return
                self.bot.edit_message_text(self._format_message(results[page], page, len(results)), 
                                           call.message.chat.id, call.message.message_id, 
                                           reply_markup=self._create_nav_markup(page, len(results), search_type, query), parse_mode="Markdown")
            except (ApiTelegramException, ValueError, IndexError) as e:
                logging.error(f"Ошибка обработки callback: {e}")
                self.bot.answer_callback_query(call.id, "Ошибка.")

class DominatusApp:
    def __init__(self):
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - [%(levelname)s] - %(name)s - %(message)s')
        self.config = Config()
        self.db = DatabaseManager(self.config.DB_PATH)
        self.bot = TeleBot(self.config.BOT_TOKEN)
        self.scanner = ChatScanner(self.config, self.db)
        self.controller = BotController(self.bot, self.db, self.config)
        self.server = Flask(__name__)
        self._setup_web_routes()

    def _setup_web_routes(self):
        @self.server.route(f'/{self.config.BOT_TOKEN}', methods=['POST'])
        def process_webhook(): self.bot.process_new_updates([types.Update.de_json(request.get_data().decode('utf-8'))]); return '', 200
        @self.server.route('/')
        def index(): return "Протокол 'Dominatus' активен."

    def _startup_background_tasks(self):
        logging.info("Запуск фоновых задач...")
        self.scanner.start()
        render_url = os.environ.get('RENDER_EXTERNAL_URL')
        if render_url:
            def guardian():
                while True: 
                    time.sleep(600)
                    try: requests.get(render_url, timeout=10)
                    except: pass
            threading.Thread(target=guardian, daemon=True).start()

    def initialize(self):
        logging.info("Инициализация протокола 'Dominatus'...")
        self._startup_background_tasks()
        logging.info("Протокол готов. Gunicorn принимает управление.")
        return self.server

app = DominatusApp()
server = app.initialize()
