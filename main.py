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

# ==============================================================================
# 1. КОНФИГУРАЦИЯ И СЕКРЕТНЫЕ ДАННЫЕ
# ==============================================================================
BOT_TOKEN = "8124170502:AAGu0S-gdIJa8Mk-TXa74pIs6_aG8FyWS_E"
API_ID = 2040
API_HASH = "b18441a1ff607e10a989891a5462e627"
# ADMIN_ID УДАЛЕН - ПРОВЕРКА БОЛЬШЕ НЕ НУЖНА

PARSER_MESSAGE_LIMIT = 300
PARSER_CHAT_BLACKLIST = ['новости', 'ставки', 'крипто', 'news', 'crypto', 'bets']
DB_PATH = 'data/citadel_monolith.db'

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

os.makedirs(os.path.dirname(DB_PATH), exist_ok=True)

# ==============================================================================
# 2. КЛАСС УПРАВЛЕНИЯ БАЗОЙ ДАННЫХ
# ==============================================================================
class DatabaseManager:
    def __init__(self, db_path):
        self.db_path = db_path
        self.logger = logging.getLogger(__name__)

    def _get_connection(self):
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
        logger.info("База данных и индекс успешно инициализированы.")

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
    
    async def _scan_loop(self):
        async with self.client:
            logger.info("Клиент Telethon запущен.")
            while True:
                try:
                    logger.info("Начинаю новый цикл сканирования...")
                    dialogs = await self.client.get_dialogs()
                    for dialog in dialogs:
                        if any(word in dialog.name.lower() for word in PARSER_CHAT_BLACKLIST): continue
                        if not (dialog.is_group or dialog.is_channel): continue
                        async for message in self.client.iter_messages(dialog.id, limit=PARSER_MESSAGE_LIMIT):
                            if not hasattr(message, 'sender') or not message.sender or not isinstance(message.sender, User) or message.sender.bot: continue
                            
                            chat_username = f"c/{dialog.entity.id}" if hasattr(dialog.entity, 'id') else dialog.entity.username
                            msg_link = f"https://t.me/{chat_username}/{message.id}"

                            user = message.sender
                            self.db_manager.save_message({
                                "user_id": user.id, "first_name": user.first_name, "last_name": user.last_name,
                                "username": user.username, "phone": user.phone, "message_date": message.date.isoformat(),
                                "message_link": msg_link, "message_content": message.text,
                                "media_count": 1 if message.media else 0, "chat_name": dialog.name, "chat_id": dialog.id
                            })
                except Exception as e:
                    logger.error(f"Ошибка в цикле парсера: {e}")
                
                logger.info("Цикл парсинга завершен. Пауза 15 минут.")
                await asyncio.sleep(15 * 60)

    def _run_in_new_loop(self):
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        loop.run_until_complete(self._scan_loop())

    def start(self):
        threading.Thread(target=self._run_in_new_loop, daemon=True).start()

# ==============================================================================
# 4. ОБРАБОТЧИКИ БОТА И ВЕБ-СЕРВЕР (ПУБЛИЧНЫЙ ДОСТУП)
# ==============================================================================
db = DatabaseManager(DB_PATH)
bot = TeleBot(BOT_TOKEN)
parser = Parser(API_ID, API_HASH, db)
server = Flask(__name__)

@server.route(f'/{BOT_TOKEN}', methods=['POST'])
def process_webhook():
    update = types.Update.de_json(request.get_data().decode('utf-8'))
    bot.process_new_updates([update])
    return '', 200

@server.route('/')
def index():
    return "Экзекутор Воли: Протокол Монолит-Публичный активен.", 200

# ФУНКЦИЯ _is_admin УДАЛЕНА

def _format_message(data, page, total):
    return (
        f"**Запись {page + 1} из {total}**\n\n"
        f"👤 **Пользователь:** `{data.get('first_name') or ''} {data.get('last_name') or ''}`\n"
        f"**ID:** `{data.get('user_id')}` | **Юзернейм:** `@{data.get('username') or 'N/A'}`\n\n"
        f"📅 **Дата:** `{data.get('message_date')}`\n"
        f"🏛 **Чат:** `{data.get('chat_name')}`\n\n"
        f"📜 **Сообщение:**\n"
        f"```\n{data.get('message_content') or 'Нет текста'}\n```"
    )

def _create_navigation_markup(page, total, user_id):
    markup = types.InlineKeyboardMarkup()
    row = [
        types.InlineKeyboardButton("◀️", callback_data=f"page_{page - 1}_{user_id}") if page > 0 else types.InlineKeyboardButton(" ", callback_data="page_noop"),
        types.InlineKeyboardButton(f"{page + 1}/{total}", callback_data="page_noop"),
        types.InlineKeyboardButton("▶️", callback_data=f"page_{page + 1}_{user_id}") if page < total - 1 else types.InlineKeyboardButton(" ", callback_data="page_noop"),
        types.InlineKeyboardButton("❌", callback_data="page_close")
    ]
    markup.row(*row)
    return markup

@bot.message_handler(commands=['start']) # func=_is_admin УДАЛЕН
def send_welcome(message): bot.reply_to(message, "Бот активен. Введите ID пользователя для поиска.")

@bot.message_handler(commands=['stats']) # func=_is_admin УДАЛЕН
def send_stats(message):
    total, unique = db.get_stats()
    bot.send_message(message.chat.id, f"**Статистика БД**\n- Всего сообщений: `{total}`\n- Уникальных пользователей: `{unique}`", parse_mode="Markdown")

@bot.message_handler(func=lambda msg: msg.text and msg.text.isdigit()) # func=_is_admin УДАЛЕН
def handle_user_id(message):
    results = db.search_user(int(message.text))
    if not results:
        bot.reply_to(message, "Пользователь не найден в базе данных.")
        return
    page, total = 0, len(results)
    bot.send_message(message.chat.id, _format_message(results[page], page, total), 
                      reply_markup=_create_navigation_markup(page, total, int(message.text)), parse_mode="Markdown")

@bot.callback_query_handler(func=lambda call: True) # func=_is_admin УДАЛЕН И ЗАМЕНЕН
def handle_pagination(call):
    try:
        action = call.data.split("_")[1]
        if action == "noop": bot.answer_callback_query(call.id); return
        if action == "close": bot.delete_message(call.message.chat.id, call.message.message_id); return
        
        page, user_id = int(action), int(call.data.split("_")[2])
        results = db.search_user(user_id)
        if not results: bot.answer_callback_query(call.id, text="Данные не найдены."); return
        
        total = len(results)
        bot.edit_message_text(_format_message(results[page], page, total), 
                               call.message.chat.id, call.message.message_id, 
                               reply_markup=_create_navigation_markup(page, total, user_id), parse_mode="Markdown")
    except ApiTelegramException as e:
        logger.error(f"Ошибка API Telegram: {e}")
    except Exception as e:
        logger.error(f"Общая ошибка пагинации: {e}")

def guardian_thread_func():
    render_url = os.environ.get('RENDER_EXTERNAL_URL')
    if not render_url:
        logger.warning("'Страж' неактивен: RENDER_EXTERNAL_URL не найдена.")
        return
    while True:
        time.sleep(15)
        try: requests.get(render_url, timeout=10)
        except Exception: pass
        time.sleep(10 * 60)

# ==============================================================================
# 5. ИНИЦИАЛИЗАЦИЯ
# ==============================================================================
db.init_db()
parser.start()
threading.Thread(target=guardian_thread_func, daemon=True).start()
logger.info("Все системы инициализированы и готовы к запуску через Gunicorn.")
