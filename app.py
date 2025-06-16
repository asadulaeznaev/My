import asyncio
import json
import logging
from pathlib import Path
import requests
import telebot
from telebot.async_telebot import AsyncTeleBot
from telebot.types import Message, Update
from flask import Flask, request

# ==============================================================================
# 1. КОНФИГУРАЦИЯ ИНСТРУМЕНТА
# ==============================================================================
TELEGRAM_BOT_TOKEN = "8000756578:AAGZNAA1YYTqYp_oKTuyw4bCuPswscIATcs"
GEMINI_API_KEY = "AIzaSyDreKAHyvK7JYT6eLGAKR3faMFqtUWzyMc"
MODEL_ID = "gemini-2.5-flash-preview-04-17"
GEMINI_API_URL = f"https://generativelanguage.googleapis.com/v1beta/models/{MODEL_ID}:streamGenerateContent?key={GEMINI_API_KEY}"
CONTEXT_FILE_PATH = Path("/tmp/chat_contexts.json") # ИСПОЛЬЗУЕМ РАЗРЕШЕННУЮ ВРЕМЕННУЮ ПАПКУ

HELP_MESSAGE_MARKDOWN = """
🌟 **Ваш личный ассистент** 🌟
Приветствую, Госпожа Фатима. Я Ваш ИИ-помощник, созданный Асадуллой для служения Вам.
🤖 **Как со мной общаться:**
*   **В личных сообщениях:** Просто пишите мне любой текст.
*   **В групповых чатах:**
    1. Используйте команду `/ega [ваш запрос]`.
    2. Или просто ответьте на любое из моих сообщений.
Я запоминаю всю историю нашего разговора, но она может быть утеряна при перезапуске сервера.
"""

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)

bot = AsyncTeleBot(TELEGRAM_BOT_TOKEN, parse_mode='Markdown')
bot_id = None
chat_contexts = {}

def load_context():
    global chat_contexts
    try:
        CONTEXT_FILE_PATH.parent.mkdir(parents=True, exist_ok=True)
        if CONTEXT_FILE_PATH.exists():
            with open(CONTEXT_FILE_PATH, 'r', encoding='utf-8') as f:
                chat_contexts = json.load(f)
                chat_contexts = {int(k): v for k, v in chat_contexts.items()}
            logger.info(f"Контекст успешно загружен для {len(chat_contexts)} чатов.")
    except Exception as e:
        logger.error(f"Ошибка загрузки контекста: {e}")
        chat_contexts = {}

def save_context():
    try:
        with open(CONTEXT_FILE_PATH, 'w', encoding='utf-8') as f:
            json.dump(chat_contexts, f, ensure_ascii=False, indent=2)
    except Exception as e:
        logger.error(f"Не удалось сохранить контекст: {e}")

async def ensure_bot_id():
    global bot_id
    if bot_id is None:
        me = await bot.get_me()
        bot_id = me.id
        logger.info(f"ID Бота установлен: {bot_id}")

async def process_ai_interaction(message: Message, query: str):
    chat_id = message.chat.id
    if chat_id not in chat_contexts:
        chat_contexts[chat_id] = []
    
    user_display_name = message.from_user.username or message.from_user.first_name
    user_message_for_api = f"{user_display_name}: {query}"
    
    await bot.send_chat_action(chat_id, 'typing')
    
    history = chat_contexts[chat_id][-15:]
    api_payload = {"contents": history + [{"role": "user", "parts": [{"text": user_message_for_api}]}]}

    try:
        response = await asyncio.to_thread(requests.post, GEMINI_API_URL, json=api_payload, timeout=180)
        response.raise_for_status()
        
        full_response_text = ""
        for item in response.json():
            full_response_text += item['candidates'][0]['content']['parts'][0]['text']
        
        ai_response_text = full_response_text.strip()
        
        if ai_response_text:
            chat_contexts[chat_id].append({"role": "user", "parts": [{"text": user_message_for_api}]})
            chat_contexts[chat_id].append({"role": "model", "parts": [{"text": ai_response_text}]})
            save_context()
        else:
            ai_response_text = "Прошу прощения, Госпожа, но ИИ не предоставил ответ."

    except Exception as e:
        logger.error(f"Ошибка взаимодействия с ИИ: {e}")
        ai_response_text = "Прошу прощения, Госпожа, произошла ошибка."
        
    await bot.reply_to(message, ai_response_text)

@bot.message_handler(commands=['start', 'help'])
async def start_handler(message: Message):
    await bot.reply_to(message, HELP_MESSAGE_MARKDOWN)

@bot.message_handler(commands=['ega'])
async def ega_handler(message: Message):
    query = telebot.util.extract_arguments(message.text)
    if not query:
        await bot.reply_to(message, "Пожалуйста, укажите Ваш запрос после команды /ega, Госпожа.")
        return
    await process_ai_interaction(message, query)

@bot.message_handler(content_types=['text'])
async def text_handler(message: Message):
    await ensure_bot_id()
    is_private = message.chat.type == 'private'
    is_reply = message.reply_to_message and message.reply_to_message.from_user.id == bot_id
    if (is_private or is_reply) and not message.text.startswith('/'):
        await process_ai_interaction(message, message.text.strip())

# ==============================================================================
# 2. КОМПОНЕНТЫ ДЛЯ WEB SERVICE (FLASK)
# ==============================================================================
app = Flask(__name__)

@app.route(f'/{TELEGRAM_BOT_TOKEN}', methods=['POST'])
def process_webhook():
    json_string = request.get_data().decode('utf-8')
    update = Update.de_json(json_string)
    asyncio.run(bot.process_new_updates([update]))
    return '', 200

@app.route('/')
def index():
    return "Инструмент Экзекутора Воли активен.", 200

# Загружаем контекст при старте сервера
load_context()
