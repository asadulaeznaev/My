import os
import asyncio
import logging
import threading
from flask import Flask
from telebot import TeleBot, types
from telethon import TelegramClient
from telethon.tl.types import User
import nest_asyncio

nest_asyncio.apply()

from dominatus.core.config import config

bot = TeleBot(config.BOT_TOKEN)
logging.basicConfig(level=logging.INFO)

async def perform_live_search(chat_link, query):
    results = []
    session_path = 'data/parser_session'
    os.makedirs(os.path.dirname(session_path), exist_ok=True)
    client = TelegramClient(session_path, int(config.API_ID), config.API_HASH)

    try:
        await client.connect()
        if not await client.is_user_authorized():
            logging.critical("Parser account auth failed.")
            return "AUTH_FAILED"

        entity = await client.get_entity(chat_link)
        chat_title = getattr(entity, 'title', 'N/A')
        
        limit = config.PARSER_MESSAGE_LIMIT
        query_lower = query.lower()

        async for msg in client.iter_messages(entity, limit=limit):
            if not msg or not msg.text or not msg.sender or not isinstance(msg.sender, User):
                continue
            
            if query_lower in msg.text.lower():
                u = msg.sender
                results.append({
                    'chat_title': chat_title,
                    'first_name': u.first_name,
                    'last_name': u.last_name,
                    'user_id': u.id,
                    'message_content': msg.text,
                    'message_link': f"https://t.me/c/{entity.id}/{msg.id}"
                })
                if len(results) >= 200:
                    break
    
    except Exception as e:
        logging.error(f"Live search for {chat_link} failed: {e}", exc_info=True)
        return "ERROR"
    finally:
        if client.is_connected():
            await client.disconnect()

    return results

def _reply(msg, text):
    bot.reply_to(msg, text, parse_mode="Markdown")

@bot.message_handler(commands=['start', 'help'])
def help_cmd(m):
    _reply(m, config.HELP_MESSAGE)

@bot.message_handler(commands=['my_id'])
def my_id_cmd(m):
    _reply(m, f"Ваш ID: `{m.from_user.id}`")

@bot.message_handler(commands=['search'])
def search_cmd(m):
    try:
        _, link, query = m.text.split(maxsplit=2)
    except ValueError:
        _reply(m, "Формат: `/search <ссылка> <запрос>`")
        return

    reply_msg = bot.reply_to(m, "⏳ Начинаю поиск в реальном времени. Это может занять несколько минут...")

    def search_thread():
        loop = asyncio.new_event_loop()
        asyncio.set_event_loop(loop)
        
        results = loop.run_until_complete(perform_live_search(link, query))
        
        if results == "AUTH_FAILED":
            bot.edit_message_text("❗️ **Ошибка:** Не удалось авторизоваться в аккаунте парсера. Проверьте сессию.", reply_msg.chat.id, reply_msg.message_id)
        elif results == "ERROR" or not results:
            bot.edit_message_text("🤷‍♂️ Ничего не найдено или произошла ошибка при поиске.", reply_msg.chat.id, reply_msg.message_id)
        else:
            output = f"✅ **Найдено {len(results)} сообщений по запросу «{query}»:**\n\n"
            for item in results[:20]:
                output += (
                    f"*{item.get('chat_title')}*\n"
                    f"👤 `{item.get('first_name') or ''} {item.get('last_name') or ''}`:\n"
                    f"💬 ```{item.get('message_content')[:200]}```\n"
                    f"[🔗 Перейти]({item.get('message_link')})\n---\n"
                )
            bot.edit_message_text(output, reply_msg.chat.id, reply_msg.message_id, 
                                  parse_mode="Markdown", disable_web_page_preview=True)

    threading.Thread(target=search_thread).start()

def run_bot():
    print("Bot is running in polling mode...")
    bot.polling(none_stop=True)

if __name__ == '__main__':
    run_bot()
