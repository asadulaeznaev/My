import logging
import threading
import time
import requests
from flask import Flask, request
from telebot import TeleBot, types

from dominatus.core.config import config
from dominatus.core.database import DatabaseManager
from dominatus.worker.tasks import scan_chat_task

db = DatabaseManager(config.DATABASE_URL)
bot = TeleBot(config.BOT_TOKEN, threaded=False)

def _create_nav_markup(page, total_pages, q_type, q_data):
    if total_pages <= 1: return None
    
    row = []
    if page > 0:
        row.append(types.InlineKeyboardButton("◀️", callback_data=f"nav:{q_type}:{page - 1}:{q_data}"))
    else:
        row.append(types.InlineKeyboardButton(" ", callback_data="noop"))
        
    row.append(types.InlineKeyboardButton(f"{page + 1}/{total_pages}", callback_data="noop"))
    
    if page < total_pages - 1:
        row.append(types.InlineKeyboardButton("▶️", callback_data=f"nav:{q_type}:{page + 1}:{q_data}"))
    else:
        row.append(types.InlineKeyboardButton(" ", callback_data="noop"))
        
    row.append(types.InlineKeyboardButton("❌", callback_data="close"))
    return types.InlineKeyboardMarkup([row])

def _format_page(item, page, total):
    return (
        f"📄 **Результат {page + 1} из {total}**\n"
        f"*{item.get('chat_title', 'N/A')}*\n\n"
        f"👤 `{item.get('first_name') or ''} {item.get('last_name') or ''} ({item.get('user_id')})`:\n"
        f"💬 ```{item.get('message_content') or 'Нет текста'}```\n"
        f"[🔗 Перейти к сообщению]({item.get('message_link')})"
    )

def _send_paginated_results(msg, results, q_type, q_data):
    if not results:
        bot.reply_to(msg, "🤷‍♂️ Ничего не найдено.")
        return
    
    page = 0
    total_pages = len(results)
    text = _format_page(results[page], page, total_pages)
    markup = _create_nav_markup(page, total_pages, q_type, q_data)
    
    bot.send_message(msg.chat.id, text, reply_to_message_id=msg.message_id, 
                     parse_mode="Markdown", reply_markup=markup, disable_web_page_preview=True)

@bot.callback_query_handler(func=lambda call: True)
def handle_callback(call):
    if call.data == "noop":
        bot.answer_callback_query(call.id)
        return
    if call.data == "close":
        bot.delete_message(call.message.chat.id, call.message.message_id)
        bot.answer_callback_query(call.id, "Закрыто.")
        return
    try:
        _, q_type, page_str, q_data = call.data.split(":", 3)
        page = int(page_str)
        
        if q_type == 'global':
            results = db.search_all(q_data)
        else:
            chat_id = int(q_type)
            results = db.search_one(chat_id, q_data)

        if not results:
            bot.answer_callback_query(call.id, "Ошибка: результаты не найдены.")
            return

        total_pages = len(results)
        text = _format_page(results[page], page, total_pages)
        markup = _create_nav_markup(page, total_pages, q_type, q_data)
        
        bot.edit_message_text(text, call.message.chat.id, call.message.message_id, 
                              parse_mode="Markdown", reply_markup=markup, disable_web_page_preview=True)
    except Exception as e:
        logging.error(f"Callback error: {e}")
        bot.answer_callback_query(call.id, "❗️ Ошибка обработки.")

def _reply(msg, text):
    bot.reply_to(msg, text, parse_mode="Markdown")

@bot.message_handler(commands=['start', 'help'])
def help_cmd(m): _reply(m, config.HELP_MESSAGE)

@bot.message_handler(commands=['my_id'])
def my_id_cmd(m): _reply(m, f"Ваш ID: `{m.from_user.id}`")
    
@bot.message_handler(commands=['stats'])
def stats_cmd(m):
    msg, chat = db.get_stats()
    _reply(m, f"📈 **Статистика**\n*Сообщений:* {msg['count']}\n*Пользователей:* {msg['count1']}\n*Чатов (готово/всего):* {chat['count1']}/{chat['count']}")

@bot.message_handler(commands=['add'])
def add_cmd(m):
    try:
        link = m.text.split(maxsplit=1)[1]
        db.add_chat(link, m.from_user.id)
        scan_chat_task.delay(link)
        _reply(m, f"✅ `{link}`\nПринято в очередь на сканирование.")
    except IndexError: _reply(m, "Формат: `/add <ссылка>`")

@bot.message_handler(commands=['status'])
def status_cmd(m):
    try:
        link = m.text.split(maxsplit=1)[1]
        info = db.get_chat(link)
        if not info: _reply(m, "🤷‍♂️ Чат не найден в базе."); return
        status_emoji = {"PENDING": "⏳", "SCANNING": "🔄", "COMPLETED": "✅", "FAILED": "❗️"}
        s = info['status']
        _reply(m, f"**{info.get('chat_title', info['chat_link'])}**\n{status_emoji.get(s, '')} *Статус:* {s}\n*Собрано сообщений:* {info['message_count']}")
    except IndexError: _reply(m, "Формат: `/status <ссылка>`")

@bot.message_handler(commands=['search'])
def search_cmd(m):
    try:
        _, link, query = m.text.split(maxsplit=2)
        info = db.get_chat(link)
        if not info or not info.get('chat_id'): _reply(m, "❗️ Чат не найден или не отсканирован."); return
        _send_paginated_results(m, db.search_one(info['chat_id'], query), str(info['chat_id']), query)
    except ValueError: _reply(m, "Формат: `/search <ссылка> <запрос>`")

@bot.message_handler(func=lambda msg: not msg.text.startswith('/'))
def global_search_cmd(m):
    _send_paginated_results(m, db.search_all(m.text), 'global', m.text)

def _keep_alive():
    while True:
        time.sleep(600)
        if not config.WEBAPP_URL: continue
        try:
            requests.get(config.WEBAPP_URL, timeout=10)
        except requests.RequestException:
            pass

def create_app():
    app = Flask(__name__)
    app.route(f'/{config.BOT_TOKEN}', methods=['POST'])(webhook)
    app.route('/')(index)
    
    bot.remove_webhook()
    time.sleep(0.5)
    if config.WEBAPP_URL:
        bot.set_webhook(url=f"{config.WEBAPP_URL}/{config.BOT_TOKEN}")
        threading.Thread(target=_keep_alive, daemon=True).start()
    return app

def webhook():
    bot.process_new_updates([types.Update.de_json(request.get_data().decode('utf-8'))])
    return '', 200

def index(): return "Dominatus v3.4: Active"
