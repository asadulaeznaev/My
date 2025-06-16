import threading
import logging
from flask import Flask, request
from telebot import types
from config import Config
from database import DatabaseManager
from parser_logic import Parser
from bot_logic import BotHandler

class App:
    def __init__(self, config: Config):
        self.config = config
        logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
        self.logger = logging.getLogger(__name__)

        self.db = DatabaseManager(self.config.DB_PATH)
        self.bot_instance = config.bot
        self.bot_handler = BotHandler(self.config, self.db)
        self.parser = Parser(self.config, self.db)
        self.flask_app = Flask(__name__)
        self.setup_routes()

    def setup_routes(self):
        webhook_path = f'/{self.config.BOT_TOKEN}'
        self.flask_app.add_url_rule(webhook_path, 'process_webhook', self.process_webhook, methods=['POST'])
        self.flask_app.add_url_rule('/', 'index', lambda: "Экзекутор Воли: Протокол Цитадель активен.")

    def process_webhook(self):
        json_string = request.get_data().decode('utf-8')
        update = types.Update.de_json(json_string)
        self.bot_instance.process_new_updates([update])
        return '', 200

    def run(self):
        self.logger.info("Инициализация Базы Данных и индекса...")
        self.db.init_db()

        self.logger.info("Регистрация обработчиков бота...")
        self.bot_handler.register_handlers()
        
        self.logger.info("Запуск парсера в фоновом потоке...")
        self.parser.start()
        
        self.logger.info("Активация 'Внутреннего Стража'...")
        guardian_thread = threading.Thread(target=self.config.guardian_thread_func, daemon=True)
        guardian_thread.start()
        
        self.logger.info("Веб-сервер готов к запуску через Gunicorn.")

config_instance = Config()
app = App(config_instance)
server = app.flask_app
