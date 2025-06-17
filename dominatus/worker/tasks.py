import os
import asyncio
import logging
from celery import Celery
from telethon import TelegramClient
from telethon.tl.types import User

from dominatus.core.config import config
from dominatus.core.database import DatabaseManager

celery_app = Celery('tasks', broker=config.REDIS_URL, backend=config.REDIS_URL)

class ScanWorker:
    def __init__(self):
        self.db = DatabaseManager(config.DATABASE_URL)
        session_path = 'data/parser_session'
        os.makedirs(os.path.dirname(session_path), exist_ok=True)
        self.client = TelegramClient(session_path, int(config.API_ID), config.API_HASH)

    async def run_scan(self, link):
        task = self.db.get_chat(link)
        if not task:
            logging.error(f"Task for {link} not found.")
            return
        try:
            await self.client.connect()
            if not await self.client.is_user_authorized():
                logging.critical("Parser account auth failed.")
                return

            self.db.update_chat(link, 'SCANNING')
            entity = await self.client.get_entity(link)
            self.db.update_chat(link, 'SCANNING', chat_title=getattr(entity, 'title', 'N/A'), chat_id=entity.id)
            
            batch, count, limit = [], 0, config.PARSER_MESSAGE_LIMIT
            async for msg in self.client.iter_messages(entity, limit=limit):
                if not msg or not msg.sender or not isinstance(msg.sender, User) or msg.sender.bot: continue
                u = msg.sender
                batch.append((u.id, u.first_name, u.last_name, u.username,
                              msg.date, f"https://t.me/c/{entity.id}/{msg.id}", msg.text, entity.id))
                count += 1
                if len(batch) >= 100:
                    self.db.save_messages(batch)
                    batch.clear()
            
            if batch: self.db.save_messages(batch)
            self.db.update_chat(link, 'COMPLETED', message_count=count)
            logging.info(f"Task {link} done. Found {count} messages.")
        except Exception as e:
            logging.error(f"Task {link} failed: {e}", exc_info=True)
            self.db.update_chat(link, 'FAILED')
        finally:
            if self.client.is_connected(): await self.client.disconnect()

@celery_app.task(name='tasks.scan_chat')
def scan_chat_task(link):
    asyncio.run(ScanWorker().run_scan(link))
