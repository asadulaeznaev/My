import psycopg2
from psycopg2.extras import DictCursor, execute_values
from contextlib import contextmanager

class DatabaseManager:
    def __init__(self, db_url):
        self.db_url = db_url
        self._init_db()

    @contextmanager
    def _get_conn(self):
        conn = psycopg2.connect(self.db_url)
        try:
            yield conn
        finally:
            conn.close()

    def _execute(self, query, params=None, fetch=None):
        with self._get_conn() as conn:
            with conn.cursor(cursor_factory=DictCursor) as cursor:
                cursor.execute(query, params or ())
                if fetch == 'one': return dict(cursor.fetchone()) if cursor.rowcount > 0 else None
                if fetch == 'all': return [dict(row) for row in cursor.fetchall()]
            conn.commit()
    
    def _init_db(self):
        self._execute('''
            CREATE TABLE IF NOT EXISTS scanned_chats(id SERIAL PRIMARY KEY,chat_link TEXT NOT NULL UNIQUE,chat_id BIGINT,chat_title TEXT,submitter_id BIGINT NOT NULL,status TEXT NOT NULL,message_count INTEGER DEFAULT 0,last_scanned TIMESTAMPTZ);
            CREATE TABLE IF NOT EXISTS messages(id SERIAL PRIMARY KEY,user_id BIGINT NOT NULL,first_name TEXT,last_name TEXT,username TEXT,message_date TIMESTAMPTZ NOT NULL,message_link TEXT NOT NULL UNIQUE,message_content TEXT,chat_id BIGINT NOT NULL,tsv tsvector);
            CREATE INDEX IF NOT EXISTS idx_messages_user_id ON messages(user_id);
            CREATE INDEX IF NOT EXISTS idx_messages_chat_id ON messages(chat_id);
            CREATE INDEX IF NOT EXISTS messages_tsv_idx ON messages USING GIN(tsv);
            DO $$ BEGIN
                CREATE FUNCTION messages_tsv_trigger() RETURNS trigger AS $f$ BEGIN new.tsv := to_tsvector('russian',coalesce(new.message_content,''));RETURN new;END; $f$ LANGUAGE plpgsql;
                DROP TRIGGER IF EXISTS tsvectorupdate ON messages;
                CREATE TRIGGER tsvectorupdate BEFORE INSERT OR UPDATE ON messages FOR EACH ROW EXECUTE PROCEDURE messages_tsv_trigger();
            EXCEPTION WHEN duplicate_function THEN NULL;END;$$;''')

    def add_chat(self, link, user_id):
        self._execute("INSERT INTO scanned_chats(chat_link,submitter_id,status)VALUES(%s,%s,'PENDING') ON CONFLICT(chat_link)DO NOTHING",(link,user_id))

    def update_chat(self, link, status, **kwargs):
        updates = ", ".join([f"{k}=%s" for k in kwargs])
        params = [status] + list(kwargs.values()) + [link]
        self._execute(f"UPDATE scanned_chats SET status=%s,last_scanned=NOW(){', '+updates if updates else ''} WHERE chat_link=%s", tuple(params))
        
    def get_chat(self, link):
        return self._execute("SELECT*FROM scanned_chats WHERE chat_link=%s",(link,),fetch='one')

    def save_messages(self, messages):
        sql="INSERT INTO messages(user_id,first_name,last_name,username,message_date,message_link,message_content,chat_id)VALUES %s ON CONFLICT(message_link)DO NOTHING"
        with self._get_conn() as conn:
            with conn.cursor() as cursor:
                execute_values(cursor,sql,messages)
            conn.commit()
    
    def _run_search(self, query, params):
        return self._execute(query+" ORDER BY m.message_date DESC LIMIT 200",params,fetch='all')

    def search_all(self, term):
        q="SELECT m.*,sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id=sc.chat_id"
        if term.isdigit():return self._run_search(f"{q} WHERE m.user_id=%s",(int(term),))
        if term.startswith('@'):return self._run_search(f"{q} WHERE m.username=%s",(term.lstrip('@'),))
        return self._run_search(f"{q} WHERE m.tsv@@to_tsquery('russian',%s)",(' & '.join(term.split()),))

    def search_one(self, chat_id, term):
        q="SELECT m.*,sc.chat_title FROM messages m JOIN scanned_chats sc ON m.chat_id=sc.chat_id WHERE m.chat_id=%s"
        if term.isdigit():return self._run_search(f"{q} AND m.user_id=%s",(chat_id,int(term)))
        if term.startswith('@'):return self._run_search(f"{q} AND m.username=%s",(chat_id,term.lstrip('@')))
        return self._run_search(f"{q} AND m.tsv@@to_tsquery('russian',%s)",(chat_id,' & '.join(term.split())))

    def get_stats(self):
        msg=self._execute("SELECT COUNT(*),COUNT(DISTINCT user_id)FROM messages",fetch='one')
        chat=self._execute("SELECT COUNT(*),COUNT(CASE WHEN status='COMPLETED'THEN 1 END)FROM scanned_chats",fetch='one')
        return msg,chat
