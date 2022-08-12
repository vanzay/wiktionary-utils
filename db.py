import sqlite3

BATCH_SIZE = 100
PAGE_SIZE = 100


class DB:
    def __init__(self, connection: str):
        self.connection = connection
        self._conn = None

    def connect(self):
        self._conn = sqlite3.connect(self.connection)

    def execute(self, sql: str, params=()):
        cursor = self._conn.cursor()
        cursor.execute(sql, params)

    def executemany(self, sql: str, params: list):
        cursor = self._conn.cursor()
        cursor.executemany(sql, params)

    def fetch(self, sql: str, params=()):
        cursor = self._conn.cursor()
        cursor.execute(sql, params)
        return cursor.fetchall()

    def commit(self):
        self._conn.commit()

    def close(self):
        self._conn.close()


class ArticleModel:

    def __init__(self, db: DB):
        self.db = db

    def count_articles(self, lang: str):
        return self.db.fetch(f"SELECT count(*) FROM articles WHERE lang = '{lang}'")[0][0]

    def find_articles(self, lang: str, offset: int, page_size=PAGE_SIZE):
        return self.db.fetch(f'''
            SELECT id, title, body
            FROM articles
            WHERE lang = '{lang}'
            ORDER BY id
            LIMIT {page_size}
            OFFSET {offset}
            ''')

    def update_body(self, article_id: int, body: str):
        self.db.execute('UPDATE articles SET body=? WHERE id=?', (body, article_id))

    def delete(self, article_id: int):
        self.db.execute('DELETE FROM articles WHERE id=?', (article_id,))


class BatchInsert:

    def __init__(self, db: DB, table: str, columns: list, batch_size=BATCH_SIZE):
        self.db = db
        self.table = table
        self.columns = columns
        self.data = []
        self.batch_size = batch_size

    def insert(self, row: tuple):
        self.data.append(row)
        if len(self.data) >= self.batch_size:
            self.flush()

    def flush(self):
        if len(self.data) > 0:
            self.db.executemany(f'''
                INSERT INTO {self.table}({','.join(self.columns)})
                VALUES ({('?,' * len(self.columns))[0:-1]})''', self.data)
            self.data = []
            self.db.commit()


def process_articles(db: DB, lang: str, row_handler):
    article_model = ArticleModel(db)
    articles_count = article_model.count_articles(lang)
    offset = 0
    while offset < articles_count:
        rows = article_model.find_articles(lang, offset)
        for row in rows:
            row_handler(row)
        db.commit()
        offset += PAGE_SIZE
