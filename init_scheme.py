from config import DB_FILENAME
from db import DB

db = DB(DB_FILENAME)
db.connect()

db.execute(f'''
    CREATE TABLE articles (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        lang TEXT NOT NULL,
        wiki_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL);''')

db.execute('''
    CREATE TABLE templates (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wiki_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL);''')

db.execute('''
    CREATE TABLE modules (
        id INTEGER PRIMARY KEY AUTOINCREMENT,
        wiki_id INTEGER NOT NULL,
        title TEXT NOT NULL,
        body TEXT NOT NULL);''')

db.commit()
db.close()
