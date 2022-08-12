import requests

from config import DB_FILENAME, WIKTIONARY_DOMAIN
from db import DB, ArticleModel, process_articles


def download_html(term: str, lang: str) -> str:
    response = requests.get(f'https://{WIKTIONARY_DOMAIN}/wiki/{term}?action=render',
                            headers={'User-Agent': 'Wiktionary Browser'})
    content = response.content.decode('utf-8')
    return next(part for part in content.split('<hr>') if f'id="{lang}"' in part)


def handle_row(row):
    article_id = row[0]
    title = row[1]
    body = row[2]
    if '<h2>' in body:
        return

    try:
        # TODO language
        html = download_html(title, 'Spanish')
        article_model.update_body(article_id, html)
        print(f'Processed article {article_id} {title}')
    except StopIteration:
        article_model.delete(article_id)
        print(f'Article {article_id} {title} is absent')


if __name__ == '__main__':
    db = DB(DB_FILENAME)
    article_model = ArticleModel(db)
    db.connect()
    process_articles(db, 'spanish', handle_row)
    db.close()
