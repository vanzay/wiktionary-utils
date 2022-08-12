import json

import lxml.html

from config import DB_FILENAME, DICT_FILENAME
from db import DB, ArticleModel, process_articles


def handle_row(row):
    article_id = row[0]
    title = row[1]
    html = row[2]

    old_value = terms_dict.get(title, None)
    if not old_value:
        terms_dict[title] = '-'

    for term_form in extract_term_forms(html):
        if term_form == title:
            continue

        old_value = terms_dict.get(term_form, None)
        if not old_value or old_value == '-' or len(old_value) > len(title):
            terms_dict[term_form] = title

    print(f'Processed article {article_id} {title}')


def extract_term_forms(html) -> set:
    tree = lxml.html.fromstring(html)
    return set([elem.text_content() for elem in tree.xpath("//*[contains(@class, 'form-of ')]")])


def save_dict(filename: str):
    f = open(filename, 'a')
    f.write(json.dumps(terms_dict))
    f.close()


if __name__ == '__main__':
    terms_dict = dict()
    db = DB(DB_FILENAME)
    article_model = ArticleModel(db)
    db.connect()
    process_articles(db, 'spanish', handle_row)
    db.close()
    save_dict(DICT_FILENAME)
