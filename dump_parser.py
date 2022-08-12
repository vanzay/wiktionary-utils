import bz2
import re

import mwxml

from config import DUMP_PATH, DB_FILENAME, LANGS
from db import BatchInsert, DB

NAMESPACE_MAIN_PAGE = 0
NAMESPACE_TEMPLATE_PAGE = 10
NAMESPACE_MODULE_PAGE = 828
NAMESPACES = [NAMESPACE_MAIN_PAGE, NAMESPACE_TEMPLATE_PAGE, NAMESPACE_MODULE_PAGE]

BAD_TERM_REGEX = re.compile('.*[\\d:/&].*')
HEADLINE_REGEX = re.compile('^(=+)\\s*(.+?)\\s*(=+)$')


def is_valid_term(term: str) -> bool:
    return not (BAD_TERM_REGEX.search(term) or term.endswith('-') or term.startswith('-'))


def split_by_language(text: str) -> dict:
    result = {}
    lang = ''
    section_lines = []
    for line in text.split('\n'):
        line = line.strip()
        headline_search = HEADLINE_REGEX.search(line)
        if headline_search:
            level = len(headline_search.group(1))
            headline_name = headline_search.group(2)
            if level == 2:
                if len(section_lines) > 0:
                    result[lang] = '\n'.join(section_lines)
                    section_lines = []
                lang = headline_name.lower()
            else:
                section_lines.append(line)
        else:
            section_lines.append(line)

    if section_lines:
        result[lang] = '\n'.join(section_lines)

    return result


def process_page(page: mwxml.Page) -> tuple | None:
    if page.namespace not in NAMESPACES:
        return None

    # there is only one revision per page in *pages-articles dumps
    for revision in page:
        return page.id, page.namespace, page.title, revision.model, revision.text


def process_xml_dump(filename: str) -> None:
    dump = mwxml.Dump.from_file(bz2.open(filename))
    for page in dump:
        fields = process_page(page)
        if not fields:
            continue

        page_id, namespace, title, model, text = fields

        if not text:
            continue

        if namespace == NAMESPACE_MAIN_PAGE:
            if is_valid_term(title):
                lang_dict = split_by_language(text)
                for lang, markup in lang_dict.items():
                    if lang in LANGS:
                        articles.insert((lang, page_id, title, markup))
        elif namespace == NAMESPACE_TEMPLATE_PAGE:
            templates.insert((page_id, title, text))
        elif namespace == NAMESPACE_MODULE_PAGE:
            modules.insert((page_id, title, text))

        print(f'Processed page {page_id} {title}')


if __name__ == '__main__':
    db = DB(DB_FILENAME)
    db.connect()

    articles = BatchInsert(db, 'articles', ['lang', 'wiki_id', 'title', 'body'])
    templates = BatchInsert(db, 'templates', ['wiki_id', 'title', 'body'])
    modules = BatchInsert(db, 'modules', ['wiki_id', 'title', 'body'])

    process_xml_dump(DUMP_PATH)

    articles.flush()
    templates.flush()
    modules.flush()

    db.close()
