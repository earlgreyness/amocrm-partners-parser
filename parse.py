#!/usr/bin/env python3
import re
from concurrent.futures import ThreadPoolExecutor

import attr
import requests
from bs4 import BeautifulSoup

AMO_HOST = 'https://www.amocrm.ru'

RUSSIAN_LETTERS = set('абвгдежзиклмнопрстуфхцчшщъыьэюя')
DIGITS = set('0123456789')


@attr.s(frozen=True)
class Contact:
    websites = attr.ib()
    emails = attr.ib()
    cities = attr.ib()
    phones = attr.ib()


def get_soup(url):
    html_doc = requests.get(url).text
    return BeautifulSoup(html_doc, 'html.parser')


def parse_list():
    soup = get_soup(AMO_HOST + '/partners/')
    container = soup.find('div', class_='partners-list__container')
    urls = []
    for link in container.children:
        try:
            urls.append(AMO_HOST + link.get('href'))
        except AttributeError:
            pass

    return urls


def parse_line(tag):
    try:
        return tag.span.a.span.string
    except AttributeError:
        pass

    try:
        return tag.span.span.string
    except AttributeError:
        pass

    try:
        return tag.a.string
    except AttributeError:
        pass

    return tag.string


def build_contact(line):
    websites = []
    emails = []
    cities = []
    phones = []

    for item in line:
        if item is None:
            continue

        item = item.replace('\r\n', '').replace('</a>', '').strip()
        letters = set(item.lower())
        if item.startswith('г.'):
            cities.append(item)
        elif re.match(r'.+@.+\..+', item):
            emails.append(item)
        elif len(letters & DIGITS) > 3:
            phones.append(item)
        elif len(letters & RUSSIAN_LETTERS) > 3:
            cities.append(item)
        else:
            websites.append(item)

    return Contact(
        websites=websites,
        emails=emails,
        cities=cities,
        phones=phones,
    )


def parse_single(url):
    soup = get_soup(url)
    container = soup.find('div', class_='partners-detail__contacts')

    details = container.find_all(['p', 'a', 'span'], recursive=False)

    lines = [parse_line(tag) for tag in details]
    return build_contact(lines)


def main():
    urls = parse_list()

    for url in urls:
        contact = parse_single(url)
        print(contact)
        print()


if __name__ == '__main__':
    main()
