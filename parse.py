#!/usr/bin/env python3
import csv
import pickle
import re
from collections import OrderedDict
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime

import attr
import requests
from bs4 import BeautifulSoup
from phonenumbers import (
    NumberParseException, PhoneNumberFormat, parse as parse_phonenumber,
    format_number, is_possible_number, is_valid_number,
)

AMO_HOST = 'https://www.amocrm.ru'
DIGITS = set('0123456789+()')


@attr.s(frozen=True)
class Contact:
    url = attr.ib()
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


def build_contact(line, url):
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
        elif re.match(r'^.+@.+\..+$', item):
            emails.append(item)
        elif len(letters & DIGITS) > 3:
            phones.append(item)
        elif re.match(r'^[^ ]+\.[^ ]+$', item):
            websites.append(item)
        else:
            cities.append(item)

    return Contact(
        url=url,
        websites=websites,
        emails=emails,
        cities=cities,
        phones=phones,
    )


def parse_single(url):
    soup = get_soup(url)
    container = soup.find('div', class_='partners-detail__contacts')

    if container is None:
        lines = []
    else:
        details = container.find_all(['p', 'a', 'span'], recursive=False)
        lines = [parse_line(tag) for tag in details]

    return build_contact(lines, url)


def normalize_phone(phone):
    try:
        parsed = parse_phonenumber(phone, 'RU')
    except NumberParseException:
        return phone
    if not is_possible_number(parsed) or not is_valid_number(parsed):
        return phone
    return format_number(parsed, PhoneNumberFormat.INTERNATIONAL)


def save_to_csv(filename):
    with open(filename, 'rb') as file:
        contacts = [x for x in pickle.load(file).values() if x is not None]

    cols_website = max(len(x.websites) for x in contacts)
    cols_email = max(len(x.emails) for x in contacts)
    cols_city = max(len(x.cities) for x in contacts)
    cols_phone = max(len(x.phones) for x in contacts)

    def extend(items, n):
        return items + [''] * (n - len(items))

    with open('amocrm-partner-contacts.csv', 'w') as file:
        writer = csv.writer(file)

        for contact in contacts:
            websites = extend(contact.websites, cols_website)
            emails = extend(contact.emails, cols_email)
            cities = extend(contact.cities, cols_city)
            phones = extend([normalize_phone(x) for x in contact.phones], cols_phone)

            writer.writerow([contact.url] + websites + emails + cities + phones)


def main():
    filename = 'dump-{}.pickle'.format(int(datetime.now().timestamp()))
    urls = parse_list()
    contacts = OrderedDict((x, None) for x in urls)

    with ThreadPoolExecutor() as executor:
        for i, contact in enumerate(executor.map(parse_single, urls), start=1):
            contacts[contact.url] = contact
            print('Done {}/{}: {}'.format(i, len(urls), contact))
            if i % 100 == 0:
                with open(filename, 'wb') as file:
                    pickle.dump(contacts, file)


if __name__ == '__main__':
    main()
