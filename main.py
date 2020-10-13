import requests
import json
from bs4 import BeautifulSoup
from fake_useragent import UserAgent

url_page = "http://isga.obrnadzor.gov.ru/accredreestr/search/?page="
url_details = "http://isga.obrnadzor.gov.ru/accredreestr/details/"

DATA = []


def get_count_of_pages():
    resp = requests.post(url_page + '100000000', \
                         data={
                             'searchby': 'organization',
                             'regionId': '76',
                             'eduOrgTypeId': '40', 
                             'certStatusId': '1'
                             }, 
                         headers={
                             'User-Agent': UserAgent().chrome})  # Обращаемся к очень большой странице, скорее всего которой не будет
    soup = BeautifulSoup(resp.text, 'lxml')  # В ответ получаем самую последнюю страницу, скорее всего 9124-тую
    page = soup.findAll('li')[-2].find('a').text  # И берем вытаскиваем тест с предпоследней кнопки
    return int(page)


def get_INN(data_id):
    resp = requests.get(url_details + data_id + "/")
    soup = BeautifulSoup(resp.text, 'lxml')
    inn = soup.findAll('tr')[5].findAll('td')[1]  # Ищем 6 строчку и берем из нее ИНН
    return inn.text


def get_data_from_page(page):
    resp = requests.post(url_page + page, \
                         data={'searchby': 'organization', 'regionId': '76', 'eduOrgTypeId': '40', 'certStatusId': '1'}, \
                         headers={'User-Agent': UserAgent().chrome})
    soup = BeautifulSoup(resp.text, 'lxml')
    tbody = soup.findAll('tr')[1:]
    for tr in tbody:
        _id = tr['data-id']
        name = tr.findAll('td')[1].text.replace("\n", "")
        inn = get_INN(_id)
        DATA.append({"name": name, "inn": inn})
        # print(_id, name, inn)


def parse():
    print("Getting count of page...")
    count = get_count_of_pages()
    # count = 300
    print("Count of pages:", count)
    for i in range(1, count + 1):
        print('Parsing page ' + str(i))
        get_data_from_page(str(i))


if __name__ == '__main__':
    parse()
    with open('data.json', 'w', encoding='utf-8') as f:
        json.dump(DATA, f, ensure_ascii=False)
