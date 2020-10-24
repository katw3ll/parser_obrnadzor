import xml.etree.cElementTree as Etree 
from fake_useragent import UserAgent
from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
import zipfile
import pymongo
import bson
import os

URL_GENERIC = r'http://isga.obrnadzor.gov.ru/accredreestr/search/?page='
URL_SPECIFIC = r'http://isga.obrnadzor.gov.ru/accredreestr/details/'

data = {
    'regionId': '',
    'searchby': 'organization', 
    'eduOrgName': '', 
    'eduOrgInn': '', 
    'eduOrgOgrn': '',  
    'eduOrgAddress': '', 
    'eduOrgTypeId': '', 
    'eduOrgKindId': '',
    'indEmplLastName': '',
    'indEmplFirstName': '',
    'indEmplMiddleName': '',
    'indEmplAddress': '',
    'indEmplEgrip': '',
    'indEmplInn': '',
    'certRegNum': '', 
    'certSerialNum': '', 
    'certFormNum': '', 
    'certIssueFrom': '', 
    'certIssueTo': '', 
    'certEndFrom': '', 
    'certEndTo': '',  
    'certStatusId': '',
    'certificatesupplementstatusId': '',
    'eduProgCode': '',
    'eduProgName': '',
    'extended': ''
}

headers = {'User-Agent': UserAgent().chrome}


def TranserDataToVps():
    '''
        Перемещение локальной коллекции на сервер VPS
    '''
    collection_local = InitDbConnection(True, False)
    collection_vps = InitDbConnection(False, True)

    data = collection_local.find({})
    for document in data:
        collection_vps.insert_one(document)


def GetCountOfPages():
    '''
    Узнаем количество страниц, на которых расположена таблица
    с неполной информацией об объектах.
    '''
    resp = requests.post(URL_GENERIC + '1', data=data, headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')
    return int(soup.find('h3').text.split('(')[1][:5])


def GetGenericSoup(page_number):
    '''
    Получаем страницу, на которой находится таблица с информацией о множестве
    объектов, но информация о них является неполной.
    '''
    resp = requests.post(URL_GENERIC + str(page_number), data=data, headers=headers)
    soup = BeautifulSoup(resp.text, 'html.parser')
    return soup


def GetSpecificSoup(key):
    '''
    Получаем страницу, на которой находится таблица, в которой хранится
    полная информация об одном объекте.
    '''
    resp = requests.get(URL_SPECIFIC + str(key) + '/1/')
    soup = BeautifulSoup(resp.text, 'html.parser')
    return soup


def InitDbConnection(option, option_delete):
    if option: # Подключение к локальной, пересоздание и обновление коллекции obrnadzor_local
        client = MongoClient('localhost', 27017)
        db = client.RKN
        if 'obrnadzor_local' in db.collection_names() and option_delete:
            db.drop_collection('obrnadzor_local')
        collection = db.obrnadzor_local
        return collection
    else: # Подключение к бд на сервере, пересоздание и обновление коллекции obrnadzor
        client = MongoClient('23.105.226.109',
                    username='root',
                    password='MW6Vh6dlw4FaNv0aSi4Rs15Y',
                    authSource='admin',
                    authMechanism='SCRAM-SHA-1')
        db = client.RKN
        if 'obrnadzor' in db.collection_names() and option_delete:
            db.drop_collection('obrnadzor')
        collection = db.obrnadzor
        return collection


def FillParts(td_list, document_part):
    '''
    Документ, вставляемый в mongo состоит из 2-х частей, т.к таблица
    с полной информацией об одном объекте разделена на две части.
    Эта функция заполняет ту часть, которая передана в эту функцию.
    '''
    if len(td_list) > 1:
        document_part[td_list[0].text] = td_list[1].text
        return document_part


def InsertIntoDb(option, option_delete):
    collection = InitDbConnection(option, option_delete)
    document = {}
    for i in range(GetCountOfPages() + 1):
        soup_generic = GetGenericSoup(i)
        for tr_generic in soup_generic.find('tbody').find_all('tr'):
            data_id = tr_generic.attrs.get('data-id')
            soup_specific = GetSpecificSoup(data_id)
            count_of_specific_tr = len(soup_specific.find('tbody').find_all('tr'))
            document = {}
            document_part_1 = {}
            document_part_2 = {}
            for tr_specific, j in zip(soup_specific.find('tbody').find_all('tr'), range(count_of_specific_tr)):
                td_list = tr_specific.find_all('td')
                if j < 6:
                    document_part_1 = FillParts(td_list, document_part_1)
                elif j > 6:
                    document_part_2 = FillParts(td_list, document_part_2)
            document['Сведения об образовательной организации или организации, осуществляющей обучение'] = document_part_1
            document_part_2['Дата выдачи свидетельства'] = document_part_2['Дата выдачи свидетельства'].split()[0]
            document_part_2['Срок действия свидетельства'] = document_part_2['Срок действия свидетельства'].split()[0]
            document_part_2['Дата публикации сведений в сводном реестре'] = document_part_2['Дата публикации сведений в сводном реестре'].split()[0]
            document['Общие сведения о государственной аккредитации'] = document_part_2
            collection.insert_one(document)

if __name__ == '__main__':
    # InsertIntoDb(True, False)
    # TranserDataToVps()