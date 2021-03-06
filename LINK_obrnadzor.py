import xml.etree.cElementTree as Etree 
from fake_useragent import UserAgent
from pymongo import MongoClient
from bs4 import BeautifulSoup
import requests
import zipfile
import pymongo
import bson
import os

URL = r'http://isga.obrnadzor.gov.ru/accredreestr/'


def GetSoup():
    resp = requests.get(URL)
    soup = BeautifulSoup(resp.text, 'html.parser')
    return soup


def InitDbConnectionObrnadzor(option, option_delete):
    '''
    option == True => подключение к локальной бд \n
    option == False => подключение к бд на VPS

    option_delete == True => удаление коллекции \n
    option_delete == False => коллекция не удаляется

    Подключаемся к коллекции obrnadzor, из которой будет происходить выборка по критериям
    '''
    if option: # Подключение к локальной, пересоздание и обновление коллекции obrnadzor_local
        client = MongoClient('localhost', 27017)
        db = client.RKNN
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
        db = client.RKNN
        if 'obrnadzor' in db.collection_names() and option_delete:
            db.drop_collection('obrnadzor')
        collection = db.obrnadzor
        return collection

    
def InitDbConnectionEdu(option, option_delete):
    '''
    option == True => подключение к локальной бд \n
    option == False => подключение к бд на VPS

    option_delete == True => удаление коллекции \n
    option_delete == False => коллекция не удаляется

    Подключаемся к коллекции edu, в которую будет производиться запись данных из obrnadzor
    '''
    if option: # Подключение к локальной, пересоздание и обновление коллекции edu_records
        client = MongoClient('localhost', 27017)
        db = client.RKNN
        if 'edu_records' in db.collection_names() and option_delete:
            db.drop_collection('edu_records')
        collection = db.edu_records
        return collection
    else: # Подключение к бд на сервере, пересоздание и обновление коллекции edu
        client = MongoClient('23.105.226.109',
                    username='root',
                    password='MW6Vh6dlw4FaNv0aSi4Rs15Y',
                    authSource='admin',
                    authMechanism='SCRAM-SHA-1')
        db = client.RKNN
        if 'edu' in db.collection_names() and option_delete:
            db.drop_collection('edu')
        collection = db.edu
        return collection


def GetRegionsDict():
    '''
    Получаем словарь вида: 'номер региона': 'название региона'
    '''
    soup = GetSoup()
    select = soup.find('select', attrs = {'name': 'regionId', 'class': 'form-control'})
    options_list = select.find_all('option')
    dictionary = {'': 'Не выбрано'}
    for option in options_list:
        if option.text != 'Не выбрано':
            data = option.text.split(' - ')
            if len(data) > 2:
                dictionary[data[0]] = data[1] + ' - ' + data[2]
            else:
                dictionary[data[0]] = data[1]
    return dictionary


def InsertIntoDb(option_obrnadzor, option_edu, option_delete_obrnadzor, option_delete_edu, inn = None, region = None):
    '''
    option_obrnadzor == True => подключение к локальной бд (obrnadzor_local) \n
    option_obrnadzor == False => подключение к бд на VPS (obrnadzor)

    option_edu == True => подключение к локальной бд (edu_local) \n
    option_edu == False => подключение к бд на VPS (edu) \n

    option_delete_obrnadzor == True => удаление коллекции \n
    option_delete_obrnadzor == False => коллекция не удаляется

    option_delete_edu == True => удаление коллекции \n
    option_delete_edu == False => коллекция не удаляется

    Выборка элементов из коллекции obrnadzor и вставка их в edu.
    Выборка происходит по 2-м критериям:
        1) ИНН
        2) Номер региона
    Можно ввести как один критерий, так и сразу два. \n
    Если не вводить никаких критериев отбора, то будут выбраны и вставлены все элементы из коллекции.
    '''
    collection = InitDbConnectionObrnadzor(option_obrnadzor, option_delete_obrnadzor)
    collection_edu = InitDbConnectionEdu(option_edu, option_delete_edu) 
    dictionary = GetRegionsDict()
    query = {}
    if region != None and inn == None:
        query = {'Сведения об образовательной организации или организации, осуществляющей обучение.Субъект РФ': dictionary[str(region)]}
    elif region == None and inn != None:
        query = {'Сведения об образовательной организации или организации, осуществляющей обучение.ИНН': str(inn)}
    elif region != None and inn != None:
        query = {'Сведения об образовательной организации или организации, осуществляющей обучение.ИНН': str(inn),
                 'Сведения об образовательной организации или организации, осуществляющей обучение.Субъект РФ': dictionary[str(region)]}
    for result in collection.find(query):
        collection_edu.insert_one(result)


if __name__ == '__main__':
    InsertIntoDb(True, False, False, True, None, None)