import xml.etree.ElementTree as Etree 
from fake_useragent import UserAgent
from pymongo import MongoClient
import requests
import zipfile
import pymongo
import os

class DB:
    def __init__(self, URL = '', PATH_ZIP = '', PATH_FOLDER_XML = '', PATH_XML = ''):
        self.URL = URL
        self.PATH_ZIP = PATH_ZIP
        self.PATH_XML = PATH_XML
        self.PATH_FOLDER_XML = PATH_FOLDER_XML    

    def DownloadData(self, URL, PATH_ZIP):
        '''
        Скачивание zip файла
        '''
        try:
            z = open(PATH_ZIP, "wb")
            r = requests.get(URL, stream = True, headers = {'User-Agent': UserAgent().chrome})
            for chunk in r.iter_content(chunk_size = 8388608):
                    z.write(chunk)
        except FileExistsError as err:
            print(str(err))
        else:
            z.close()
        # print("Файл закрыт! DownloadData")

    def GetXML(self, PATH_ZIP, PATH_FOLDER_XML):
        '''
        Распаковка zip и получение xml
        '''
        try:
            z = zipfile.ZipFile(PATH_ZIP, 'r')
            z.extractall(PATH_FOLDER_XML)
            self.PATH_XML = PATH_FOLDER_XML + "\\" + str(z.namelist()[0])
        except FileExistsError as err:
            print(str(err))
        except FileNotFoundError as err:
            print(str(err))
        else:
            z.close()
            # print("Файл закрыт! GetXML")

    def InitDbConnection(self, option):
        if option: # Подключение к локальной, пересоздание и обновление коллекции records_local
            client = MongoClient('localhost', 27017)
            db = client.RKN
            if 'records_local' in db.collection_names():
                db.drop_collection('records_local')
            collection = db.records_local
        else: # Подключение к бд на сервере, пересоздание и обновление коллекции records
            client = MongoClient('23.105.226.109',
                        username='root',
                        password='MW6Vh6dlw4FaNv0aSi4Rs15Y',
                        authSource='admin',
                        authMechanism='SCRAM-SHA-1')
            db = client.RKN
            if 'records' in db.collection_names():
                db.drop_collection('records')
            collection = db.records
        return collection

    def InsertIntoLocal(self, PATH_XML, option):
        '''
        Запись в локальную бд
        option == True => запись в локальную бд
        option == False => запись в бд на сервере
        '''
        collection = self.InitDbConnection(option)

        context = Etree.iterparse(PATH_XML, events=('start','end'))
        context = iter(context)
        event, _ = context.__next__()

        document = {}
        for event, elem in context:
            if str(elem.tag).split("}")[1] != 'record':
                if str(elem.tag).split("}")[1] == 'is_list':
                    is_list = {}
                    additional_tag = 0
                    event, elem = context.__next__() # Get initial 'is'
                    while str(elem.tag).split("}")[1] != 'is_list':
                        is_name = str(elem.tag).split("}")[1] + str(additional_tag)
                        is_list[is_name] = {}
                        event, elem = context.__next__() # Get inner elements in any 'is'
                        while str(elem.tag).split("}")[1] != 'is':
                            is_list[is_name][str(elem.tag).split("}")[1]] = elem.text
                            event, elem = context.__next__()
                        event, elem = context.__next__() # Get next 'is'
                        additional_tag += 1
                    document['is_list'] = is_list
                    continue
                document[str(elem.tag).split('}')[1]] = elem.text
            if str(elem.tag).split("}")[1] == 'record' and event == 'end':
                collection.insert_one(document)
                elem.clear()
                document = {}

if __name__ == "__main__":
    '''
    URL - ссылка для скачивания zip
    PATH_ZIP - Куда скачать zip с сайта
    PATH_FOLDER_XML - Куда распаковать zip
    PATH_XML - Полный путь до xml (для InsertIntoLocal)
    '''

    URL = 'https://rkn.gov.ru/opendata/7705846236-OperatorsPD/data-20201017T0000-structure-20180129T0000.zip' 
    PATH_ZIP = r'C:\Users\areak\Desktop\parser_API\data.zip'
    PATH_XML = r'C:\Users\areak\Desktop\parser_API\data-20201017T0000-structure-20180129T0000.xml'
    PATH_FOLDER_XML = r'C:\Users\areak\Desktop\parser_API'

    db = DB(URL, PATH_ZIP, PATH_FOLDER_XML)
    # db.DownloadData(db.URL, db.PATH_ZIP)
    # db.GetXML(db.PATH_ZIP, db.PATH_FOLDER_XML)
    db.InsertIntoLocal(PATH_XML, True)