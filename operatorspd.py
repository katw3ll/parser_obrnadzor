from xml.etree import ElementTree
from zipfile import ZipFile
import requests
from bs4 import BeautifulSoup
import sys
import datetime
from pymongo import MongoClient
from itertools import islice

RKN_URL = 'https://rkn.gov.ru'
DATASET_PAGE_URL = 'https://rkn.gov.ru/opendata/7705846236-OperatorsPD/'
FAKE_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0'
ZIP_DOWNLOAD_PATH = 'operatorspd.zip'
INSERT_CHUNK_SIZE = 10000

def find_dataset(url=DATASET_PAGE_URL, user_agent=FAKE_USER_AGENT):
    '''
    Fetches dataset URL from Roskomnadzor's website

    :param url:
    :param user_agent:
    :return:
    '''
    headers = {
        'User-Agent': user_agent,
    }
    page = requests.get(url, headers=headers)
    soup = BeautifulSoup(page.text, 'html.parser')

    # Find main table
    table = soup.find('table', attrs={'class': 'TblList'})

    # Find row that starts with '9'
    for row in table.find_all('tr'):
        cols = row.find_all('td')
        if cols[0].text == '9':
            return RKN_URL + cols[2].find('a')['href']


def download(url, save_path, chunk_size=4096, user_agent=FAKE_USER_AGENT):
    '''
    Downloads a file from a given URL into `save_path`
    Note that Roskomnadzor checks if a client has legit user agent, and rejects requests without one

    :param url: URL from which a file will be downloaded
    :param save_path: Path to the saved file
    :param chunk_size: Chunk size (in bytes).
    :param user_agent: User agent string that will be sent alongside request.
    '''

    headers = {
        'User-Agent': user_agent,
    }

    request = requests.get(url, stream=True, headers=headers)
    with open(save_path, 'wb') as file:
        for chunk in request.iter_content(chunk_size=chunk_size):
            file.write(chunk)


def unpack(zip_file_path):
    '''
    Unpacks zip file at `zip_file_path` and returns file name of extracted XML

    :param zip_file_path: Path to ZIP archive
    :return: Path to unzipped XML
    '''
    with open(zip_file_path, 'rb') as file:
        archive = ZipFile(file, 'r')
        archive.extractall()
        return archive.namelist()[0]


def parse_record(elem):
    '''
    Parses an operator record into dictionary

    :param elem:
    :return:
    '''
    operator = {}
    for e in elem:
        if 'date' in e.tag:
            try:
                operator[e.tag] = datetime.datetime.strptime(elem.text, '%Y-%m-%d')
            except ValueError:
                continue
        elif e.tag == 'is_list':
            parse_is = lambda elem: {m.tag:m.text for m in elem}
            operator[e.tag] = [parse_is(i) for i in e]
        else:
            operator[e.tag] = e.text

    return operator


def operators(file_name):
    '''
    Iterates over operators in a XML

    :param file_name: XML file to parse operator info from
    :return: Iterator over Operator objects
    '''

    for event, elem in ElementTree.iterparse(file_name, events=('end',)):
        # Strip Roskomnadzor's XML namespace
        _, _, elem.tag = elem.tag.rpartition('}')

        # Process a `record` element when ElementTree finishes parsing it
        if event == 'end' and elem.tag == 'record':
            yield parse_record(elem)
            elem.clear()


def chunked(iterable, size):
    it = iter(iterable)
    while True:
        chunk = tuple(islice(it, size))
        if not chunk:
            break
        yield chunk


def main(xml_path=None):
    if xml_path is None:
        dataset_url = find_dataset()
        download(dataset_url, ZIP_DOWNLOAD_PATH)
        xml_path = unpack(ZIP_DOWNLOAD_PATH)
    client = MongoClient()
    operatorspd = client.RKN.operatorspd

    # Wiping the collection and writing data again sounds dirty, but actually seems to be
    # faster than upserting the data, mostly because MongoDB doesn't have to compare
    # objects to each other.
    operatorspd.drop()

    # Write the data in chunks to lower memory requirements
    print('Chunk size is %d operators' % INSERT_CHUNK_SIZE)
    for i, chunk in enumerate(chunked(operators(xml_path), INSERT_CHUNK_SIZE)):
        print('Writing chunk #%d' % i)
        operatorspd.insert_many(chunk)


if __name__ == '__main__':
    if len(sys.argv) >= 2:
        main(sys.argv[1])
    else:
        main()
