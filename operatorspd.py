from xml.etree import ElementTree
from zipfile import ZipFile
import requests
from bs4 import BeautifulSoup


RKN_URL = 'https://rkn.gov.ru'
DATASET_PAGE_URL = 'https://rkn.gov.ru/opendata/7705846236-OperatorsPD/'
FAKE_USER_AGENT = 'Mozilla/5.0 (Windows NT 10.0; Win64; x64; rv:81.0) Gecko/20100101 Firefox/81.0'


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
    with open(zip_file_path, 'rb') as file:
        archive = ZipFile(file, 'r')
        archive.extractall()


def extract_text(parent_element, element_name):
    node = parent_element.find(element_name)
    if node is not None:
        return node.text


# Fields to be extracted for each InfoSystem
INFOSYSTEM_FIELDS = ['name',
                     'pd_category',
                     'category_sub_txt',
                     'actions_category',
                     'pd_handle',
                     'transgran_transfer',
                     'db_country']

# Fields to be extracted for each Operator
OPERATOR_FIELDS = ['pd_operator_num',
                   'enter_date',
                   'enter_order',
                   'status',
                   'name_full',
                   'inn',
                   'address',
                   'income_date',
                   'territory',
                   'purpose_txt',
                   'basis',
                   'safeguards_txt',
                   'resp_name',
                   'startdate',
                   'stop_condition',
                   'stop_date',
                   'enter_order_num',
                   'enter_order_date',
                   'end_order_date',
                   'end_order_num']


class InfoSystem:
    def __init__(self, elem):
        for field in INFOSYSTEM_FIELDS:
            vars(self)[field] = extract_text(elem, field)


class Operator:
    def __init__(self, elem):
        for field in OPERATOR_FIELDS:
            vars(self)[field] = extract_text(elem, field)

        self.infosystems = []
        infosystem_list = elem.find('is_list')
        for is_elem in infosystem_list.iter('is'):
            self.infosystems.append(InfoSystem(is_elem))


def operators(file_name):
    '''
    Iterates over operators in a given XML

    :param file_name: File to parse operator info from
    :return: Iterator over Operator objects
    '''

    for event, elem in ElementTree.iterparse(file_name, events=('end',)):
        # Strip Roskomnadzor's XML namespace
        _, _, elem.tag = elem.tag.rpartition('}')

        # Process a `record` element when ElementTree finishes parsing it
        if event == 'end' and elem.tag == 'record':
            yield Operator(elem)
            elem.clear()


if __name__ == '__main__':
    # dataset_url = find_dataset()
    # download(dataset_url, 'operatorspd.zip')
    # unpack('operatorspd.zip')

    for operator in operators('data-20201016T0000-structure-20180129T0000.xml'):
        print(f'inn={operator.inn} name_full={operator.name_full}')
