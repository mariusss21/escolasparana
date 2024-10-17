import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import logging
import warnings
import os

# Disable warnings globally
warnings.filterwarnings("ignore")

# Configure logging
logging.basicConfig(level=logging.INFO, format="%(levelname)s - %(message)s")
logger = logging.getLogger('')
logger.setLevel(logging.INFO)

def normalize_string(s):
    """
    Normalize the strings (city and school names)
    - Convert to lowercase
    - Replace specific characters
    - Remove special characters using regex
    
    Args:
        s (str): string to be normalized

    Returns:
        str: normalized string
    """
    s = s.lower()
    s = s.replace('á', 'a').replace('ã', 'a').replace(' ', '')
    s = re.sub(r'[^a-zA-Z0-9\s]', '', s)
    return s


def extract_cities_schools(filename):
    """
    Read excel file and format the data
    Create a list of unique cities

    Args:
        filename (str): excel file with the schools data

    Returns:
        pd.DataFrame, list: excel dataframe and list of cities
    """
    df_excel = pd.read_excel(filename)
    df_excel['escola_normalizada'] = df_excel['Estabelecimento_scrapping'].apply(normalize_string)
    df_excel['municipio_normalizada'] = df_excel['mun2'].apply(normalize_string)

    list_cities = df_excel['municipio_normalizada'].unique()

    return df_excel, list_cities


def initial_request(session, url):
    """
    Make the initial request to the website

    Args:
        session (requests.Session): requests session
        url (str): url to be requested

    Returns:
        str, str, dict: windowid, viewstate and dictionary of cities and their codes
    """

    r = session.get(url)
    logger.info(f'Initial request status: {r.status_code}')

    if r.status_code == 200:

        # extracting windowid
        if 'windowId' in r.text:
            windowid = r.text.split('windowId=')[1].split('"')[0]
            logger.info(f'windowid: {windowid}')
        else:
            raise Exception('WindowId not found')

        # extracting all cities and their codes
        soup = BeautifulSoup(r.text, 'html.parser')
        select_tag = soup.find('select', {'id': 'initial:j_idt97:municipio_input'})
        dict_city_codes = {}

        for option in select_tag.find_all('option'):
            if 'value' in str(option):
                dict_city_codes[normalize_string(option.text)] = {'city_code':option['value']}

        # extracting viewstate
        viewstate = soup.find('input', {'id': 'j_id1:javax.faces.ViewState:0'})['value']

        return windowid, viewstate, dict_city_codes
    else:
        raise Exception('Error in initial request')
    

def extract_city_data(session, windowid, viewstate, dict_city_codes, df_excel, list_cities):
    """
    Extract data about city schools
    Extract data about school professionals

    Args:
        session (requests.Session): requests session
        windowid (str): windowid
        viewstate (str): viewstate
        dict_city_codes (dict): dictionary of cities and their codes
        df_excel (pd.DataFrame): dataframe with the schools data
        list_cities (list): list of cities

    Returns:
        list: list of dataframes with the schools data
    """
    list_df_schools = []

    for city in list_cities:
        school_list = df_excel.loc[df_excel['municipio_normalizada'] == city, 'escola_normalizada'].unique()
        city_code = dict_city_codes[city]['city_code']

        headers = {
            'Accept': 'application/xml, text/xml, */*; q=0.01',
            'Accept-Language': 'en-US,en;q=0.9,pt-BR;q=0.8,pt;q=0.7',
            'Cache-Control': 'no-cache',
            'Connection': 'keep-alive',
            'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
            'Faces-Request': 'partial/ajax',
            'Origin': 'https://www.consultaescolas.pr.gov.br',
            'Pragma': 'no-cache',
            'Referer': 'https://www.consultaescolas.pr.gov.br/consultaescolas-java/pages/templates/initial2.jsf;jsessionid=mt0APdvWFqRvLZKoCVAhAvUnZ5npnhpE4TR-QtPX.sseed75003?windowId=3ea',
            'Sec-Fetch-Dest': 'empty',
            'Sec-Fetch-Mode': 'cors',
            'Sec-Fetch-Site': 'same-origin',
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36',
            'X-Requested-With': 'XMLHttpRequest',
            'sec-ch-ua': '"Google Chrome";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform': '"Windows"',
        }

        data = {
            'javax.faces.partial.ajax': 'true',
            'javax.faces.source': 'initial:j_idt97:municipio',
            'javax.faces.partial.execute': 'initial:j_idt97:municipio',
            'javax.faces.partial.render': 'initial:j_idt97:escola',
            'javax.faces.behavior.event': 'change',
            'javax.faces.partial.event': 'change',
            'initial': 'initial',
            'javax.faces.ViewState': viewstate,
            'initial:j_idt97:nucleo_focus': '',
            'initial:j_idt97:nucleo_input': 'Selecione...',
            'initial:j_idt97:municipio_focus': '',
            'initial:j_idt97:municipio_input': str(city_code),
            'initial:j_idt97:redeEnsino_focus': '',
            'initial:j_idt97:redeEnsino_input': '3',
            'initial:j_idt97:escola_focus': '',
            'initial:j_idt97:escola_input': 'Selecione...',
            'initial:j_idt97:j_idt99_collapsed': 'false',
            'initial:mapa_collapsed': 'true',
            'initial:info_collapsed': 'false',
            'initial:j_idt416_selection': '',
            'initial:j_idt414_collapsed': 'false',
            'initial:j_idt427_selection': '',
            'initial:j_idt425_collapsed': 'false',
            'initial:j_idt424_collapsed': 'false',
            'initial:j_idt505_collapsed': 'false',
        }

        r = session.post(f'https://www.consultaescolas.pr.gov.br/consultaescolas-java/pages/templates/initial2.jsf?windowId={windowid}', headers=headers, data=data)
        logger.info(f'Find schools in {city} - Request status: {r.status_code}')

        # Extract all school codes for the city 
        soup = BeautifulSoup(r.text, 'lxml')
        select_tag = soup.find('select')

        if select_tag:
            option_tag = select_tag.find_all('option')
        else:
            continue

        if len(option_tag) == 0:
            continue

        # Extract data for each school in school_list
        for option in select_tag.find_all('option'):
            if 'value' in str(option):
                if normalize_string(option.text) in school_list:
                    df_escola = extract_school_data(session, city, city_code, option.text, option['value'], windowid)
                    list_df_schools.append(df_escola)

    return list_df_schools


def extract_school_data(session, city, city_code, school, school_code, windowid):
    logger.info(f'Extracting data from {city} - {school} ({city_code}/{school_code})')

    url = f'https://www.consultaescolas.pr.gov.br/consultaescolas-java/pages/templates/initial2.jsf?windowId={windowid}&codigoMunicipio={city_code}&codigoEstab={school_code}'
    r = session.get(url)
    logger.info(f'Access school {school} data - Request status: {r.status_code}')

    soup = BeautifulSoup(r.text, 'html.parser')
    viewstate = soup.find('input', {'id': 'j_id1:javax.faces.ViewState:0'})['value']

    # click to access school professionals 
    data = {
        'initial': 'initial',
        'javax.faces.ViewState': viewstate,
        'initial:j_idt97:nucleo_focus': '',
        'initial:j_idt97:nucleo_input': 'Selecione...',
        'initial:j_idt97:municipio_focus': '',
        'initial:j_idt97:municipio_input': 'Selecione...',
        'initial:j_idt97:redeEnsino_focus': '',
        'initial:j_idt97:redeEnsino_input': '2',
        'initial:j_idt97:escola_focus': '',
        'initial:j_idt97:escola_input': 'Selecione...',
        'initial:j_idt97:j_idt99_collapsed': 'false',
        'initial:j_idt167_collapsed': 'false',
        'initial:listaMapa': '',
        'initial:markerSelecionado': school_code,
        'initial:mapa_collapsed': 'true',
        'initial:info_collapsed': 'false',
        'initial:j_idt484_selection': '',
        'initial:j_idt482_collapsed': 'false',
        'initial:j_idt495:1:j_idt497_selection': '',
        'initial:j_idt495:1:j_idt496_collapsed': 'false',
        'initial:j_idt495:2:j_idt497_selection': '',
        'initial:j_idt495:2:j_idt496_collapsed': 'false',
        'initial:j_idt495:3:j_idt497_selection': '',
        'initial:j_idt495:3:j_idt496_collapsed': 'false',
        'initial:j_idt495:4:j_idt497_selection': '',
        'initial:j_idt495:4:j_idt496_collapsed': 'false',
        'initial:j_idt493_collapsed': 'false',
        'initial:j_idt505_collapsed': 'false',
        'initial:j_idt366': 'initial:j_idt366',
    }

    # Post to access school professionals
    url = f'https://www.consultaescolas.pr.gov.br/consultaescolas-java/pages/templates/initial2.jsf?windowId={windowid}'
    r = session.post(url, data=data)
    logger.info(f'Access school professionals (post) - Request status: {r.status_code}')

    # Get to simulate redirect and get the viewstate
    url = f'https://www.consultaescolas.pr.gov.br/consultaescolas-java/pages/paginas/profissionais/profissionaisEstabelecimento.jsf?windowId={windowid}'

    r = session.get(url)
    logger.info(f'Access school professionals (get) - Request status: {r.status_code}')

    soup = BeautifulSoup(r.text, 'html.parser')
    viewstate = soup.find('input', {'id': 'j_id1:javax.faces.ViewState:0'})['value']

    # Access school professional supply
    data = {
        'merendaForm': 'merendaForm',
        'javax.faces.ViewState': viewstate,
        'merendaForm:j_idt78:nucleo_focus': '',
        'merendaForm:j_idt78:nucleo_input': 'Selecione...',
        'merendaForm:j_idt78:municipio_focus': '',
        'merendaForm:j_idt78:municipio_input': 'Selecione...',
        'merendaForm:j_idt78:redeEnsino_focus': '',
        'merendaForm:j_idt78:redeEnsino_input': '',
        'merendaForm:j_idt78:escola_focus': '',
        'merendaForm:j_idt78:escola_input': 'Selecione...',
        'merendaForm:j_idt78:j_idt80_collapsed': 'false',
        'merendaForm:j_idt160_selection': '',
        'merendaForm:j_idt158_collapsed': 'false',
        'merendaForm:j_idt113': 'merendaForm:j_idt113'
    }

    # Post to access school professional supply
    url = f'https://www.consultaescolas.pr.gov.br/consultaescolas-java/pages/paginas/profissionais/profissionaisEstabelecimento.jsf?windowId={windowid}'
    r = session.post(url, data=data)
    logger.info(f'Access school professional supply (post) - Request status: {r.status_code}')

    # Get to simulate redirect and get the viewstate
    url = f'https://www.consultaescolas.pr.gov.br/consultaescolas-java/pages/paginas/profissionais/demandaSuprimentosEstabelecimento.jsf?windowId={windowid}'
    r = session.get(url)
    logger.info(f'Access school professional supply (get) - Request status: {r.status_code}')

    soup = BeautifulSoup(r.text, 'html.parser')
    viewstate = soup.find('input', {'id': 'j_id1:javax.faces.ViewState:0'})['value']

    # Extract the general table of supplies and demands (big table)
    table = soup.find_all('table', {'role':'grid'})[1]

    # Extract all ids from the table (will be used to merge big table with detailed table)
    ids = []
    for row in table.find_all('tr'):
        for cell in row.find_all('td'):
            if '<div' not in str(cell):
                if 'id="' in str(cell):
                    ids.append(str(cell).split('id="')[1].split('"')[0])
                else:
                    ids.append(None)

    df_big = pd.read_html(str(table))[0]
    df_big['ids'] = ids

    # Extract detailed table of supplies and demands for each id (add dataframes to list)
    list_df = []
    logger.info('Accessing detailed tables of supplies and demands (post)...')
    for id in df_big['ids'].unique():

        url = f'https://www.consultaescolas.pr.gov.br/consultaescolas-java/pages/paginas/profissionais/demandaSuprimentosEstabelecimento.jsf?windowId={windowid}'

        data = {
            'javax.faces.partial.ajax': 'true',
            'javax.faces.source': id,
            'primefaces.ignoreautoupdate': 'true',
            'javax.faces.partial.execute': '@all',
            'javax.faces.partial.render': 'formDemanda:gradeConsultaDetalhe',
            id: id,
            'formDemanda': 'formDemanda',
            'javax.faces.ViewState': viewstate,
            'formDemanda:j_idt71:nucleo_focus': '',
            'formDemanda:j_idt71:nucleo_input': 'Selecione...',
            'formDemanda:j_idt71:municipio_focus': '',
            'formDemanda:j_idt71:municipio_input': 'Selecione...',
            'formDemanda:j_idt71:redeEnsino_focus': '',
            'formDemanda:j_idt71:redeEnsino_input': '',
            'formDemanda:j_idt71:escola_focus': '',
            'formDemanda:j_idt71:escola_input': 'Selecione...',
            'formDemanda:j_idt71:j_idt73_collapsed': 'false',
            'formDemanda:j_idt106_collapsed': 'false',
            'formDemanda:gradeConsultaDetalhe_collapsed': 'false',
        }

        headers = {
        'Accept': 'application/xml, text/xml, */*; q=0.01',
        'Accept-Language': 'en-US,en;q=0.9,pt;q=0.8,es;q=0.7',
        'Cache-Control': 'no-cache',
        'Connection': 'keep-alive',
        'Content-Type': 'application/x-www-form-urlencoded; charset=UTF-8',
        'Faces-Request': 'partial/ajax',
        'Origin': 'https://www.consultaescolas.pr.gov.br',
        'Pragma': 'no-cache',
        'Referer': 'https://www.consultaescolas.pr.gov.br/consultaescolas-java/pages/paginas/profissionais/demandaSuprimentosEstabelecimento.jsf?windowId=ff6',
        'Sec-Fetch-Dest': 'empty',
        'Sec-Fetch-Mode': 'cors',
        'Sec-Fetch-Site': 'same-origin',
        'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/129.0.0.0 Safari/537.36 Edg/129.0.0.0',
        'X-Requested-With': 'XMLHttpRequest',
        'sec-ch-ua': '"Microsoft Edge";v="129", "Not=A?Brand";v="8", "Chromium";v="129"',
        'sec-ch-ua-mobile': '?0',
        'sec-ch-ua-platform': '"Windows"',
        }

        r = session.post(url, data=data, headers=headers)
        df = pd.read_html(r.content)[0]
        df['ids'] = id
        list_df.append(df)
        print('')

    logger.info('Concatenating detailed tables of supplies and demands...')
    df_detail = pd.concat(list_df)
    df_detail = df_detail.dropna(subset='Nome')
    df_detail['municipio'] = city
    df_detail['escola'] = school

    df_big.columns = ['Disciplina - Função', 'Turno', 'Demanda', 'Suprimento', 'Vagas', 'Excessos', 'Detalhes', 'ids']

    logger.info('Merging big table with detailed table...')
    df_merged = df_big.merge(df_detail, on='ids', how='left')
    df_merged = df_merged.fillna(0)

    return df_merged


def main():
    filename = 'escolas_parana.xlsx'
    df_excel, list_cities = extract_cities_schools(filename)
    session = requests.Session()

    windowid, viewstate, dict_city_codes = initial_request(session, 'https://www.consultaescolas.pr.gov.br/consultaescolas-java/pages/templates/initial2.jsf?')
    list_dataframes = extract_city_data(session, windowid, viewstate, dict_city_codes, df_excel, list_cities)

    df_final = pd.concat(list_dataframes)
    df_final.to_csv('escolas_parana.csv', index=False)

if __name__ == "__main__":
    main()
