import pandas as pd
from datetime import date

from selenium import webdriver
from selenium.webdriver.common.by import By
from selenium.webdriver.support.ui import Select
from selenium.webdriver.support.ui import WebDriverWait

import boto3

def create_bovespa_parquet():

    # Criar web driver usando url da b3
    driver = webdriver.Chrome()
    driver.implicitly_wait(30)
    driver.get('https://sistemaswebb3-listados.b3.com.br/indexPage/day/IBOV?language=pt-br')

    # Trocar consulta para Setor de Atuacao
    consulta_dropdown = Select(driver.find_element(By.ID, 'segment'))
    consulta_dropdown.select_by_value('2')

    # Trocar consulta para 120 valores na pagina
    pagina_dropdown = Select(driver.find_element(By.ID, 'selectPage'))
    pagina_dropdown.select_by_visible_text('120')

    # # Esperar até a lista atualizar com todos os elementos
    wait = WebDriverWait(driver, 30)
    wait.until(lambda driver : len(driver.find_elements(By.XPATH, '//table/tbody/tr')) > 20)

    # Criar arquivo parquet
    html_table = driver.find_element(By.CLASS_NAME, 'table').get_attribute('outerHTML')
    df = pd.read_html(html_table, decimal=',', thousands='.')[0]

    df.columns = ['_'.join(col).strip() for col in df.columns.values]
    df = df.rename(columns={
        'Setor_Setor': 'setor',
        'Código_Código': 'codigo',
        'Ação_Ação': 'acao',
        'Tipo_Tipo': 'tipo',
        'Qtde. Teórica_Qtde. Teórica': 'qtd',
        '%Setor_Part. (%)': 'part',
        '%Setor_Part. (%)Acum.': 'acum'
    })

    df['data'] = date.today().strftime('%Y-%m-%d')

    df = df.iloc[:-2, :]

    df.to_parquet('bovespa.parquet')

def upload_s3(file_name, bucket, access_key, secret_key, token, object_name=None):

    session = boto3.Session(
        aws_access_key_id=access_key,
        aws_secret_access_key=secret_key,
        aws_session_token=token
    )
    s3_client = session.client('s3')

    s3_client.upload_file(file_name, bucket, object_name)


create_bovespa_parquet()
# upload_s3(file_path, bucket, access_key, secret_key, token, object_name)