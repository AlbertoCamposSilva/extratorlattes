import extratorlattes as lt
import os
import sys
import time
import glob
import shutil
import pandas
import json
import csv
import psycopg2
from tkinter import EXCEPTION

from configparser import ConfigParser
from datetime import datetime
from collections import OrderedDict
from sqlalchemy import create_engine, false
from bs4 import BeautifulSoup, Tag
# from Lattes import Lattes
# from Database import Database
# from Indicadores import Indicadores
# from Database import Database
import threading
from functools import wraps
import pickle


class Carga:
    lista_indicadores = {}
    bd_lista_ids = []
    indicadores = pandas.DataFrame()
    dados_pessoais = pandas.DataFrame()
    demanda_bruta = pandas.DataFrame()

    def __init__(self,
                 path='C:/Lattes/',
                 carga='C:/Users/albertos/CNPq/Lattes/Planilhas/R358737.csv',
                 show_execution_time=False,
                 connect_to_bd=True,
                 ):
        self.path = path
        self.temp_path = self.set_temp_path()
        self.show_execution_time = show_execution_time
        self.bd_lista_ids = None
        self.indicadores = []
        self.publicações = []
        self.palavras_chave = []
        self.carga = carga
        self.erros_anteriores = []
        self.arquivos_no_HD = {}
        self.arquivos_no_csv = {}
        self.emails = None
        self.telefones = None
        self.chamada = None
        self.regiao = None
        self.nomes_citação = None
        self.nome_completo = None
        self.nacionalidade = None
        self.CPF = None
        self.data_nascimento = None
        self.sexo = None
        self.areas_do_conhecimento = [
            ('Grande Área', 'Área', 'Sub-Área', 'Especialidade')]
        if connect_to_bd == True:
            try:
                self.bd = lt.Database('CNPq')
                self.connect_to_bd = connect_to_bd
                self.lattes = lt.Lattes()
            except:
                self.connect_to_bd = False

        self.data_mínima_atualização_lattes = '2020-01-01'
        self.log_file = 'd:/indicadores.log'
        self.save_list_to_disk = True
        self.ids_para_atualizar_file = 'd:/lista_ids_para_atualizar.pickle'
        self.ids_para_atualizar = []
        self.ids_para_pular = []
        self.nível_mínimo = None

        # Origens
        self.de_hd = False
        self.de_all_lattes = False
        self.de_bd_demanda_bruta = False
        self.de_dados_pessoais = False

        # Exceções
        self.pular_indicadores = False
        self.pular_palavras_chave = False
        self.pular_areas_conhecimento = False
        self.pular_publicações = False
        self.pular_dados_gerais = False
        self.pular_vinculos = False
        self.pular_erros = False
        self.pular_jsons = False

        # Importações
        self.on_conflic_update = True
        self.importar_indicadores = True
        self.importar_palavras_chave = True
        self.importar_areas_conhecimento = True
        self.importar_publicações = True
        self.importar_dados_gerais = True
        self.importar_vinculos = True

        self.auto_save_json_to_bd = True
        self.pegar_lista_de_importação_de_arquivo = False

        # Para não atualizar indicadores, mas apenas importar o Lattes, definir como False
        self.atualiza_indicadores = True

    def timing(f):
        @wraps(f)
        def wrap(*args, **kw):
            if args[0].show_execution_time:
                ts = datetime.now()
            result = f(*args, **kw)
            if args[0].show_execution_time:
                te = datetime.now()
                print('func:%r took: %2.4f sec' %
                      (f.__name__, (te-ts).total_seconds()))
            return result
        return wrap

    @timing
    def get_list_ids_dados_gerais_data(self, data=None):
        '''
        Pega lista de IDs, já importados ao BD, com data de atualização maior que a informada.
        Variável: data
        Valor padrão: todos os ids (data = "01/01/1900")
        Sintaxe: 
            cg = Carga()
            ids = cg.get_list_ids_dados_gerais_data("01/01/2020")
        '''
        if data == None:
            data = "01/01/1900"
        sql = '''
            select distinct dados_gerais.id from dados_gerais inner join all_lattes
            on dados_gerais.id = all_lattes.id
            WHERE all_lattes.dt_atualizacao >= %s
            '''
        ids = self.bd.query(sql, data)
        return ids

#  .d8b.  db    db d888888b  .d88b.  .88b  d88.  .d8b.  d888888b d88888b
# d8' `8b 88    88 `~~88~~' .8P  Y8. 88'YbdP`88 d8' `8b `~~88~~' 88'
# 88ooo88 88    88    88    88    88 88  88  88 88ooo88    88    88ooooo
# 88~~~88 88    88    88    88    88 88  88  88 88~~~88    88    88~~~~~
# 88   88 88b  d88    88    `8b  d8' 88  88  88 88   88    88    88.
# YP   YP ~Y8888P'    YP     `Y88P'  YP  YP  YP YP   YP    YP    Y88888P


# d88888b db    db d8b   db  .o88b. d888888b d888888b  .d88b.  d8b   db .d8888.
# 88'     88    88 888o  88 d8P  Y8 `~~88~~'   `88'   .8P  Y8. 888o  88 88'  YP
# 88ooo   88    88 88V8o 88 8P         88       88    88    88 88V8o 88 `8bo.
# 88~~~   88    88 88 V8o88 8b         88       88    88    88 88 V8o88   `Y8b.
# 88      88b  d88 88  V888 Y8b  d8    88      .88.   `8b  d8' 88  V888 db   8D
# YP      ~Y8888P' VP   V8P  `Y88P'    YP    Y888888P  `Y88P'  VP   V8P `8888Y'


    @timing
    def get_xml_auto_source(self, id=None, pegar_data_pelos_indicadores=True):
        '''Pega o xml de um Currículo Lattes, sendo que verifica a necessidade antes.

        Será usado a seguinte ordem de prioridade:
        1. Banco de Dados Postgree
        2. Arquivo ZIP
        3. SOAP do Extrator Lattes

        Parâmetros:
        id (str): define o id do Lattes a ser pego.
        '''
        if not id == None:
            self.lattes.id = id
        if self.lattes.bd_dt_atualizacao == None:
            self.lattes.get_lattes()
            if not self.lattes.bd_dt_atualizacao == None:
                if self.lattes.dt_atualizacao == None:
                    self.lattes.get_atualizacao_SOAP()
                    if self.lattes.dt_atualizacao == None:
                        pegar_data_pelos_indicadores = True
        if self.lattes.bd_dt_atualizacao == None or self.lattes.dt_atualizacao == None or (
                self.lattes.dt_atualizacao > self.lattes.bd_dt_atualizacao):
            self.lattes.get_zip_from_SOAP()
        else:
            print('Não é necessário atualizar o currículo.')
        if pegar_data_pelos_indicadores:
            self.lattes.dt_atualizacao = self.lattes.get_atualizacao_JSON()
            print('Data de atualização do Lattes pega pelo XML: ',
                  self.lattes.dt_atualizacao)

        if self.lattes.xml == None:
            self.lattes.get_xml()


# d888888b .88b  d88. d8888b.  .d88b.  d8888b. d888888b
#   `88'   88'YbdP`88 88  `8D .8P  Y8. 88  `8D `~~88~~'
#    88    88  88  88 88oodD' 88    88 88oobY'    88
#    88    88  88  88 88~~~   88    88 88`8b      88
#   .88.   88  88  88 88      `8b  d8' 88 `88.    88
# Y888888P YP  YP  YP 88       `Y88P'  88   YD    YP


#  .d8b.  db      db           db       .d8b.  d888888b d888888b d88888b .d8888.
# d8' `8b 88      88           88      d8' `8b `~~88~~' `~~88~~' 88'     88'  YP
# 88ooo88 88      88           88      88ooo88    88       88    88ooooo `8bo.
# 88~~~88 88      88           88      88~~~88    88       88    88~~~~~   `Y8b.
# 88   88 88booo. 88booo.      88booo. 88   88    88       88    88.     db   8D
# YP   YP Y88888P Y88888P      Y88888P YP   YP    YP       YP    Y88888P `8888Y'


    @staticmethod
    def importa_arquivo_demanda_bruta(
            filename=r'Detalhamento_da_Demanda_Bruta___Lista_de_Propostas.xlsx',
            path=r'C:\Users\silva\CNPq\Lattes\Python\Carga\Demanda Bruta',
            apagar_existente=True,
            atualuizar_bd=True,
            renomear_arquivo=True,
            importar_se_já_existe=False,
            apagar_se_já_existe=True):
        '''
    Importa um arquivo de Demanda Bruta ao BD. 
    O arquivo deve ser gerado por meio do site relatorios.cnpe.br

    Parâmetros:
    path: o caminho do arquivo
    filename: o nome do arquivo
    apagar_existente: para evitar dupla importação, se apagará dados da mesma chamada antes de incluir a imnportação no BD.
    atualuizar_bd: se atualizará o banco de dados
    renomear_arquivo: se o nome do arquivo importado será mudado para formato mais amigável

        '''
        def pega_novo_nome(chamada):
            return "".join(x for x in f'Demanda Bruta {chamada}'.replace(' ', '_').replace("/", "_") if x.isalnum() or x == "_").replace("__", "_")+'.xlsx'

        # Pegar o caminho completo
        file = os.path.join(path, filename)

        # Pegar os dados da chamada
        header = pandas.read_excel(file, header=None, nrows=3)
        chamada = header[3][1]
        data_importação = datetime.strptime(
            header[3][2][-16:], '%d/%m/%Y %H:%M')

        # Pega o teórico novo nome do aquivo e faz verificações
        print(f'Chamada: {chamada}')
        novo_nome = os.path.join(
            os.path.dirname(file), pega_novo_nome(chamada))
        novo_nome_existe = os.path.isfile(novo_nome)

        print(
            f'O arquivo {novo_nome} {"já" if novo_nome_existe else "não"} existe.')

        if importar_se_já_existe == True or novo_nome_existe == False:
            # Importar os dados para memória
            data = pandas.read_excel(file, skiprows=3)

            # Tratamento da Tabela
            del data['E-Mail do Solicitante']
            data = data.rename(
                columns={'Unnamed: 37': 'E-Mail do Solicitante'})
            del data['Nível']
            data = data.rename(columns={'Unnamed: 3': 'Nível'})
            for column in data.columns:
                if 'Unnamed' in column:
                    del data[column]
            data['Chamada'] = chamada
            data['Data Importação'] = data_importação
            data['id'] = data['Link CVLattes do Proponente'].str[-16:].astype(
                'int64')
            data['Início'] = pandas.to_datetime(data['Início'])
            data['Término'] = pandas.to_datetime(data['Término'])
            data['Envio'] = pandas.to_datetime(
                data['Data de Envio'] + " " + data['Hora de Envio'])
            data['Carga'] = pandas.to_datetime(
                data['Data da Carga'] + " " + data['Hora da Carga'])

            del data['Data de Envio']
            del data['Hora de Envio']
            del data['Data da Carga']
            del data['Hora da Carga']
            data['Valor da Bolsa'] = data['Valor da Bolsa'].astype('float')
            data['Valor do Custeio'] = data['Valor do Custeio'].astype('float')
            data['Valor de Capital'] = data['Valor de Capital'].astype('float')
            data['Valor Total'] = data['Valor Total'].astype('float')
            data['CPF do Proponente'] = data['CPF do Proponente'].astype(
                'Int64')

            # Apagar dados anteriores
            if apagar_existente:
                sql = f'''
                delete from demanda_bruta
                where "Chamada" = '{chamada}'
                '''
                db = lt.Database()
                db.execute(sql)

            # Atualizar tabela do Banco de Dados
            if atualuizar_bd:
                data.to_sql(name='demanda_bruta', con=lt.Database.engine(),
                            if_exists='append', index=False)

            # Renomeia op arquivo para nome mais amigável e confirmando a importação

            if renomear_arquivo:
                try:
                    os.rename(file, novo_nome)
                except:
                    if apagar_se_já_existe:
                        os.remove(novo_nome)
                        os.rename(file, novo_nome)
        else:
            print('Arquivo já existente. Pulando.')

    @staticmethod
    def import_demanda_bruta_para_bd(pasta=r'C:\Users\silva\CNPq\Lattes\Python\Carga\Demanda Bruta'):
        files = os.listdir(pasta)
        files_xls = [f for f in files if f[-3:] == 'xls' or f[-4:] == 'xlsx']
        df = pandas.DataFrame()
        for f in files_xls:
            print(f'Importando o arquivo: {f}')
            Carga.importa_arquivo_demanda_bruta(f, pasta)

    @staticmethod
    def get_zip_from_hd_save_xml_and_json_to_hs(path='D:/Lattes/Lattes_ZIP'):
        '''
Pega todos os arquivos compactados no HS e salva as versões nos formatos JSON e XML.
Sintaxe: Carga.get_zip_from_hd_save_xml_and_json_to_hs (caminho)
Caminho Padrão: 'D:/Lattes/Lattes_ZIP'
        '''
        num = 0
        ids_em_JSON = [y[y.find('Lattes_')+7:-4] for x in os.walk('D:/Lattes/Lattes_ZIP')
                       for y in glob.glob(os.path.join(x[0], '*.JSON'))]
        for x in os.walk(path):
            for y in glob.glob(os.path.join(x[0], '*.zip')):
                print(f'Importing file {y}')
                try:
                    lattes = lt.Lattes(path=path)
                    lattes.id = y[-20:-4]
                    if lattes.id not in ids_em_JSON:
                        lattes.read_zip_from_disk(filename=y)
                        lattes.get_xml()
                        lattes.save_xml_to_disk(replace=False)
                        lattes.save_json_to_disk(replace=False)
                        print(f'Saved id {lattes.id} to disk.')
                except:
                    with open('d:/erros.txt', 'a') as file:
                        file.write(y + '\n')

    @staticmethod
    def import_carga_para_bd(arquivo='C:/Users/silva/CNPq/Lattes/Planilhas/R358737.csv'):
        def insere_carga_no_bd(data):
            sql = '''
                INSERT INTO public."all_lattes" 
                    (id, sgl_pais, dt_atualizacao, cod_area, cod_nivel, dta_carga) 
                    VALUES
                    (%(id)s, %(sgl_pais)s, %(dt_atualizacao)s, %(cod_area)s, %(cod_nivel)s, %(dta_carga)s)
                    ON CONFLICT (id)
                    DO
                    UPDATE SET
                    id = EXCLUDED.id,
                    sgl_pais = EXCLUDED.sgl_pais,
                    dt_atualizacao = EXCLUDED.dt_atualizacao,
                    cod_area = EXCLUDED.cod_area,
                    cod_nivel = EXCLUDED.cod_nivel,
                    dta_carga = EXCLUDED.dta_carga;


                '''

            conn = None
            try:
                params = lt.Database.config_db_connection()
                conn = psycopg2.connect(**params)
                cur = conn.cursor()
                cur.executemany(sql, data)
                conn.commit()
                cur.close()
                print("Banco de dados atualizado.")
                return ("Data inserted.")
            except (Exception, psycopg2.DatabaseError) as error:
                print("Erro ao Inserir Dados PEssoais no BD: ", error)
                return (error)
            finally:
                if conn is not None:
                    conn.close()
        dados = []
        with open(arquivo, 'r') as csvfile:
            csv_reader = csv.reader(csvfile, delimiter=',', quotechar='"')
            next(csv_reader)
            num_lines = 0
            for row in csv_reader:
                if row[0].isnumeric():
                    num_lines += 1
                    for index, r in enumerate(row):
                        if r == '':
                            row[index] = None
                        elif r.isnumeric():
                            row[index] = int(r)
                        elif len(r) < 2:
                            row[index] = None
                    data = {
                        'id': row[0],
                        'sgl_pais': row[1],
                        'dt_atualizacao': row[2],
                        'cod_area': row[3],
                        'cod_nivel': row[4],
                        'dta_carga': row[5]
                    }
                    dados.append(data)

        insere_carga_no_bd(dados)

    @staticmethod
    def importa_tabela_en_recursos_humanos_para_bd():
        file = 'D:/Lattes/rh.csv'
        dados = []
        with open(file, encoding='utf8') as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
            numlines = 0
            next(spamreader)
            for row in spamreader:
                # print(row)
                for k, v in enumerate(row):
                    if v == '':
                        row[k] = None
                numlines += 1
                dado = {
                    'COD_RH': int(row[0]),
                    'SGL_UF_CART_IDENT': row[1],
                    'SGL_PAIS_NASC': row[2],
                    'COD_NIVEL_FORM': row[3],
                    'id': row[4],
                    'CPF_RH': row[5],
                    'NME_RH': row[6],
                    'NME_RH_FILTRO': row[7],
                    'TPO_NACIONALIDADE': row[8],
                    'NRO_PASSAPORTE': row[9],
                    'COD_SEXO': row[10],
                    'DTA_NASC': row[11],
                    'NME_CITACAO_BIBLIOG': row[12],
                    'TXT_LOCAL_NASC_RH': row[13],
                    'COD_ESTADO_CIVIL': row[14],
                    'SGL_UF_NASC': row[15],
                    'TXT_SITUACAO_LOCAL': row[16],
                    'TXT_SITUACAO_CNPQ': row[17],
                    'DTA_ENVIO_SIAFI': row[18],
                    'TXT_USERNAME_ULT_ATUALIZ': row[19],
                    'NME_CITACAO_BIBLIOG_FILTRO': row[20],
                    'TPO_DOC_ATUALIZ_CURRIC': row[21],
                    'COD_VERSAO_DOC_ATUALIZ_CURRIC': row[22],
                    'TXT_USERNAME_APLIC_ATUALIZ': row[23],
                    'STA_EXIBICAO_EMAIL': row[24],
                    'DTA_FALECIMENTO': row[25],
                    'NRO_ID_RACA_COR': row[26],
                    'SGL_PAIS_NACIONALIDADE': row[27]
                }
                dados.append(dado)
        sql = '''
            INSERT INTO public."en_recurso_humano" 
                    (
                    COD_RH, 
                    SGL_UF_CART_IDENT, 
                    SGL_PAIS_NASC, 
                    COD_NIVEL_FORM, 
                    id, 
                    CPF_RH, 
                    NME_RH, 
                    NME_RH_FILTRO, 
                    TPO_NACIONALIDADE, 
                    NRO_PASSAPORTE, 
                    COD_SEXO, 
                    DTA_NASC, 
                    NME_CITACAO_BIBLIOG, 
                    TXT_LOCAL_NASC_RH, 
                    COD_ESTADO_CIVIL, 
                    SGL_UF_NASC, 
                    TXT_SITUACAO_LOCAL, 
                    TXT_SITUACAO_CNPQ, 
                    DTA_ENVIO_SIAFI, 
                    TXT_USERNAME_ULT_ATUALIZ, 
                    NME_CITACAO_BIBLIOG_FILTRO, 
                    TPO_DOC_ATUALIZ_CURRIC, 
                    COD_VERSAO_DOC_ATUALIZ_CURRIC, 
                    TXT_USERNAME_APLIC_ATUALIZ, 
                    STA_EXIBICAO_EMAIL, 
                    DTA_FALECIMENTO, 
                    NRO_ID_RACA_COR, 
                    SGL_PAIS_NACIONALIDADE
                    )
                VALUES
                    (
                    %(COD_RH)s, 
                    %(SGL_UF_CART_IDENT)s, 
                    %(SGL_PAIS_NASC)s, 
                    %(COD_NIVEL_FORM)s, 
                    %(id)s, 
                    %(CPF_RH)s, 
                    %(NME_RH)s, 
                    %(NME_RH_FILTRO)s, 
                    %(TPO_NACIONALIDADE)s, 
                    %(NRO_PASSAPORTE)s, 
                    %(COD_SEXO)s, 
                    %(DTA_NASC)s, 
                    %(NME_CITACAO_BIBLIOG)s, 
                    %(TXT_LOCAL_NASC_RH)s, 
                    %(COD_ESTADO_CIVIL)s, 
                    %(SGL_UF_NASC)s, 
                    %(TXT_SITUACAO_LOCAL)s, 
                    %(TXT_SITUACAO_CNPQ)s, 
                    %(DTA_ENVIO_SIAFI)s, 
                    %(TXT_USERNAME_ULT_ATUALIZ)s, 
                    %(NME_CITACAO_BIBLIOG_FILTRO)s, 
                    %(TPO_DOC_ATUALIZ_CURRIC)s, 
                    %(COD_VERSAO_DOC_ATUALIZ_CURRIC)s, 
                    %(TXT_USERNAME_APLIC_ATUALIZ)s, 
                    %(STA_EXIBICAO_EMAIL)s, 
                    %(DTA_FALECIMENTO)s, 
                    %(NRO_ID_RACA_COR)s, 
                    %(SGL_PAIS_NACIONALIDADE)s
                    )
                ON CONFLICT (COD_RH)
                DO
                UPDATE SET
                    COD_RH = EXCLUDED.COD_RH,
                    SGL_UF_CART_IDENT = EXCLUDED.SGL_UF_CART_IDENT,
                    SGL_PAIS_NASC = EXCLUDED.SGL_PAIS_NASC,
                    COD_NIVEL_FORM = EXCLUDED.COD_NIVEL_FORM,
                    id = EXCLUDED.id,
                    CPF_RH = EXCLUDED.CPF_RH,
                    NME_RH = EXCLUDED.NME_RH,
                    NME_RH_FILTRO = EXCLUDED.NME_RH_FILTRO,
                    TPO_NACIONALIDADE = EXCLUDED.TPO_NACIONALIDADE,
                    NRO_PASSAPORTE = EXCLUDED.NRO_PASSAPORTE,
                    COD_SEXO = EXCLUDED.COD_SEXO,
                    DTA_NASC = EXCLUDED.DTA_NASC,
                    NME_CITACAO_BIBLIOG = EXCLUDED.NME_CITACAO_BIBLIOG,
                    TXT_LOCAL_NASC_RH = EXCLUDED.TXT_LOCAL_NASC_RH,
                    COD_ESTADO_CIVIL = EXCLUDED.COD_ESTADO_CIVIL,
                    SGL_UF_NASC = EXCLUDED.SGL_UF_NASC,
                    TXT_SITUACAO_LOCAL = EXCLUDED.TXT_SITUACAO_LOCAL,
                    TXT_SITUACAO_CNPQ = EXCLUDED.TXT_SITUACAO_CNPQ,
                    DTA_ENVIO_SIAFI = EXCLUDED.DTA_ENVIO_SIAFI,
                    TXT_USERNAME_ULT_ATUALIZ = EXCLUDED.TXT_USERNAME_ULT_ATUALIZ,
                    NME_CITACAO_BIBLIOG_FILTRO = EXCLUDED.NME_CITACAO_BIBLIOG_FILTRO,
                    TPO_DOC_ATUALIZ_CURRIC = EXCLUDED.TPO_DOC_ATUALIZ_CURRIC,
                    COD_VERSAO_DOC_ATUALIZ_CURRIC = EXCLUDED.COD_VERSAO_DOC_ATUALIZ_CURRIC,
                    TXT_USERNAME_APLIC_ATUALIZ = EXCLUDED.TXT_USERNAME_APLIC_ATUALIZ,
                    STA_EXIBICAO_EMAIL = EXCLUDED.STA_EXIBICAO_EMAIL,
                    DTA_FALECIMENTO = EXCLUDED.DTA_FALECIMENTO,
                    NRO_ID_RACA_COR = EXCLUDED.NRO_ID_RACA_COR,
                    SGL_PAIS_NACIONALIDADE = EXCLUDED.SGL_PAIS_NACIONALIDADE;
            '''

        conn = None
        try:
            params = lt.Database.config_db_connection()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.executemany(sql, dados)
            conn.commit()
            cur.close()
            print("Banco de dados atualizado.")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Erro ao Inserir Dados PEssoais no BD: ", error)
        finally:
            if conn is not None:
                conn.close()

    def carrega_lista_arquivos_no_HD(self, niveis=False, path=None):
        if path == None:
            path = self.path
        print("Pegando lista de ids já salvos no HD.")
        if niveis:
            ids_já_carregados = {}
        else:
            ids_já_carregados = []
        if niveis:
            for x0 in range(10):
                ids_já_carregados[str(x0)] = {}
                for x1 in range(10):
                    ids_já_carregados[str(x0)][str(x1)] = {}
                    for x2 in range(10):
                        ids_já_carregados[str(x0)][str(x1)][str(x2)] = {}
                        for x3 in range(10):
                            ids_já_carregados[str(x0)][str(
                                x1)][str(x2)][str(x3)] = {}
                            for x4 in range(10):
                                ids_já_carregados[str(x0)][str(
                                    x1)][str(x2)][str(x3)][str(x4)] = []
        diretórios_lidos = 0
        for x in os.walk(path):
            diretórios_lidos += 1
            print(
                f'    {round(diretórios_lidos/1.11,1)}% completos.\r', end="", flush=True)
            for y in glob.glob(os.path.join(x[0], '*.zip')):
                id = y[-20:-4]
                if id.isnumeric():
                    try:
                        if niveis:
                            ids_já_carregados[id[0]][id[1]
                                                     ][id[2]][id[3]][id[4]].append(id)
                        else:
                            ids_já_carregados.append(id)
                    except:
                        print(id)
                        raise Exception
                else:
                    print(f'\n\nErro! {id} não é um número.')
        self.arquivos_no_HD = ids_já_carregados
        return ids_já_carregados

    @staticmethod
    def db_engine():
        return lt.Database.db_engine()

    @staticmethod
    def carrega_dados_gerais(
        path='C:/Users/silva/CNPq/Lattes/Python/Carga/Dados Pessoais de Beneficiários',
        insert_bd=True,
    ):
        print('Carregando tabela atual.')
        engine = Carga.db_engine()
        # Carga.dados_pessoais = pandas.read_sql('dados_pessoais', engine)
        print('Pegando lista de arquivos para baixar:')
        files = [y for x in os.walk(path)
                 for y in glob.glob(os.path.join(x[0], '*.xlsx'))]
        if len(files) > 0:
            for file in files:
                print(f'Carregando arquivo: {file}')
                excel_file = pandas.ExcelFile(file)
                for x in range(len(excel_file.sheet_names)):
                    if x == 0:
                        dt = pandas.read_excel(excel_file, x, header=4)
                        column_names = list(dt)
                    else:
                        dt = pandas.read_excel(
                            excel_file, x, header=None, names=column_names)
                    nome_arquivo = str(file).replace('\\', '/').replace(
                        ' - Dados_Pessoais_de_Beneficiarios_de_Processos_do_CNPq', '').split('/')[-1].split('.')[0]
                    dt['chamada'] = nome_arquivo
                    dt.Lattes = dt.Lattes.str[-16:]
                    columns = {}
                    for column in dt.columns:
                        if column[:7] == 'Unnamed':
                            del dt[column]
                        else:
                            if column == 'Lattes':
                                columns['Lattes'] = 'id'
                            else:
                                columns[column] = column.lower()
                    dt = dt.rename(columns=columns)
                    dt.id = pandas.to_numeric(dt.id)
                    Carga.dados_pessoais = pandas.concat(
                        [dt, Carga.dados_pessoais], ignore_index=True)
            print('Eliminando duplicatas.')
            Carga.dados_pessoais = Carga.dados_pessoais.drop_duplicates()
            if insert_bd:
                print('Inserindo no Banco de Dados.')
                Carga.dados_pessoais.to_sql(
                    'dados_pessoais', engine, if_exists='replace')
        return Carga.dados_pessoais

    @staticmethod
    def atualiza_id_em_demanda_bruta():
        SQL = '''
            update demanda_bruta set 
                id = dados_pessoais.id 
            FROM dados_pessoais
            WHERE lower(unaccent(demanda_bruta."Proponente")) = 
                  lower(unaccent(dados_pessoais.nome))        
        '''
        try:
            params = lt.Database.config_db_connection()
            conn = psycopg2.connect(**params)
            cur = conn.cursor()
            cur.execute(SQL)
            conn.commit()
            cur.close()
            print("Banco de dados atualizado.")
            return ("IDs da Demanda Bruta atualizados")
        except (Exception, psycopg2.DatabaseError) as error:
            print("Erro ao atualizar os IDs: ", error)
            return (error)
        finally:
            if conn is not None:
                conn.close()

    def carrega_erros_anteriores(self, log_file='C:/Users/albertos/CNPq/Lattes/log.txt'):
        print('Carregando erros anteriores.')
        erros = []
        if os.path.exists(log_file):
            with open(log_file) as file:
                json_erros = json.load(file)
            for erro in json_erros:
                if isinstance(erro, dict) or isinstance(erro, OrderedDict):
                    erros.append(erro['id'])
                elif isinstance(erro, list):
                    for err in erro:
                        erros.append(err)
            del json_erros
        self.erros_anteriores = erros
        return erros

    @staticmethod
    def show_progress(tempo_inicio, num_imports_skip_before_log, linhas_totais, linhas_lidas, num_erros):

        if linhas_lidas % num_imports_skip_before_log == 0:
            os.system('cls')
            segundos_por_linha = (
                datetime.now() - tempo_inicio)/num_imports_skip_before_log
            tempo_para_fim = (linhas_totais - linhas_lidas) * \
                segundos_por_linha
            porcentagem = round(100 * (linhas_lidas/linhas_totais), 1)
            segundos_por_linha_total = (
                datetime.now() - tempo_inicio)/(linhas_lidas)
            acabará_em_total = (tempo_inicio + (linhas_totais - linhas_lidas)
                                * segundos_por_linha_total).strftime("%d/%m/%Y, %H:%M:%S")
            resposta = (
                f'Importação iniciada em {tempo_inicio.strftime("%d/%m/%Y, %H:%M:%S")}')
            resposta += (f'\n{porcentagem}% importados.\n')
            if not segundos_por_linha_total.total_seconds() == 0:
                resposta += (
                    f'\nLinhas por segundo lidas (total): {round(1/segundos_por_linha_total.total_seconds(), 1)}')
            resposta += ('\n{:,} de {:,}'.format(linhas_lidas, linhas_totais))
            resposta += ('\n{:,} erros.\n'.format(num_erros))
            resposta += (f'\nAcabará em:')
            resposta += (
                f'\n    Cálculo considerando desde o início: {acabará_em_total}\n\n')
            return resposta
        else:
            return ''

    @staticmethod
    def faz_dimensões():
        variável = {}
        for x0 in range(10):
            variável[str(x0)] = {}
            for x1 in range(10):
                variável[str(x0)][str(x1)] = {}
                for x2 in range(10):
                    variável[str(x0)][str(x1)][str(x2)] = {}
                    for x3 in range(10):
                        variável[str(x0)][str(x1)][str(x2)][str(x3)] = {}
                        for x4 in range(10):
                            variável[str(x0)][str(x1)][str(
                                x2)][str(x3)][str(x4)] = []
        return variável

    def carrega_lista_ids_bd(self, tabela="indicadores", niveis=True, data=None, nível_mínimo=None):
        ids_no_bd = []
        if data == None:
            sql = f'SELECT distinct id from public."{tabela}";'
            if not nível_mínimo == None:
                sql += f'''
                join "all_lattes"
                on CAST ("{tabela}".id as BIGINT) = "all_lattes".id
                WHERE "all_lattes".cod_nivel >= {nível_mínimo}
                '''
        elif tabela == 'all_lattes':
            sql = f'SELECT distinct "{tabela}".id from public."{tabela}"'
            sql += f'\nWHERE "{tabela}".dt_atualizacao >= \'{data}\''
            if not nível_mínimo == None:
                sql += f' and cod_nivel >= {nível_mínimo}'
        else:
            sql = f'SELECT distinct "{tabela}".id from public."{tabela}"'
            sql += f'''
                join "all_lattes"
                on CAST ("{tabela}".id as BIGINT) = "all_lattes".id
                WHERE "all_lattes".dt_atualizacao >= '{data}'
                '''
            if not nível_mínimo == None:
                sql += f' and cod_nivel >= {nível_mínimo}'

        bd_lista_ids = self.bd.query(sql)
        if niveis:
            ids_no_bd = Carga.faz_dimensões()
        for bd_id in bd_lista_ids:
            id = str(bd_id[0])
            while len(id) < 16:
                id = '0' + id
            if niveis:
                ids_no_bd[id[0]][id[1]][id[2]][id[3]][id[4]].append(id)
            else:
                ids_no_bd.append(id)
        return ids_no_bd

    def set_temp_path(self):
        if self.path[-1] == '/':
            tpath = self.path[:-1]
        else:
            tpath = self.path
        self.temp_path = tpath + '_temp' + '/'
        return self.temp_path

    @staticmethod
    def move_files_temp_to_path(path='c:/Lattes/', temp_path='c:/Lattes_temp/'):
        print('Movendo arquivos do diretório temporário para o diretório permanente.')
        files = [y for x in os.walk(temp_path)
                 for y in glob.glob(os.path.join(x[0], '*.zip'))]
        num_files = 0
        for file in files:
            shutil.move(file.replace('\\', '/'),
                        file.replace('\\', '/').replace(temp_path, path))
            num_files += 1
        print(f'Foram movidos {num_files} arquivos.')

    def carrega_ids_do_csv(self, carga, linhas_a_pular=0, data_mínima_de_atualização=-1, reset_lista=True):
        if reset_lista:
            self.arquivos_no_csv = []
        linhas_lidas = 0
        print('\n\nCarregando lista de IDs a importar pela carga de ids no SOAP')
        with open(carga) as csvfile:
            spamreader = csv.reader(csvfile, delimiter=',', quotechar='"')
            print('Pulando primeiras linhas.')
            while linhas_lidas <= linhas_a_pular:
                linhas_lidas += 1
                next(spamreader)
            print('Carregando ids a ler.')
            for row in spamreader:
                linhas_lidas += 1
                id = row[0]
                if len(id) == 16:
                    data_atualizado = datetime.strptime(row[2], '%d/%m/%Y')
                    if (
                            data_mínima_de_atualização < 0 or
                            data_atualizado >= data_mínima_de_atualização
                    ):
                        self.arquivos_no_csv.append(id)
                else:
                    print(f'Erro na linha {linhas_lidas}: {row}')

    def load_carga(self,
                   carga='C:/Users/albertos/CNPq/Lattes/Planilhas/R358737.csv',
                   max=-1,
                   data_mínima_de_atualização=-1,
                   log_file='C:/Users/albertos/CNPq/Lattes/log.txt',
                   linhas_a_pular=0,
                   tempo_a_esperar_em_horário_de_pico=0.5,
                   insere_no_bd=False,
                   num_imports_skip_before_log=100,
                   show_import_messages=False,
                   pular_erros=True,
                   pular_indicadores=True,
                   pula_bd_lattes=True,
                   pula_hd=False,
                   de_bd_demanda_bruta=True,
                   de_bd_dados_pessoais=True,
                   de_dados_pessoais=False,
                   de_carga=True,
                   começando_com=[str(x) for x in range(10)]
                   ):
        """Salva todos os currículos Lattes np HD do computador. Pode ser chamada sem inicialização.

        Exemplo de chamamento da função:
        from Lattes import Lattes
        Lattes.load_carga()

        Parâmetros:
        carga (str): caminho completo de onde se pode achar o arquivo com a carga a ser carregada.
            O arquivo pode ser baixado no seguinte endereço: http://memoria.cnpq.br/web/portal-lattes/extracoes-de-dados
        max (int): número máximo de arquivos a importar. Se negativo serão importados todos os arquivos.

        Returns:
        nothing

        """
        print(r'''
_______  _        ______   _______  _______ _________ _______ 
(  ___  )( \      (  ___ \ (  ____ \(  ____ )\__   __/(  ___  )
| (   ) || (      | (   ) )| (    \/| (    )|   ) (   | (   ) |
| (___) || |      | (__/ / | (__    | (____)|   | |   | |   | |
|  ___  || |      |  __ (  |  __)   |     __)   | |   | |   | |
| (   ) || |      | (  \ \ | (      | (\ (      | |   | |   | |
| )   ( || (____/\| )___) )| (____/\| ) \ \__   | |   | (___) |
|/     \|(_______/|/ \___/ (_______/|/   \__/   )_(   (_______)


 _______  _______  _______  _______  _______  _______ 
(  ____ \(  ___  )(       )(  ____ )(  ___  )(  ____ \
| (    \/| (   ) || () () || (    )|| (   ) || (    \/
| |      | (___) || || || || (____)|| |   | || (_____ 
| |      |  ___  || |(_)| ||  _____)| |   | |(_____  )
| |      | (   ) || |   | || (      | |   | |      ) |
| (____/\| )   ( || )   ( || )      | (___) |/\____) |
(_______/|/     \||/     \||/       (_______)\_______)



 _____     ___    ______    _____     ___  
/  __ \   / _ \   | ___ \  |  __ \   / _ \ 
| /  \/  / /_\ \  | |_/ /  | |  \/  / /_\ \
| |      |  _  |  |    /   | | __   |  _  |
| \__/\  | | | |  | |\ \   | |_\ \  | | | |
 \____/  \_| |_/  \_| \_|   \____/  \_| |_/

        ''')

        # Inicializando Variáveis
        erros = []
        if data_mínima_de_atualização > 0:
            data_mínima_de_atualização = datetime.strptime(
                data_mínima_de_atualização, '%d/%m/%Y')
        fim = ''
        linhas_totais = 0
        ids_para_pular = []
        ids_para_pular_niveis = Carga.faz_dimensões()
        linhas_lidas = 0
        num_erros = 0

        # Creating Subdirectorys, if they don't exixsts
        print('Criando diretórios temporários, se não existirem.')
        if not os.path.isdir(self.temp_path):
            os.makedirs(self.temp_path)
            # print("Created folder : ", self.temp_path)
        for x in range(10):
            file_path1 = os.path.join(self.temp_path, str(x))
            for y in range(10):
                file_path2 = os.path.join(file_path1, str(y))
                CHECK_FOLDER = os.path.isdir(file_path2)
                # If folder doesn't exist, then create it.
                if not CHECK_FOLDER:
                    os.makedirs(file_path2)
                   # print("Created folder : ", file_path2)
                else:
                    # print(file_path2, "folder already exists.")
                    pass

        # Listando IDs a Excluir
        if pular_erros:
            print('\nPegando lista de erros.')
            self.ids_para_pular.extend(self.carrega_erros_anteriores(log_file))
            num_erros = len(ids_para_pular)
        if pular_indicadores:
            self.ids_para_pular.extend(Carga.carrega_lista_ids_bd(
                tabela='indicadores', niveis=False))
        if pula_bd_lattes:
            print('\nCarregando lista de indicadores já na tabela Lattes do BD')
            self.ids_para_pular.appeextendnd(
                Carga.carrega_lista_ids_bd(tabela='lattes', niveis=False))
        if pula_hd:
            print('Gerando lista de ids a atualizar indicadores')
            self.ids_para_pular.extend(self.carrega_lista_arquivos_no_HD())

        # Listando IDs a Incluir
        if de_bd_demanda_bruta:
            print('\nCarregando lista de indicadores já na tabela Latdemanda_bruta do BD')
            self.ids_para_atualizar.extend(Carga.carrega_lista_ids_bd(
                tabela='demanda_bruta', niveis=False))
        if de_bd_dados_pessoais:
            print('\nCarregando lista de indicadores já na tabela Latdemanda_bruta do BD')
            self.ids_para_atualizar.extend(Carga.carrega_lista_ids_bd(
                tabela='dados_pessoais', niveis=False))
        if de_dados_pessoais == True:
            print(
                'Carregando lista de IDs a importar pela carga de dados pessoais do Relatórios do CNPq')
            self.ids_para_atualizar.extend(Carga.carrega_dados_gerais())
        if de_carga == True:
            self.carrega_ids_do_csv(
                carga, linhas_a_pular, data_mínima_de_atualização)

        if not self.ids_para_pular == []:
            print('\nRetirando ids a exluir...')
            entra = set(self.ids_para_atualizar)
            sai = set(self.ids_para_pular)
            string_comeco_importar = [str(f) for f in começando_com]
            self.ids_para_atualizar = [
                id for id in entra if not id in sai and id[0] in string_comeco_importar]
            del entra, sai, string_comeco_importar
            self.ids_para_pular = []

        # print('Limpando memória.')
        # del ids_já_carregados
        # del ids_no_bd

        print('Começando importação.')
        start_time = datetime.now()
        tempo_inicio = datetime.now()
        linhas_lidas = 0
        linhas_totais = len(self.ids_para_atualizar)
        print(f'\n\n Há {linhas_totais} arquivos a importar.\n\n')
        for id in self.ids_para_atualizar:
            linhas_lidas += 1
            # print(f'Importando {id}. -> Salvando arquivo compactado no disco.                              \r', end="", flush=True)
            if linhas_lidas % num_imports_skip_before_log == 0:
                Carga.move_files_temp_to_path(self.path, self.temp_path)
            # os.system('cls')
                print(Carga.show_progress(
                    tempo_inicio, num_imports_skip_before_log, linhas_totais, linhas_lidas, num_erros))
            if not show_import_messages:
                old_stdout = sys.stdout  # backup current stdout -> https://stackoverflow.com/questions/8447185/to-prevent-a-function-from-printing-in-the-batch-console-in-python
                sys.stdout = open(os.devnull, "w")
            lattes = lt.Lattes(id=id, path=self.temp_path,
                            auto_save_json_to_bd=False)
            resposta = lattes.get_zip_from_SOAP()
            # print(resposta, insere_no_bd, resposta == True and insere_no_bd == True)
            if resposta == "Curriculo recuperado com sucesso!" and insere_no_bd == True:
                print(
                    f'Importando {id}. -> Inserindo no Banco de Dados.                              \r', end="", flush=True)
                lattes.get_xml()
                lattes.insert_json()
            elif not resposta == "Curriculo recuperado com sucesso!":
                erro = {
                    'data': datetime.now().strftime("%d/%m/%Y, %H:%M:%S"),
                    'id': id,
                    'erro': resposta
                }
                erros.append(erro)
                with open(log_file, 'w') as log:
                    json.dump(erros, log, indent=4)
            if not show_import_messages:
                sys.stdout = old_stdout  # reset old stdout
            horas_agora = int(datetime.now().time().strftime("%H"))
            if (tempo_a_esperar_em_horário_de_pico > 0 and
                    datetime.today().weekday() < 5 and
                    horas_agora >= 8 and
                    horas_agora <= 18
                    ):
                print(
                    f'Importando {id}. -> Esperando para não sobrecarregar o BD do CNPq.    \r', end="", flush=True)
                time.sleep(tempo_a_esperar_em_horário_de_pico)

            if max > 0 and linhas_lidas > max:
                print('Erros:', erros)
                break
        print('Erros:', erros)

    @staticmethod
    def grava_arquivo_json(
            max=-1,
            path='C:/Downloads/',
            log_file='C:/Users/albertos/CNPq/Lattes/log_save_json.txt',
            show_import_messages=False,
            num_imports_skip_before_log=100):
        """Salva todos os currículos Lattes np HD do computador. Pode ser chamada sem inicialização.

        Exemplo de chamamento da função:
        from Carga import Carga
        Carga.load_carga()

        Parâmetros:
        carga (str): caminho completo de onde se pode achar o arquivo com a carga a ser carregada.
            O arquivo pode ser baixado no seguinte endereço: http://memoria.cnpq.br/web/portal-lattes/extracoes-de-dados
        max (int): número máximo de arquivos a importar. Se negativo serão importados todos os arquivos.

        Returns:
        nothing

        """

        print(r'''
 _______  _        ______   _______  _______ _________ _______ 
(  ___  )( \      (  ___ \ (  ____ \(  ____ )\__   __/(  ___  )
| (   ) || (      | (   ) )| (    \/| (    )|   ) (   | (   ) |
| (___) || |      | (__/ / | (__    | (____)|   | |   | |   | |
|  ___  || |      |  __ (  |  __)   |     __)   | |   | |   | |
| (   ) || |      | (  \ \ | (      | (\ (      | |   | |   | |
| )   ( || (____/\| )___) )| (____/\| ) \ \__   | |   | (___) |
|/     \|(_______/|/ \___/ (_______/|/   \__/   )_(   (_______)


 _______  _______  _______  _______  _______  _______ 
(  ____ \(  ___  )(       )(  ____ )(  ___  )(  ____ \
| (    \/| (   ) || () () || (    )|| (   ) || (    \/
| |      | (___) || || || || (____)|| |   | || (_____ 
| |      |  ___  || |(_)| ||  _____)| |   | |(_____  )
| |      | (   ) || |   | || (      | |   | |      ) |
| (____/\| )   ( || )   ( || )      | (___) |/\____) |
(_______/|/     \||/     \||/       (_______)\_______)



   ___  _____  _____  _   _                                  ______ ______ 
  |_  |/  ___||  _  || \ | |                                 | ___ \|  _  \
    | |\ `--. | | | ||  \| |    _ __    __ _  _ __   __ _    | |_/ /| | | |
    | | `--. \| | | || . ` |   | '_ \  / _` || '__| / _` |   | ___ \| | | |
/\__/ //\__/ /\ \_/ /| |\  |   | |_) || (_| || |   | (_| |   | |_/ /| |/ / 
\____/ \____/  \___/ \_| \_/   | .__/  \__,_||_|    \__,_|   \____/ |___/  

        ''')
        erros = []
        fim = ''
        linhas_lidas = 0

        print('\n\nPegando lista de arquivos zip a importar.')
        ids_em_zip = [y[y.find('Lattes_')+7:-4] for x in os.walk(path)
                      for y in glob.glob(os.path.join(x[0], '*.zip'))]
        ids_no_bd = Carga.carrega_lista_ids_bd(tabela='lattes', niveis=False)

        print('\nGerando lista de arquivos a transformar em JSON')

        arquivos = [id for id in ids_em_zip if id not in ids_no_bd[id[0]]
                    [id[1]][id[2]][id[3]][id[4]]]
        linhas_totais = len(arquivos)
        print("Linhas a ler: ", linhas_totais)

        print("Apagando itens desnecessários da memória")
        del ids_no_bd
        del ids_em_zip

        num_erros = len(erros)

        print('Começando importação.')
        tempo_inicio = datetime.now()
        for id in arquivos:
            linhas_lidas += 1
            os.system('cls')
            print(Carga.show_progress(tempo_inicio, num_imports_skip_before_log,
                  linhas_totais, linhas_lidas, num_erros))

            print(f"Importing id {id}.\r", end="", flush=True)
            try:
                if not show_import_messages:
                    old_stdout = sys.stdout  # backup current stdout -> https://stackoverflow.com/questions/8447185/to-prevent-a-function-from-printing-in-the-batch-console-in-python
                    sys.stdout = open(os.devnull, "w")
                lattes = lt.Lattes(id=id)
                lattes.path = path
                lattes.read_xml_from_zip()
                print(id + ': ' + lattes.insert_json() + '\r')
                if not show_import_messages:
                    sys.stdout = old_stdout  # reset old stdout
            except EXCEPTION as e:
                erro = {
                    'data': datetime.now().strftime("%d/%m/%Y, %H:%M:%S"),
                    'id': id,
                    'erro': str(e)
                }
                erros.append(erro)
                with open(log_file, 'w') as log:
                    json.dump(erros, log, indent=4)

            if max > 0 and linhas_lidas > max:
                break


# _________ _______  _______  _______  _______ _________
# \__   __/(       )(  ____ )(  ___  )(  ____ )\__   __/
#    ) (   | () () || (    )|| (   ) || (    )|   ) (
#    | |   | || || || (____)|| |   | || (____)|   | |
#    | |   | |(_)| ||  _____)| |   | ||     __)   | |
#    | |   | |   | || (      | |   | || (\ (      | |
# ___) (___| )   ( || )      | (___) || ) \ \__   | |
# \_______/|/     \||/       (_______)|/   \__/   )_(


# _________ _        ______  _________ _______  _______  ______   _______
# \__   __/( (    /|(  __  \ \__   __/(  ____ \(  ___  )(  __  \ (  ___  )
#    ) (   |  \  ( || (  \  )   ) (   | (    \/| (   ) || (  \  )| (   ) |
#    | |   |   \ | || |   ) |   | |   | |      | (___) || |   ) || |   | |
#    | |   | (\ \) || |   | |   | |   | |      |  ___  || |   | || |   | |
#    | |   | | \   || |   ) |   | |   | |      | (   ) || |   ) || |   | |
# ___) (___| )  \  || (__/  )___) (___| (____/\| )   ( || (__/  )| (___) |
# \_______/|/    )_)(______/ \_______/(_______/|/     \|(______/ (_______)

#  _______  _______  _______
# (  ____ )(  ____ \(  ____ \
# | (    )|| (    \/| (    \/
# | (____)|| (__    | (_____
# |     __)|  __)   (_____  )
# | (\ (   | (            ) |
# | ) \ \__| (____/\/\____) |
# |/   \__/(_______/\_______)

    def set_approach(a, b):
        return list(set(a)-set(b))

    def get_list_of_ids_to_update(self):

        ids_para_pular = []
        self.ids_para_atualizar = []

        # Origens
        if self.de_hd:
            print(
                'Carregando lista de indicadores para importar dos Lattes baixados no HD')
            self.ids_para_atualizar.extend(
                self.carrega_lista_arquivos_no_HD(niveis=False))
        if self.de_all_lattes:
            print(
                '\nCarregando lista de indicadores para importar da tabela all_lattes do BD')
            self.ids_para_atualizar.extend(self.carrega_lista_ids_bd(
                tabela='all_lattes', niveis=False, data=self.data_mínima_atualização_lattes, nível_mínimo=self.nível_mínimo))
        if self.de_bd_demanda_bruta:
            print(
                '\nCarregando lista de indicadores para importar da tabela demanda_bruta do BD')
            self.ids_para_atualizar.extend(self.carrega_lista_ids_bd(
                tabela='demanda_bruta', niveis=False, data=self.data_mínima_atualização_lattes))
        if self.de_dados_pessoais:
            print(
                '\nCarregando lista de indicadores para importar da tabela dados_pessoais do BD')
            self.ids_para_atualizar.appextendend(self.carrega_lista_ids_bd(
                tabela='dados_pessoais', niveis=False, data=self.data_mínima_atualização_lattes))

        # Exceções
        if self.pular_indicadores:
            print('\nCarregando lista de Ids da tabela indicadores para pular')
            ids_para_pular.extend(self.carrega_lista_ids_bd(
                tabela='indicadores', niveis=False))
        if self.pular_palavras_chave:
            print('\nCarregando lista de Ids da tabela palavras_chave para pular')
            ids_para_pular.extend(self.carrega_lista_ids_bd(
                tabela='palavras_chave', niveis=False))
        if self.pular_areas_conhecimento:
            print('\nCarregando lista de Ids da tabela areas_conhecimento para pular')
            ids_para_pular.extend(self.carrega_lista_ids_bd(
                tabela='areas_conhecimento', niveis=False))
        if self.pular_publicações:
            print('\nCarregando lista de Ids da tabela publicacoes para pular')
            ids_para_pular.extend(self.carrega_lista_ids_bd(
                tabela='publicacoes', niveis=False))
        if self.pular_dados_gerais:
            print('\nCarregando lista de Ids da tabela dados_gerais para pular')
            ids_para_pular.extend(self.carrega_lista_ids_bd(
                tabela='dados_gerais', niveis=False))
        if self.pular_vinculos:
            print('\nCarregando lista de Ids da tabela vinculos para pular')
            ids_para_pular.extend(self.carrega_lista_ids_bd(
                tabela='vinculos', niveis=False))
        if self.pular_erros:
            print('\nPegando lista de erros para pular.')
            ids_para_pular.extend(self.carrega_erros_anteriores(self.log_file))
        if self.pular_jsons:
            print('\nPegando lista de jsons já importados para pular.')
            ids_para_pular.extend(self.carrega_lista_ids_bd(
                tabela='lattes_json', niveis=False))

        print('\nRetirando ids a exluir...')
        self.ids_para_atualizar = list(
            set(self.ids_para_atualizar)-set(ids_para_pular))

        if self.save_list_to_disk:
            print('Salvando o arquivo com a lista no disco.')
            # Pickling https://stackoverflow.com/questions/27745500/how-to-save-a-list-to-a-file-and-read-it-as-a-list-type
            with open(self.ids_para_atualizar_file, "wb") as fp:
                pickle.dump(self.ids_para_atualizar, fp)
        return self.ids_para_atualizar

    def atualiza_todos_os_indicadores(self,
                                      num_imports_skip_before_log=100,
                                      começando_com=None,
                                      ):

        if self.pegar_lista_de_importação_de_arquivo:
            # Unpickling - https://stackoverflow.com/questions/27745500/how-to-save-a-list-to-a-file-and-read-it-as-a-list-type
            with open(self.ids_para_atualizar_file, "rb") as fp:
                self.ids_para_atualizar = pickle.load(fp)
        else:
            self.get_list_of_ids_to_update()

        print('Iniciando Importação.')
        os.system('cls')
        tempo_inicio = datetime.now()
        num_imports_skip_before_log = 10000
        linhas_totais = len(self.ids_para_atualizar)
        num = 0

        for id in self.ids_para_atualizar:
            num += 1
            if começando_com == None or id[0:len(começando_com)] == começando_com:
                print(f'{num} / {linhas_totais}. Importing id {id}.\r',
                      end="", flush=True)

                ind = lt.Indicadores(
                    id=id, auto_save_json_to_bd=self.auto_save_json_to_bd)
                ind.on_conflic_update = self.on_conflic_update
                if self.atualiza_indicadores:
                    ind.atualiza(
                        indicadores=self.importar_indicadores,
                        palavras_chave=self.importar_palavras_chave,
                        areas_conhecimento=self.importar_areas_conhecimento,
                        publicações=self.importar_publicações,
                        dados_gerais=self.importar_dados_gerais,
                        vinculos=self.importar_vinculos
                    )
            if num % num_imports_skip_before_log == 0:
                segundos_por_linha = (datetime.now() - tempo_inicio)/num
                tempo_para_fim = (linhas_totais - num) * segundos_por_linha
                porcentagem = round(100 * (num/linhas_totais), 1)
                acabará_em_total = (
                    datetime.now() + tempo_para_fim).strftime("%d/%m/%Y, %H:%M:%S")
                resposta = (
                    f'Importação iniciada em {tempo_inicio.strftime("%d/%m/%Y, %H:%M:%S")}')
                resposta += (f'\n{porcentagem}% importados.\n')
                if not segundos_por_linha.total_seconds() == 0:
                    resposta += (
                        f'\nLinhas por segundo lidas (total): {round(1/segundos_por_linha.total_seconds(), 1)}')
                resposta += ('\n{:,} de {:,}'.format(num, linhas_totais))
                resposta += (f'\nAcabará em:')
                resposta += (
                    f'\n    Cálculo considerando desde o início: {acabará_em_total}\n\n')
                os.system('cls')
                print(resposta)
