import extratorlattes
import os
import zeep
import pytz
import requests
import html
import xmltodict
import json
from zipfile import ZipFile
import io
import pathlib
from datetime import datetime
from collections import OrderedDict
import pandas


# from database import Database


class Lattes:

    def __init__(self,
                 id=None,
                 wsdl='http://servicosweb.cnpq.br/srvcurriculo/WSCurriculo?wsdl',
                 auto_save_json_to_bd=False,
                 path=r'C:\Lattes_Excrator',
                 verbose=False,
                 show_sql=False,
                 connect_to_db=False):
        """
        Inicializa a classe Lattes.

        Args:
            id (str, optional): ID do currículo Lattes (16 dígitos).
            wsdl (str): URL do WSDL do serviço SOAP do CNPq.
            auto_save_json_to_bd (bool): Se True, salva automaticamente o JSON no banco de dados.
            path (str): Caminho base para salvar arquivos.
            verbose (bool): Se True, imprime mensagens de log.
            show_sql (bool): Se True, imprime comandos SQL executados.
            connect_to_db (bool): Se True, conecta ao banco de dados.
        """
        self.auto_save_json_to_bd = auto_save_json_to_bd
        self.update_errors_on_bd = False
        self.nome = None
        self.CPF = None
        self.verbose = verbose
        self.data_nascimento = None
        self.wsdl = wsdl
        if connect_to_db:
            self.bd = extratorlattes.Database(show_sql=show_sql)
            self.can_connect_db = self.bd.connected
        else:
            self.bd = None
            self.can_connect_db = False
        self.path = path
        self.ocorrencia = None
        self.zip = None
        self.xml = None
        self.soup = None
        self.df = None
        self.json = None
        self.data_atualizacao = None
        self.bd_data_atualizacao = None
        self.replace_file_in_disk = True
        self.palavras_chave = []
        self.dados_gerais = {
            'id': None,
            'data_atualizacao': None,
            'nome_completo': None,
            'nomes_citacao': None,
            'nacionalidade': None,
            'cpf': None,
            'pais_nascimento': None,
            'uf_nascimento': None,
            'cidade_nascimento': None,
            'data_nascimento': None,
            'sexo': None,
            'numero_identidade': None,
            'orgao_emissor_identidade': None,
            'uf_orgao_emissor_identidade': None,
            'data_emissao_identidade': None,
            'numero_passaporte': None,
            'nome_pai': None,
            'nome_mae': None,
            'permissao_divulgacao': None,
            'data_falecimento': None,
            'raca_cor': None,
            'resumo_cv_rh': None,
            'resumo_cv_rh_en': None,
            'outras_informacoes_relevantes': None,
            'email': None,
            'sigla_pais_nacionalidade': None,
            'pais_nacionalidade': None,
            'orcid': None,
            'pcd': None,
        }
        self.can_get_soap = False
        self.id = self.set_id(id)
        self.client = zeep.Client(wsdl=self.wsdl)
        




#       ___           ___           ___           ___
#      /\__\         /\  \         /\  \         /\  \
#     /:/ _/_       /::\  \       /::\  \       /::\  \
#    /:/ /\  \     /:/\:\  \     /:/\:\  \     /:/\:\__\
#   /:/ /::\  \   /:/  \:\  \   /:/ /::\  \   /:/ /:/  /
#  /:/_/:/\:\__\ /:/__/ \:\__\ /:/_/:/\:\__\ /:/_/:/  /
#  \:\/:/ /:/  / \:\  \ /:/  / \:\/:/  \/__/ \:\/:/  /
#   \::/ /:/  /   \:\  /:/  /   \::/__/       \::/__/
#    \/_/:/  /     \:\/:/  /     \:\  \        \:\  \
#      /:/  /       \::/  /       \:\__\        \:\__\
#      \/__/         \/__/         \/__/         \/__/


#       ___           ___           ___           ___
#      /\__\         /\  \         /\  \         /\__\
#     /:/ _/_        \:\  \        \:\  \       /:/  /
#    /:/ /\__\        \:\  \        \:\  \     /:/  /
#   /:/ /:/  /    ___  \:\  \   _____\:\  \   /:/  /  ___
#  /:/_/:/  /    /\  \  \:\__\ /::::::::\__\ /:/__/  /\__\
#  \:\/:/  /     \:\  \ /:/  / \:\~~\~~\/__/ \:\  \ /:/  /
#   \::/__/       \:\  /:/  /   \:\  \        \:\  /:/  /
#    \:\  \        \:\/:/  /     \:\  \        \:\/:/  /
#     \:\__\        \::/  /       \:\__\        \::/  /
#      \/__/         \/__/         \/__/         \/__/


    def set_id(self, id):
        '''
            Define o id do Lattes.
            :Param: id - o id do Lattes que everá ser definido.        
        '''        
        if id is None:
            raise Exception("id must be provided")
        else:
            # self.__init__(
            #      id=id,
            #      wsdl=self.wsdl,
            #      auto_save_json_to_bd=self.auto_save_json_to_bd,
            #      path=self.path,
            #      verbose=self.verbose,
            #      show_sql=self.show_sql,
            #      connect_to_db=self.connect_to_db
            #      )
            if isinstance(id, str):
                if id.isnumeric() and len(id) <= 16:
                    self.id = id
                else:
                    raise Exception(
                        "id must be an integer or a string with at most 16 numbers")
            elif isinstance(id, int) and len(str(id))<=16:
                self.id = str(id)
            else:
                raise Exception(
                    "id must be an integer or a string with at most 16 numbers")
            while len(self.id) < 16:
                self.id = '0' + self.id
            self.dados_gerais['id'] = self.id
        return self.id


    def get_ocorrencia_cv(self):
        '''
            Retorna o status: self.can_get_soap
        '''
        self.ocorrencia = self.client.service.getOcorrenciaCV(self.id)
        return self.ocorrencia

    def check_if_can_get_soap(self):
        '''
            Verifica se o Extrator Lattes está liberado para consulta.
            Define o status: self.can_get_soap
            Retorna True ou False.      
        '''
        self.get_ocorrencia_cv()
        if '<MENSAGEM><ERRO>Serviço negado.IP:' in self.ocorrencia:
            self.can_get_soap = False
        else:
            self.can_get_soap = True
        return self.can_get_soap

    def get_zip_from_SOAP(self, set_auto_save=True, path=None):
        '''
            Acessa o Extrator Lattes e baixa o currículo compactado.
            Param id = None: se o id não for fornecido, usará o id da instância (self.id)
            Param set_auto_save=True: Salva automaticamente o Lattes baixado no HD.
            Param path=self.path : pasta onde os arquivos serão salvos.
        '''
        if path == None:
            path = self.path
        self.ocorrencia = None
        self.zip = None
        if self.verbose:
            print('Pegando Lattes compactado {self.id} via SOAP')
        self.zip = self.client.service.getCurriculoCompactado(self.id)
        if not self.zip is None:
            if self.verbose:
                print('    Lattes compactado recuperado via SOAP. Verificando validade dos dados.')
            if self.get_xml():
                if self.verbose:
                    print('    Lattes compactado válido.')
                self.ocorrencia = "Curriculo recuperado com sucesso!"
                if set_auto_save:
                    salvou_no_disco = self.save_zip_to_disk(path)
                    if salvou_no_disco:
                        if self.verbose:
                            print('Lattes salvo no disco.')
                    else:
                        self.ocorrencia = "Curriculo recuperado com sucesso.mas houve falha ao salvar no disco."
                        if self.verbose:
                            print('Lattes não salvo no disco.')
            else:
                if self.verbose:
                    print(f'    Lattes compactado inválido.')
                if self.ocorrencia is None:
                    self.ocorrencia = self.json.get('MENSAGEM', {}).get('ERRO', None)
                if self.ocorrencia is None:
                    self.ocorrencia = self.get_ocorrencia_cv()
                if self.ocorrencia is None:
                    self.ocorrencia = 'Erro desconhecido ao recuperar o currículo.'
                self.zip = None
                self.json = None
                self.xml = None
                if self.verbose:
                    print(f'        {self.ocorrencia}')
            # if self.auto_save_json_to_bd and not self.json == None:
            #     self.insert_json()
        return self.ocorrencia

    def get_atualizacao_SOAP(self, id=None):
        """
        Consulta a data de atualização do currículo via serviço SOAP.

        Args:
            id (str, optional): ID do currículo. Se None, usa o self.id.

        Returns:
            bool: True se conseguiu obter a data.

        Raises:
            ValueError: Se o ID for inválido ou serviço negado.
        """
        if not id == None:
            self.id = id
        if self.can_get_soap:
            client = zeep.Client(wsdl=self.wsdl)
            self.data_atualizacao = client.service.getDataAtualizacaoCV(
                self.id)
            if self.data_atualizacao == None:
                raise ValueError('Invalid ID')
            elif '<MENSAGEM><ERRO>Serviço negado.' in self.data_atualizacao:
                self.data_atualizacao = None
                raise ValueError(
                    'Your IP does not have permission to download the Curriculum.')
            else:
                self.data_atualizacao = datetime.strptime(
                    self.data_atualizacao, '%d/%m/%Y %H:%M:%S').replace(tzinfo=pytz.UTC)
                return True
        else:
            raise ValueError(
                'Your IP does not have permission to download the Curriculum.')

    def get_id(self, CPF, nome, data_nascimento):
        """
        Recupera o ID Lattes a partir de CPF, nome e data de nascimento.

        Args:
            CPF (str): CPF do pesquisador.
            nome (str): Nome completo.
            data_nascimento (str): Data de nascimento (dd/mm/aaaa).

        Returns:
            str: ID Lattes (16 dígitos).
        """
        self.CPF = CPF
        self.nome = nome
        self.data_nascimento = data_nascimento
        client = zeep.Client(wsdl=self.wsdl)
        id = client.service.getIdentificadorCNPq(
            self.CPF, self.nome, self.data_nascimento)
        if id == None:
            raise ValueError('Invalid CPF, Name or DateBirth informed.')
        elif '<MENSAGEM><ERRO>Serviço negado.' in id:
            raise ValueError(
                'Your IP does not have permission to download the Curriculum.')
        return id

#  _____ _   _  _____ ___________ _____
# |_   _| \ | |/  ___|  ___| ___ \_   _|
#   | | |  \| |\ `--.| |__ | |_/ / | |
#   | | | . ` | `--. \  __||    /  | |
#  _| |_| |\  |/\__/ / |___| |\ \  | |
#  \___/\_| \_/\____/\____/\_| \_| \_/


# ______ _   _ _   _ _____
# |  ___| | | | \ | /  __ \
# | |_  | | | |  \| | /  \/
# |  _| | | | | . ` | |
# | |   | |_| | |\  | \__/\
# \_|    \___/\_| \_/\____/

    def insert_xml(self):
        """
        Insere o XML do currículo na tabela lattes_xml do banco de dados.
        """
        if self.id == None or self.xml == None:
            raise Exception('No id or xml to insert.')
        sql = """INSERT INTO lattes_xml (
            id, xml)
            VALUES(%s, XMLPARSE(DOCUMENT %s)::xml)
            ON CONFLICT (id)
            DO
            UPDATE SET
            xml = EXCLUDED.xml;
            """
        data = (self.id,
                self.xml,
                )
        return self.bd.execute(sql, data)

    def insert_json(self):
        """
        Insere o JSON do currículo na tabela lattes_json do banco de dados.
        """
        if self.id == None or self.json == None:
            raise Exception('No id or json to insert.')
        sql = """INSERT INTO lattes_json (
            id, json)
            VALUES(%s, %s::json)
            ON CONFLICT (id)
            DO
            UPDATE SET
            json = EXCLUDED.json;
            """
        data = (self.id,
                json.dumps(self.json)
               )
        return self.bd.execute(sql, data)

    def insert_lattes_atualizacao_bd(self):
        """
        Atualiza a data de atualização do currículo na tabela lattes_atualizacao.
        """
        if self.id == None or self.data_atualizacao == None:
            raise Exception('No id or data_atualizacao to insert.')
        sql = """INSERT INTO public."lattes_atualizacao"(
            id, last_updated)
            VALUES(%s, TIMESTAMP %s)
            ON CONFLICT (id)
            DO
            UPDATE SET
            last_updated = EXCLUDED.last_updated,
            created_at = now()
            ;
            """
        data = (self.id,
                self.data_atualizacao.strftime('%Y-%m-%dT%H:%M:%S.%f'))
        return self.bd.execute(sql, data)


#  _____  _____ _____
# |  __ \|  ___|_   _|
# | |  \/| |__   | |
# | | __ |  __|  | |
# | |_\ \| |___  | |
#  \____/\____/  \_/


# ______ _   _ _   _ _____
# |  ___| | | | \ | /  __ \
# | |_  | | | |  \| | /  \/
# |  _| | | | | . ` | |
# | |   | |_| | |\  | \__/\
# \_|    \___/\_| \_/\____/

    def get_xml_bd(self):
        """
        Recupera o XML do banco de dados para o ID atual.

        Returns:
            bool: True se encontrou e carregou o XML.
        """
        if self.bd is None:
            raise Exception('No database connection.')
        sql = """SELECT xml from lattes_xml
        where id = %s;
        """
        data = (self.id,)
        resultado = self.bd.query(sql, data, many=False)
        if not resultado == None:
            self.xml = resultado[0]
            return True
        return False

    def get_json_bd(self):
        """
        Recupera o JSON do banco de dados para o ID atual.

        Returns:
            bool: True se encontrou e carregou o JSON.
        """
        if self.bd is None:
            raise Exception('No database connection.')
        sql = """SELECT json from lattes_json
        where id = %s;
        """
        data = (self.id,)
        resultado = self.bd.query(sql, data, many=False)
        if not resultado == None:
            self.json = resultado[0]
            return True
        return False

    def get_lattes_atualizacao_bd(self):
        """
        Recupera a data de atualização armazenada no banco de dados.

        Returns:
            bool: True se encontrou os dados.
        """
        if self.bd is None:
            raise Exception('No database connection.')
        sql = """SELECT created_at, last_updated from lattes_atualizacao
        where id = %s;
        """
        data = (self.id,)
        resultado = self.bd.query(sql, data, many=False)
        if not resultado == None:
            self.bd_data_atualizacao = resultado[0].replace(tzinfo=pytz.UTC)
            self.bd_created_at = resultado[1].replace(tzinfo=pytz.UTC)
            return True
        return False


# d8888b.      d888888b      .d8888.      db   dD
# 88  `8D        `88'        88'  YP      88 ,8P'
# 88   88         88         `8bo.        88,8P
# 88   88         88           `Y8b.      88`8b
# 88  .8D        .88.        db   8D      88 `88.
# Y8888D'      Y888888P      `8888Y'      YP   YD


# d88888b db    db d8b   db  .o88b. d888888b d888888b  .d88b.  d8b   db .d8888.
# 88'     88    88 888o  88 d8P  Y8 `~~88~~'   `88'   .8P  Y8. 888o  88 88'  YP
# 88ooo   88    88 88V8o 88 8P         88       88    88    88 88V8o 88 `8bo.
# 88~~~   88    88 88 V8o88 8b         88       88    88    88 88 V8o88   `Y8b.
# 88      88b  d88 88  V888 Y8b  d8    88      .88.   `8b  d8' 88  V888 db   8D
# YP      ~Y8888P' VP   V8P  `Y88P'    YP    Y888888P  `Y88P'  VP   V8P `8888Y'


    def get_lattes(self, id=None):
        """
        Tenta carregar o currículo Lattes de várias fontes (disco, BD, SOAP).

        Args:
            id (str, optional): ID do currículo.

        Returns:
            bool: True se conseguiu carregar o currículo.
        """
        if not id == None:
            self.id = id
        conseguiu = False
        conseguiu = self.read_zip_from_disk()
        if self.verbose:
            print(f'read_zip_from_disk:{conseguiu}')
        # if not conseguiu:
        #     conseguiu = self.get_json_bd()
           # if self.verbose: print(f'get_json_bd:{conseguiu}')
        if not conseguiu:
            conseguiu = self.read_json_from_disk()
            if self.verbose:
                print(f'read_json_from_disk:{conseguiu}')
        if not conseguiu:
            conseguiu = self.get_zip_from_SOAP()
            if self.verbose:
                print(f'get_zip_from_SOAP:{conseguiu}')
        if conseguiu:
            conseguiu = self.get_xml()
            if self.verbose:
                print(f'get_xml:{conseguiu}')
        return conseguiu

    @staticmethod
    def get_saving_path(type, path, id, create_path_if_new=False):
        """
        Gera o caminho completo para salvar arquivos do Lattes.

        Args:
            type (str): Tipo de arquivo ('zip', 'xml', 'json').
            path (str): Diretório base.
            id (str): ID do Lattes.
            create_path_if_new (bool): Se True, cria os diretórios se não existirem.

        Returns:
            str: Caminho completo do arquivo.
        """
        id = str(id)
        filename = "Lattes_" + str(id) + "." + type
        path_name = os.path.join(path, 'Lattes_' + type.upper())
        if type == "zip":
            full_path = os.path.join(path_name, id[0], id[1])
        else:
            full_path = os.path.join(
                path_name, id[0], id[1], id[2], id[3], id[4])
        if create_path_if_new:
            pathlib.Path(full_path).mkdir(parents=True, exist_ok=True)
        return os.path.join(full_path, filename)

    def read_zip_from_disk(self, filename=None, path=None, get_from_SOAP_if_not_exists=True):
        """
        Lê o arquivo ZIP do disco.

        Args:
            filename (str, optional): Caminho do arquivo.
            path (str, optional): Diretório base.
            get_from_SOAP_if_not_exists (bool): Se True, tenta baixar do SOAP se não achar no disco.

        Returns:
            bool: True se sucesso.
        """
        if path == None:
            path = self.path
        if filename == None:
            filename = Lattes.get_saving_path(
                type="zip", path=path, id=self.id)
        try:
            with open(filename, 'rb') as f:
                self.zip = f.read()
            if self.zip == None:
                if get_from_SOAP_if_not_exists and self.can_get_soap:
                    return self.get_zip_from_SOAP()
                else:
                    return False

        except:
            return False
        return True

    def read_xml_from_disk(self, filename=None, path=None):
        """
        Lê o arquivo XML do disco.

        Args:
            filename (str, optional): Caminho do arquivo.
            path (str, optional): Diretório base.

        Returns:
            bool: True se sucesso.
        """
        if path == None:
            path = self.path
        if filename == None:
            filename = Lattes.get_saving_path("xml", path=path, id=self.id)
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                self.xml = f.read()
            if not self.xml == None:
                return True
            else:
                return False
        except:
            return False

    def read_json_from_disk(self, filename=None, path=None):
        """
        Lê o arquivo JSON do disco.

        Args:
            filename (str, optional): Caminho do arquivo.
            path (str, optional): Diretório base.

        Returns:
            bool: True se sucesso.
        """
        if path == None:
            path = self.path
        if filename == None:
            filename = Lattes.get_saving_path("JSON", path=path, id=self.id)
        try:
            with open(filename, 'r', encoding='utf-8') as f:
                self.json = json.load(f)
            if not self.json == None:
                return True
            else:
                return False
        except:
            return False

    def read_from_disk(self, path=None):
        """
        Tenta ler ZIP, XML e JSON do disco.

        Args:
            path (str, optional): Diretório base.

        Returns:
            bool: True se todos foram lidos com sucesso.
        """
        if path == None:
            path = self.path
        if not path == None:
            self.path = path
        resposta = True
        if not self.read_zip_from_disk():
            resposta = False
        if not self.read_xml_from_disk():
            resposta = False
        if not self.read_json_from_disk():
            resposta = False
        return resposta

    def save_xml_to_disk(self, filename=None, path=None, replace=False):
        """
        Salva o XML no disco.

        Args:
            filename (str, optional): Caminho do arquivo.
            path (str, optional): Diretório base.
            replace (bool): Se True, sobrescreve arquivo existente.

        Returns:
            bool: True se salvou.
        """
        if path == None:
            path = self.path
        if filename == None:
            filename = Lattes.get_saving_path(
                "xml", path=path, id=self.id, create_path_if_new=True)
        if self.verbose:
            print(f'Salvando o arquivo {filename}.')
        if replace or not os.path.isfile(filename):
            with open(filename, 'w', encoding='utf-8') as f:
                f.write(self.xml)
            return True
        return False

    def save_json_to_disk(self, filename=None, path=None, replace=False):
        """
        Salva o JSON no disco.

        Args:
            filename (str, optional): Caminho do arquivo.
            path (str, optional): Diretório base.
            replace (bool): Se True, sobrescreve arquivo existente.

        Returns:
            bool: True se salvou.
        """
        if path == None:
            path = self.path
        if filename == None:
            filename = Lattes.get_saving_path(
                "JSON", path=path, id=self.id, create_path_if_new=True)
        if replace or not os.path.isfile(filename):
            with open(filename, 'w', encoding='utf-8') as f:
                json.dump(self.json, f, indent=4)
            return True
        return False

    def save_zip_to_disk(self, path=None, filename=None, replace=None):
        """
        Salva o ZIP no disco.

        Args:
            path (str, optional): Diretório base.
            filename (str, optional): Caminho do arquivo.
            replace (bool, optional): Se True, sobrescreve. Se None, usa self.replace_file_in_disk.

        Returns:
            bool: True se salvou.
        """
        if replace is None:
            replace = self.replace_file_in_disk
        if path == None:
            path = self.path
        if filename == None:
            filename = Lattes.get_saving_path(
                "zip", path=path, id=self.id, create_path_if_new=True)
        if replace or not os.path.isfile(filename):
            with open(filename, 'wb') as f:
                f.write(self.zip)
            return True
        return False

    def save_to_disk(self, path=None, replace=False):
        """
        Salva ZIP, XML e JSON no disco.

        Args:
            path (str, optional): Diretório base.
            replace (bool): Se True, sobrescreve arquivos existentes.

        Returns:
            bool: True se salvou todos.
        """
        if not path == None:
            self.path = path
        resposta = True
        if not self.save_zip_to_disk(replace=replace):
            resposta = False
        if not self.save_xml_to_disk(replace=replace):
            resposta = False
        if not self.save_json_to_disk(replace=replace):
            resposta = False
        return resposta


#  dP""b8    dP"Yb    88b 88   Yb    dP   888888   88""Yb   888888
# dP   `"   dP   Yb   88Yb88    Yb  dP    88__     88__dP     88
# Yb        Yb   dP   88 Y88     YbdP     88""     88"Yb      88
#  YboodP    YbodP    88  Y8      YP      888888   88  Yb     88

#  888888   88   88   88b 88    dP""b8   888888   88    dP"Yb    88b 88   .dP"Y8
# 88__     88   88   88Yb88   dP   `"     88     88   dP   Yb   88Yb88   `Ybo."
# 88""     Y8   8P   88 Y88   Yb          88     88   Yb   dP   88 Y88   o.`Y8b
# 88       `YbodP'   88  Y8    YboodP     88     88    YbodP    88  Y8   8bodP'

    def recorre_sobre_todo_json(self, d, path=None):
        """
        Percorre recursivamente o JSON para decodificar entidades HTML.

        Args:
            d (dict/list): Estrutura de dados a percorrer.
            path (list, optional): Caminho atual na estrutura.

        Returns:
            dict/list: Estrutura processada.
        """
        if not path:
            path = []
        for k, v in d.items():
            if isinstance(v, dict) or isinstance(v, OrderedDict):
                self.recorre_sobre_todo_json(v, path + [k])
            elif isinstance(v, list):
                num = 0
                for item in v:
                    self.recorre_sobre_todo_json(item, path + [k] + [num])
                    num += 1
            else:
                if not v == None:
                    d[k] = html.unescape(v)
                else:
                    pass
        return d

    def get_xml(self):
        """
        Extrai o XML do arquivo ZIP carregado e converte para JSON.

        Returns:
            bool: True se sucesso.
        """
        if self.zip is None:
            raise Exception('No zip to get xml from.')  

        with ZipFile(io.BytesIO(self.zip)) as myzip:
            with myzip.open(myzip.namelist()[0]) as myfile:
                self.xml = myfile.read()
            self.json = xmltodict.parse(self.xml)
            self.json = self.recorre_sobre_todo_json(self.json)
            self.xml = self.xml.decode(
                'iso-8859-1').replace('encoding="ISO-8859-1" ', '')
        if self.json.get('MENSAGEM', {}).get('ERRO', None) is not None:
            self.ocorrencia = self.json['MENSAGEM']['ERRO']
            self.json = None
            self.xml = None
            self.zip = None
            if self.verbose:
                print(f'        {self.ocorrencia}')
            return False
        if self.verbose:
            print('XML extraído do ZIP e convertido para JSON.')
        if self.auto_save_json_to_bd:
            self.insert_json()
        self.get_id_by_xml()
        return True


    def get_id_by_xml(self):
        """
        Extrai o ID e data de atualização do JSON/XML carregado.
        """
        if self.json is None:
            return None


        curriculo_vitae = self.json.get('CURRICULO-VITAE', {})
        numero_identificador = curriculo_vitae.get('@NUMERO-IDENTIFICADOR')
        if numero_identificador is not None and numero_identificador.isnumeric() and len(numero_identificador) == 16:
            if self.id is None:
                self.set_id(numero_identificador)
            else:
                curr_id = self.id
                self.set_id(numero_identificador)
                if not curr_id == self.id:
                    raise Exception(
                        'O ID do Currículo não corresponde ao ID informado.')

        self.dados_gerais['id'] = self.id

        data = curriculo_vitae.get('@DATA-ATUALIZACAO')
        hora = curriculo_vitae.get('@HORA-ATUALIZACAO')
        if data and hora:
            self.dados_gerais['data_atualizacao'] = datetime.strptime(
                data + hora, '%d%m%Y%H%M%S')
            self.data_atualizacao = self.dados_gerais['data_atualizacao'].replace(
                tzinfo=pytz.UTC)


    def get_dados_gerais(self):
        """
        Extrai os dados gerais do currículo a partir do JSON e os armazena no dicionário self.dados_gerais.
        """
        if not self.json:
            if self.verbose:
                print("JSON do currículo não carregado. Execute get_xml() primeiro.")
            return

        
        curriculo_vitae = self.json.get('CURRICULO-VITAE', {})
        dados_gerais_json = curriculo_vitae.get('DADOS-GERAIS', {})

        # Extraindo os dados usando .get() para evitar KeyErrors
        self.dados_gerais['nome_completo'] = dados_gerais_json.get(
            '@NOME-COMPLETO')
        self.dados_gerais['nomes_citacao'] = dados_gerais_json.get(
            '@NOME-EM-CITACOES-BIBLIOGRAFICAS')
        self.dados_gerais['nacionalidade'] = dados_gerais_json.get(
            '@NACIONALIDADE')
        self.dados_gerais['cpf'] = dados_gerais_json.get('@CPF')
        self.dados_gerais['pais_nascimento'] = dados_gerais_json.get(
            '@PAIS-DE-NASCIMENTO')
        self.dados_gerais['uf_nascimento'] = dados_gerais_json.get(
            '@UF-NASCIMENTO')
        self.dados_gerais['cidade_nascimento'] = dados_gerais_json.get(
            '@CIDADE-NASCIMENTO')
        self.dados_gerais['data_nascimento'] = dados_gerais_json.get(
            '@DATA-NASCIMENTO')
        if self.dados_gerais['data_nascimento'] is not None:
            try:
                self.dados_gerais['data_nascimento'] = datetime.strptime(
                    self.dados_gerais['data_nascimento'], '%d/%m/%Y').date()
            except ValueError:
                self.dados_gerais['data_nascimento'] = None
        self.dados_gerais['sexo'] = dados_gerais_json.get('@SEXO')
        self.dados_gerais['numero_identidade'] = dados_gerais_json.get(
            '@NUMERO-IDENTIDADE')
        self.dados_gerais['orgao_emissor_identidade'] = dados_gerais_json.get(
            '@ORGAO-EMISSOR')
        self.dados_gerais['uf_orgao_emissor_identidade'] = dados_gerais_json.get(
            '@UF-ORGAO-EMISSOR')
        self.dados_gerais['data_emissao_identidade'] = dados_gerais_json.get(
            '@DATA-DE-EMISSAO')
        if self.dados_gerais['data_emissao_identidade'] is not None:
            try:
                self.dados_gerais['data_emissao_identidade'] = datetime.strptime(
                    self.dados_gerais['data_emissao_identidade'], '%d/%m/%Y').date()
            except ValueError:
                self.dados_gerais['data_emissao_identidade'] = None
        self.dados_gerais['numero_passaporte'] = dados_gerais_json.get(
            '@NUMERO-DO-PASSAPORTE')
        self.dados_gerais['nome_pai'] = dados_gerais_json.get('@NOME-DO-PAI')
        self.dados_gerais['nome_mae'] = dados_gerais_json.get('@NOME-DA-MAE')
        self.dados_gerais['permissao_divulgacao'] = dados_gerais_json.get(
            '@PERMISSAO-DE-DIVULGACAO')
        if self.dados_gerais['permissao_divulgacao'] is not None:
            self.dados_gerais['permissao_divulgacao'] = self.dados_gerais['permissao_divulgacao'].upper() == 'SIM'
        self.dados_gerais['data_falecimento'] = dados_gerais_json.get(
            '@DATA-FALECIMENTO')
        if self.dados_gerais['data_falecimento'] is not None:
            try:
                self.dados_gerais['data_falecimento'] = datetime.strptime(
                    self.dados_gerais['data_falecimento'], '%d/%m/%Y').date()
            except ValueError:
                self.dados_gerais['data_falecimento'] = None
        self.dados_gerais['raca_cor'] = dados_gerais_json.get('@RACA-OU-COR')
        self.dados_gerais['sigla_pais_nacionalidade'] = dados_gerais_json.get('@SIGLA-PAIS-NACIONALIDADE')
        self.dados_gerais['pais_nacionalidade'] = dados_gerais_json.get('@PAIS-DE-NACIONALIDADE')
        self.dados_gerais['orcid'] = dados_gerais_json.get('@ORCID-ID')
        self.dados_gerais['pcd'] = dados_gerais_json.get('@PCD')
        if self.dados_gerais['pcd'] is not None:
            self.dados_gerais['pcd'] = self.dados_gerais['pcd'].upper() == 'SIM'

        # Elementos aninhados
        resumo_cv = dados_gerais_json.get('RESUMO-CV', {})
        if resumo_cv:
            self.dados_gerais['resumo_cv_rh'] = resumo_cv.get(
                '@TEXTO-RESUMO-CV-RH')
            self.dados_gerais['resumo_cv_rh_en'] = resumo_cv.get(
                '@TEXTO-RESUMO-CV-RH-EN')
        endereco = dados_gerais_json.get('ENDERECO')
        if endereco:
            self.dados_gerais['email'] = endereco.get('@ELETRONICO')

        outras_info = dados_gerais_json.get(
            'OUTRAS-INFORMACOES-RELEVANTES', {})
        if outras_info:
            self.dados_gerais['outras_informacoes_relevantes'] = outras_info.get(
                '@OUTRAS-INFORMACOES-RELEVANTES')

        if self.verbose:
            print("Dados gerais extraídos com sucesso.")

        return self.dados_gerais

    def get_enderecos(self):
        """
        Extrai os endereços profissional e residencial do currículo.

        Retorna uma lista de dicionários, onde cada dicionário representa um endereço
        e contém todos os seus campos, além do 'id_lattes' e 'tipo_endereco'.
        """
        if self.json is None:
            if self.verbose:
                print("JSON do currículo não carregado. Execute get_xml() primeiro.")
            return None

        enderecos_formatados = []
        dados_gerais = self.json.get('CURRICULO-VITAE', {}).get('DADOS-GERAIS', {})
        if not dados_gerais:
            return []

        endereco_container = dados_gerais.get('ENDERECO', {})
        if not endereco_container:
            return []

        # Itera sobre todas as chaves do container de endereços para ser flexível
        # a outros tipos de endereço que possam existir no XML.
        for chave_json, bloco_endereco in endereco_container.items():
            # Pula os campos de metadados, que geralmente começam com '@'.
            if chave_json.startswith('@'):
                continue

            # Garante que o bloco de endereço é um dicionário ou uma lista.
            if not isinstance(bloco_endereco, (dict, list)):
                continue

            # Define o tipo de endereço a partir da chave do JSON.
            # Ex: 'ENDERECO-PROFISSIONAL' se torna 'PROFISSIONAL'.
            tipo_endereco = chave_json.replace('ENDERECO-', '').upper()

            lista_blocos = [bloco_endereco] if isinstance(bloco_endereco, dict) else bloco_endereco

            for endereco_individual in lista_blocos:
                if not isinstance(endereco_individual, dict):
                    continue
                dados_endereco = {'id_lattes': self.id, 'tipo_endereco': tipo_endereco}
                for chave, valor in endereco_individual.items():
                    chave_limpa = chave.lstrip('@').lower()
                    dados_endereco[chave_limpa] = valor
                enderecos_formatados.append(dados_endereco)

        return enderecos_formatados

    
    def envia_arquivo_para_servidor (self):
        """
        Envia o arquivo ZIP atual para um servidor remoto.

        Returns:
            requests.Response: Resposta do servidor.
        """
        # Enviando um arquivo zipado para o servidor
        files = {"file": self.zip}
        response = requests.post(f"http://albertocampos.ddns.net:8182/upload/{self.id}", files=files)
        return response
    
    def pega_formacao_profissional(self):
        """
        Extrai as atuações profissionais e suas atividades/vínculos aninhados,
        estruturando os dados em um formato relacional, similar a um banco de dados.

        Retorna um dicionário onde cada chave é uma "tabela" (ex: 'atuacoes_profissionais',
        'vinculos') e cada valor é uma lista de dicionários (os "registros").
        """
        if self.json is None:
            if self.verbose:
                print("JSON do currículo não carregado. Execute get_xml() primeiro.")
            return None

        # Estrutura para armazenar os dados extraídos em formato de "tabelas"
        dados_extraidos = {
            "atuacoes_profissionais": [],
            "vinculos": [],
            "atuacao_areas_conhecimento": [],
            "atuacao_palavras_chave": [],
            "atuacao_setores_atividade": [],
            "atividades_direcao_admin": [],
            "atividades_ensino": [],
            "atividades_pesquisa_desenvolvimento": [],
            "atividades_extensao_universitaria": [],
            "atividades_estagio": [],
            "atividades_servico_tecnico_especializado": [],
            "atividades_treinamento_ministrado": [],
            "atividades_outras": []
        }

        atuacoes_container = self.json.get('CURRICULO-VITAE', {}).get('ATUACOES-PROFISSIONAIS', {})
        if not atuacoes_container:
            return dados_extraidos

        lista_atuacoes_raw = atuacoes_container.get('ATUACAO-PROFISSIONAL', [])
        if isinstance(lista_atuacoes_raw, dict):
            lista_atuacoes_raw = [lista_atuacoes_raw]

        for atuacao_raw in lista_atuacoes_raw:
            codigo_instituicao = atuacao_raw.get('@CODIGO-INSTITUICAO-EMPRESA')

            # Helper para normalizar chaves e adicionar IDs de referência
            def _processar_item_aninhado(item_dict, tabela_destino):
                if not isinstance(item_dict, dict): return
                dados = {k.lstrip('@').lower().replace('-', '_'): v for k, v in item_dict.items()}
                dados['id_lattes'] = self.id
                dados['codigo_instituicao_empresa'] = codigo_instituicao
                tabela_destino.append(dados)

            # 1. Processar a atuação principal (campos de primeiro nível)
            dados_atuacao_principal = {k.lstrip('@').lower().replace('-', '_'): v for k, v in atuacao_raw.items() if isinstance(v, str)}
            dados_atuacao_principal['id_lattes'] = self.id
            dados_extraidos['atuacoes_profissionais'].append(dados_atuacao_principal)

            # 2. Processar Vínculos
            vinculos_container = atuacao_raw.get('VINCULOS', {})
            lista_vinculos = vinculos_container.get('VINCULO', [])
            if isinstance(lista_vinculos, dict): lista_vinculos = [lista_vinculos]
            for vinculo in lista_vinculos:
                _processar_item_aninhado(vinculo, dados_extraidos['vinculos'])

            # 3. Processar Atividades
            mapa_atividades = {
                'ATIVIDADES-DE-DIRECAO-E-ADMINISTRACAO': ('DIRECAO-E-ADMINISTRACAO', 'atividades_direcao_admin'),
                'ATIVIDADES-DE-ENSINO': ('ENSINO', 'atividades_ensino'),
                'ATIVIDADES-DE-PESQUISA-E-DESENVOLVIMENTO': ('PESQUISA-E-DESENVOLVIMENTO', 'atividades_pesquisa_desenvolvimento'),
                'ATIVIDADES-DE-EXTENSAO-UNIVERSITARIA': ('EXTENSAO-UNIVERSITARIA', 'atividades_extensao_universitaria'),
                'ATIVIDADES-DE-ESTAGIO': ('ESTAGIO', 'atividades_estagio'),
                'ATIVIDADES-DE-SERVICO-TECNICO-ESPECIALIZADO': ('SERVICO-TECNICO-ESPECIALIZADO', 'atividades_servico_tecnico_especializado'),
                'ATIVIDADES-DE-TREINAMENTO-MINISTRADO': ('TREINAMENTO-MINISTRADO', 'atividades_treinamento_ministrado'),
                'OUTRAS-ATIVIDADES-TECNICO-CIENTIFICA': ('ATIVIDADE-TECNICO-CIENTIFICA', 'atividades_outras'),
            }
            for chave_container, (chave_item, nome_tabela) in mapa_atividades.items():
                container = atuacao_raw.get(chave_container, {})
                lista_itens = container.get(chave_item, [])
                if isinstance(lista_itens, dict): lista_itens = [lista_itens]
                for item in lista_itens:
                    _processar_item_aninhado(item, dados_extraidos[nome_tabela])

            # 4. Processar Áreas do Conhecimento
            areas_container = atuacao_raw.get('AREAS-DO-CONHECIMENTO', {})
            for _, area_dict in areas_container.items():
                if isinstance(area_dict, dict):
                    _processar_item_aninhado(area_dict, dados_extraidos['atuacao_areas_conhecimento'])

            # 5. Processar Palavras-chave e Setores de Atividade
            for chave_container_str, nome_tabela, nome_campo in [
                ('PALAVRAS-CHAVE', 'atuacao_palavras_chave', 'palavra_chave'),
                ('SETORES-DE-ATIVIDADE', 'atuacao_setores_atividade', 'setor_atividade')
            ]:
                container = atuacao_raw.get(chave_container_str, {})
                for _, valor_item in container.items():
                    if valor_item:
                        dados_item = {
                            'id_lattes': self.id,
                            'codigo_instituicao_empresa': codigo_instituicao,
                            nome_campo: valor_item
                        }
                        dados_extraidos[nome_tabela].append(dados_item)

        return dados_extraidos
        
    def pega_publicacoes(self):
        """
        Extrai as produções bibliográficas do currículo, como artigos, livros, capítulos e trabalhos em eventos.
        A função estrutura os dados em um formato relacional, achatando os detalhes de cada publicação
        e tratando a lista de autores corretamente.

        Retorna um dicionário onde cada chave é uma "tabela" (ex: 'artigos_publicados',
        'livros_publicados', etc.) e cada valor é uma lista de dicionários (os "registros").
        """
        if self.json is None:
            if self.verbose:
                print("JSON do currículo não carregado. Execute get_xml() primeiro.")
            return None

        # Estrutura para armazenar os dados extraídos em formato de "tabelas"
        dados_extraidos = {
            "artigos_publicados": [],
            "livros_publicados": [],
            "capitulos_livros": [],
            "trabalhos_em_eventos": [],
            "textos_em_jornais_ou_revistas": [],
            "outras_producoes_bibliograficas": []
        }

        prod_bib_container = self.json.get('CURRICULO-VITAE', {}).get('PRODUCAO-BIBLIOGRAFICA', {})
        if not prod_bib_container:
            return dados_extraidos

        # Função auxiliar para processar uma lista de itens de publicação
        def processar_lista_itens(lista_itens, nome_tabela):
            if not lista_itens:
                return
            
            # Garante que o input seja sempre uma lista para iteração
            if isinstance(lista_itens, dict):
                lista_itens = [lista_itens]

            for item_raw in lista_itens:
                if not isinstance(item_raw, dict):
                    continue

                dados_publicacao = {'id_lattes': self.id}
                autores_list = []

                # Itera sobre as partes da publicação (DADOS-BASICOS, DETALHAMENTO, AUTORES, etc.)
                for part_key, part_value in item_raw.items():
                    if part_key == 'AUTORES':
                        # Trata o campo de autores, que pode ser um dict (autor único) ou uma lista (múltiplos autores)
                        lista_autores_raw = part_value if isinstance(part_value, list) else [part_value]
                        for autor_dict in lista_autores_raw:
                            if isinstance(autor_dict, dict):
                                autores_list.append({k.lstrip('@').lower().replace('-', '_'): v for k, v in autor_dict.items()})
                    elif isinstance(part_value, dict):
                        # Achata outros dicionários aninhados (ex: DADOS-BASICOS-DO-ARTIGO)
                        for attr_key, attr_value in part_value.items():
                            dados_publicacao[attr_key.lstrip('@').lower().replace('-', '_')] = attr_value
                    else:
                        # Adiciona outros atributos de nível superior
                        dados_publicacao[part_key.lstrip('@').lower().replace('-', '_')] = part_value
                
                dados_publicacao['autores'] = autores_list
                dados_extraidos[nome_tabela].append(dados_publicacao)

        # 1. Artigos Publicados
        processar_lista_itens(prod_bib_container.get('ARTIGOS-PUBLICADOS', {}).get('ARTIGO-PUBLICADO', []), "artigos_publicados")

        # 2. Livros e Capítulos
        livros_e_cap_container = prod_bib_container.get('LIVROS-E-CAPITULOS', {})
        processar_lista_itens(livros_e_cap_container.get('LIVROS-PUBLICADOS-OU-ORGANIZADOS', {}).get('LIVRO-PUBLICADO-OU-ORGANIZADO', []), "livros_publicados")
        processar_lista_itens(livros_e_cap_container.get('CAPITULOS-DE-LIVROS-PUBLICADOS', {}).get('CAPITULO-DE-LIVRO-PUBLICADO', []), "capitulos_livros")

        # 3. Trabalhos em Eventos
        processar_lista_itens(prod_bib_container.get('TRABALHOS-EM-EVENTOS', {}).get('TRABALHO-EM-EVENTOS', []), "trabalhos_em_eventos")

        # 4. Textos em Jornais ou Revistas
        processar_lista_itens(prod_bib_container.get('TEXTOS-EM-JORNAIS-OU-REVISTAS', {}).get('TEXTO-EM-JORNAL-OU-REVISTA', []), "textos_em_jornais_ou_revistas")
        
        # 5. Outras Produções Bibliográficas
        processar_lista_itens(prod_bib_container.get('OUTRA-PRODUCAO-BIBLIOGRAFICA', {}).get('OUTRA-PRODUCAO-BIBLIOGRAFICA', []), "outras_producoes_bibliograficas")

        return dados_extraidos


    def pega_formacao_academica(self):
        """
        Extrai a formação acadêmica/titulação (Graduação, Mestrado, Doutorado, Pós-Doutorado).
        Retorna um dicionário com uma única tabela 'formacao' contendo todos os níveis.
        """
        if self.json is None:
            if self.verbose: print("JSON do currículo não carregado.")
            return None

        dados_extraidos = {"formacao": []}
        container = self.json.get('CURRICULO-VITAE', {}).get('DADOS-GERAIS', {}).get('FORMACAO-ACADEMICA-TITULACAO', {})
        if not container:
            return dados_extraidos

        mapa_formacao = {
            'GRADUACAO': 'GRADUACAO',
            'MESTRADO': 'MESTRADO',
            'DOUTORADO': 'DOUTORADO',
            'POS-DOUTORADO': 'POS-DOUTORADO',
            'LIVRE-DOCENCIA': 'LIVRE-DOCENCIA'
        }

        for chave_json, nivel_formacao in mapa_formacao.items():
            lista_itens = container.get(chave_json, [])
            if isinstance(lista_itens, dict):
                lista_itens = [lista_itens]
            
            for item in lista_itens:
                if isinstance(item, dict):
                    dados_item = {k.lstrip('@').lower().replace('-', '_'): v for k, v in item.items()}
                    dados_item['id_lattes'] = self.id
                    dados_item['nivel'] = nivel_formacao
                    dados_extraidos["formacao"].append(dados_item)
        
        return dados_extraidos


    def pega_producao_tecnica(self):
        """
        Extrai todas as produções técnicas do currículo.
        Retorna um dicionário de tabelas, uma para cada tipo de produção técnica.
        """
        if self.json is None:
            if self.verbose: print("JSON do currículo não carregado.")
            return None

        dados_extraidos = {
            "software": [], "patente": [], "produto_tecnologico": [], "processo_ou_tecnica": [],
            "trabalho_tecnico": [], "relatorio_pesquisa": [], "curso_curta_duracao": [],
            "material_didatico": [], "maquete": [], "outra_producao_tecnica": []
        }
        container = self.json.get('CURRICULO-VITAE', {}).get('PRODUCAO-TECNICA', {})
        if not container:
            return dados_extraidos

        mapa_producao = {
            'SOFTWARE': ('SOFTWARE', 'software'),
            'PATENTE': ('PATENTE', 'patente'),
            'PRODUTO-TECNOLOGICO': ('PRODUTO-TECNOLOGICO', 'produto_tecnologico'),
            'PROCESSO-OU-TECNICA': ('PROCESSO-OU-TECNICA', 'processo_ou_tecnica'),
            'TRABALHO-TECNICO': ('TRABALHO-TECNICO', 'trabalho_tecnico'),
            'RELATORIO-DE-PESQUISA': ('RELATORIO-DE-PESQUISA', 'relatorio_pesquisa'),
            'CURSO-DE-CURTA-DURACAO-MINISTRADO': ('CURSO-DE-CURTA-DURACAO-MINISTRADO', 'curso_curta_duracao'),
            'MATERIAL-DIDATICO-OU-INSTRUCIONAL': ('MATERIAL-DIDATICO-OU-INSTRUCIONAL', 'material_didatico'),
            'MAQUETE': ('MAQUETE', 'maquete'),
            'OUTRA-PRODUCAO-TECNICA': ('OUTRA-PRODUCAO-TECNICA', 'outra_producao_tecnica')
        }

        for chave_container, (chave_item, nome_tabela) in mapa_producao.items():
            sub_container = container.get(chave_container, {})
            lista_itens = sub_container.get(chave_item, [])
            if isinstance(lista_itens, dict):
                lista_itens = [lista_itens]

            for item_raw in lista_itens:
                if not isinstance(item_raw, dict): continue
                
                dados_item = {'id_lattes': self.id, 'autores': []}
                for part_key, part_value in item_raw.items():
                    if part_key == 'AUTORES':
                        lista_autores_raw = part_value if isinstance(part_value, list) else [part_value]
                        for autor_dict in lista_autores_raw:
                            if isinstance(autor_dict, dict):
                                dados_item['autores'].append({k.lstrip('@').lower().replace('-', '_'): v for k, v in autor_dict.items()})
                    elif isinstance(part_value, dict):
                        for attr_key, attr_value in part_value.items():
                            dados_item[attr_key.lstrip('@').lower().replace('-', '_')] = attr_value
                    else:
                        dados_item[part_key.lstrip('@').lower().replace('-', '_')] = part_value
                
                dados_extraidos[nome_tabela].append(dados_item)

        return dados_extraidos


    def pega_orientacoes(self):
        """
        Extrai orientações concluídas e em andamento.
        Retorna um dicionário com duas tabelas: 'orientacoes_concluidas' e 'orientacoes_em_andamento'.
        """
        if self.json is None:
            if self.verbose: print("JSON do currículo não carregado.")
            return None

        dados_extraidos = {"orientacoes_concluidas": [], "orientacoes_em_andamento": []}

        def processar_orientacoes(container, nome_tabela, status):
            if not container: return
            
            mapa_niveis = {
                'ORIENTACOES-CONCLUIDAS-PARA-MESTRADO': 'MESTRADO', 'ORIENTACOES-CONCLUIDAS-PARA-DOUTORADO': 'DOUTORADO',
                'ORIENTACOES-CONCLUIDAS-PARA-POS-DOUTORADO': 'POS-DOUTORADO', 'OUTRAS-ORIENTACOES-CONCLUIDAS': 'OUTRAS',
                'ORIENTACAO-EM-ANDAMENTO-DE-MESTRADO': 'MESTRADO', 'ORIENTACAO-EM-ANDAMENTO-DE-DOUTORADO': 'DOUTORADO',
                'ORIENTACAO-EM-ANDAMENTO-DE-POS-DOUTORADO': 'POS-DOUTORADO', 'ORIENTACAO-EM-ANDAMENTO-DE-INICIACAO-CIENTIFICA': 'INICIACAO_CIENTIFICA'
            }

            for chave_item, nivel in mapa_niveis.items():
                if chave_item in container:
                    lista_itens = container.get(chave_item, [])
                    if isinstance(lista_itens, dict):
                        lista_itens = [lista_itens]
                    
                    for item_raw in lista_itens:
                        if not isinstance(item_raw, dict): continue
                        
                        dados_item = {'id_lattes': self.id, 'status': status, 'nivel': nivel}
                        for part_key, part_value in item_raw.items():
                            if isinstance(part_value, dict):
                                for attr_key, attr_value in part_value.items():
                                    dados_item[attr_key.lstrip('@').lower().replace('-', '_')] = attr_value
                            else:
                                dados_item[part_key.lstrip('@').lower().replace('-', '_')] = part_value
                        dados_extraidos[nome_tabela].append(dados_item)

        # Orientações Concluídas
        concluidas_container = self.json.get('CURRICULO-VITAE', {}).get('OUTRA-PRODUCAO', {}).get('ORIENTACOES-CONCLUIDAS', {})
        processar_orientacoes(concluidas_container, 'orientacoes_concluidas', 'CONCLUIDA')

        # Orientações em Andamento
        andamento_container = self.json.get('CURRICULO-VITAE', {}).get('DADOS-COMPLEMENTARES', {}).get('ORIENTACOES-EM-ANDAMENTO', {})
        processar_orientacoes(andamento_container, 'orientacoes_em_andamento', 'EM_ANDAMENTO')

        return dados_extraidos


    def _extrair_secao_simples(self, caminho_container, mapa_itens, nome_tabela_base):
        """Função auxiliar genérica para extrair seções com estrutura similar."""
        if self.json is None:
            if self.verbose: print("JSON do currículo não carregado.")
            return None

        dados_extraidos = {f"{nome_tabela_base}": []}
        container = self.json.get('CURRICULO-VITAE', {})
        for chave in caminho_container:
            container = container.get(chave, {})
        if not container:
            return dados_extraidos

        for chave_json, tipo_item in mapa_itens.items():
            sub_container = container.get(chave_json, {})
            lista_itens = sub_container.get(tipo_item, [])
            if isinstance(lista_itens, dict):
                lista_itens = [lista_itens]
            
            for item_raw in lista_itens:
                if not isinstance(item_raw, dict): continue
                
                dados_item = {'id_lattes': self.id, 'tipo': chave_json}
                for part_key, part_value in item_raw.items():
                    if isinstance(part_value, dict):
                        for attr_key, attr_value in part_value.items():
                            dados_item[attr_key.lstrip('@').lower().replace('-', '_')] = attr_value
                    else:
                        dados_item[part_key.lstrip('@').lower().replace('-', '_')] = part_value
                dados_extraidos[nome_tabela_base].append(dados_item)
        
        return dados_extraidos

    def pega_participacao_bancas(self):
        """Extrai a participação em bancas."""
        caminho = ['DADOS-COMPLEMENTARES', 'PARTICIPACAO-EM-BANCA-TRABALHOS-CONCLUSAO']
        mapa = {
            'PARTICIPACAO-EM-BANCA-DE-MESTRADO': 'BANCA-DE-MESTRADO',
            'PARTICIPACAO-EM-BANCA-DE-DOUTORADO': 'BANCA-DE-DOUTORADO',
            'PARTICIPACAO-EM-BANCA-DE-EXAME-QUALIFICACAO': 'BANCA-DE-EXAME-QUALIFICACAO',
            'PARTICIPACAO-EM-BANCA-DE-APERFEICOAMENTO-ESPECIALIZACAO': 'BANCA-DE-APERFEICOAMENTO-ESPECIALIZACAO',
            'PARTICIPACAO-EM-BANCA-DE-GRADUACAO': 'BANCA-DE-GRADUACAO'
        }
        return self._extrair_secao_simples(caminho, mapa, 'bancas')

    def pega_participacao_eventos(self):
        """Extrai a participação em eventos, congressos, etc."""
        caminho = ['DADOS-COMPLEMENTARES', 'PARTICIPACAO-EM-EVENTOS-CONGRESSOS']
        mapa = {
            'PARTICIPACAO-EM-CONGRESSO': 'CONGRESSO',
            'PARTICIPACAO-EM-FEIRA': 'FEIRA',
            'PARTICIPACAO-EM-SEMINARIO': 'SEMINARIO',
            'PARTICIPACAO-EM-SIMPOSIO': 'SIMPOSIO',
            'PARTICIPACAO-EM-OFICINA': 'OFICINA',
            'PARTICIPACAO-EM-ENCONTRO': 'ENCONTRO',
            'PARTICIPACAO-EM-EXPOSICAO': 'EXPOSICAO',
            'PARTICIPACAO-EM-OLIMPIADA': 'OLIMPIADA'
        }
        return self._extrair_secao_simples(caminho, mapa, 'eventos')

    def pega_premios_titulos(self):
        """Extrai prêmios e títulos."""
        if self.json is None: return None
        dados_extraidos = {"premios_titulos": []}

        container = self.json.get('CURRICULO-VITAE', {}).get('DADOS-COMPLEMENTARES', {}).get('PREMIOS-E-TITULOS', {})
        lista_itens = container.get('PREMIO-OU-TITULO', [])
        if isinstance(lista_itens, dict):
            lista_itens = [lista_itens]
        
        for item in lista_itens:
            if isinstance(item, dict):
                dados_item = {k.lstrip('@').lower().replace('-', '_'): v for k, v in item.items()}
                dados_item['id_lattes'] = self.id
                dados_extraidos["premios_titulos"].append(dados_item)
        return dados_extraidos

    def pega_idiomas(self):
        """Extrai informações de idiomas."""
        if self.json is None: return None
        dados_extraidos = {"idiomas": []}

        container = self.json.get('CURRICULO-VITAE', {}).get('DADOS-GERAIS', {}).get('IDIOMAS', {})
        lista_itens = container.get('IDIOMA', [])
        if isinstance(lista_itens, dict):
            lista_itens = [lista_itens]
        
        for item in lista_itens:
            if isinstance(item, dict):
                dados_item = {k.lstrip('@').lower().replace('-', '_'): v for k, v in item.items()}
                dados_item['id_lattes'] = self.id
                dados_extraidos["idiomas"].append(dados_item)
        return dados_extraidos
        
    def json_to_dataframe(self):
        """
        Percorre todo o JSON do currículo e o converte em um DataFrame pandas.

        Cada elemento terminal (que não é um dicionário ou lista) se torna uma linha no DataFrame.
        O DataFrame resultante é armazenado em self.df.

        O DataFrame terá as seguintes colunas:
        - id: O ID Lattes do currículo.
        - path: O caminho JSON para o elemento, como uma string.
        - key: A chave do elemento.
        - value: O valor do elemento.
        """
        if not self.json:
            if self.verbose:
                print("Dados JSON não carregados. Não é possível criar o DataFrame.")
            self.df = pandas.DataFrame()
            return self.df

        rows = []
        self._flatten_json_recursive(self.json, [], rows)

        self.df = pandas.DataFrame(rows)
        if self.verbose:
            print(f"DataFrame criado com {len(self.df)} linhas.")
        return self.df
    
    def dataframe_pivotado(self):
        """
        Converte o DataFrame longo (self.df) em um DataFrame pivotado (largo).

        Cada "elemento" do JSON (um dicionário de atributos ou um item de uma lista simples)
        se torna uma linha na tabela resultante. As propriedades de um elemento
        se tornam colunas.

        O DataFrame resultante é armazenado em self.df_pivotado.
        """
        if self.df is None or self.df.empty:
            if self.verbose:
                print("DataFrame original está vazio ou não foi gerado. Execute json_to_dataframe() primeiro.")
            self.df_pivotado = pandas.DataFrame()
            return self.df_pivotado

        df = self.df.copy()

        # A chave para a distinção de "elementos" é o tipo da 'key' no DataFrame longo.
        # Chaves de string (ex: '@NOME-COMPLETO') indicam propriedades de um objeto.
        # Chaves de inteiro (ex: 0, 1, 2...) indicam itens em uma lista de valores simples.
        is_property_mask = df['key'].apply(lambda x: isinstance(x, str))

        # --- 1. Processa os objetos com propriedades ---
        df_properties = df[is_property_mask].copy()
        pivoted_props = pandas.DataFrame()
        if not df_properties.empty:
            # O caminho do elemento é o path da propriedade menos a própria chave da propriedade.
            # Isso agrupa todas as propriedades do mesmo objeto.
            df_properties['element_path'] = df_properties['path'].str.rsplit(' -> ', n=1).str[0]
            
            # Pivotar para transformar as chaves (propriedades) em colunas.
            # O 'element_path' identifica unicamente cada objeto.
            pivoted_props = df_properties.pivot_table(
                index=['id', 'element_path'],
                columns='key',
                values='value',
                aggfunc='first'
            ).reset_index()
            # Renomeia a coluna de caminho para o nome padrão 'path'
            pivoted_props.rename(columns={'element_path': 'path'}, inplace=True)

        # --- 2. Processa os itens de listas simples ---
        df_list_items = df[~is_property_mask].copy()
        if not df_list_items.empty:
            # Para itens de lista, cada item já é um elemento. O 'path' completo o identifica.
            # Apenas renomeamos a coluna 'value' para um nome genérico e selecionamos as colunas.
            df_list_items = df_list_items.rename(columns={'value': 'value_item'})[['id', 'path', 'value_item']]

        # --- 3. Combina os resultados ---
        if not pivoted_props.empty:
            final_df = pivoted_props
        elif not df_list_items.empty:
            final_df = df_list_items
        else:
            final_df = pandas.DataFrame()

        self.df_pivotado = final_df
        if self.verbose:
            print(f"DataFrame pivotado criado com {len(self.df_pivotado)} linhas e {len(self.df_pivotado.columns)} colunas.")
        return self.df_pivotado


    def _flatten_json_recursive(self, data, path, rows_list):
        """
        Função auxiliar recursiva para achatar os dados JSON.
        """
        if isinstance(data, (dict, OrderedDict)):
            for key, value in data.items():
                new_path = path + [key]
                if isinstance(value, (dict, OrderedDict, list)):
                    self._flatten_json_recursive(value, new_path, rows_list)
                else:
                    rows_list.append({'id': self.id, 'path': ' -> '.join(map(str, new_path)), 'key': key, 'value': value})
        elif isinstance(data, list):
            for i, item in enumerate(data):
                new_path = path + [str(i)]
                if isinstance(item, (dict, OrderedDict, list)):
                    self._flatten_json_recursive(item, new_path, rows_list)
                else:
                    rows_list.append({'id': self.id, 'path': ' -> '.join(map(str, new_path)), 'key': i, 'value': item})

    def pega_dados_lattes(self):
        """
        Executa todas as funções de extração de dados e retorna um dicionário
        contendo todas as tabelas extraídas do currículo Lattes.
        """
        if self.json is None:
            if self.verbose:
                print("JSON do currículo não carregado. Execute get_xml() primeiro.")
            return None
        self.get_dados_gerais()
        self.dados_completos = {
            "dados_gerais": self.dados_gerais,
            "enderecos": self.get_enderecos(),
            "formacao_academica": self.pega_formacao_academica(),
            "formacao_profissional": self.pega_formacao_profissional(),
            "publicacoes": self.pega_publicacoes(),
            "producao_tecnica": self.pega_producao_tecnica(),
            "orientacoes": self.pega_orientacoes(),
            "bancas": self.pega_participacao_bancas(),
            "eventos": self.pega_participacao_eventos(),
            "premios_titulos": self.pega_premios_titulos(),
            "idiomas": self.pega_idiomas()
        }

        return self.dados_completos
    

    