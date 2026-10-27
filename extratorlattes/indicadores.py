import extratorlattes
from extratorlattes.lattes_database import LattesDatabase as Database
from datetime import datetime
from shutil import ExecError
import json
import time
from collections import OrderedDict
# from Lattes import Lattes
# from acs_toolbox.database import Database
import psycopg2
import requests
import json
import pandas
from functools import wraps
import numpy as np


# Tabelas derivadas do lattes_json que foram removidas do banco do CNPq em 10/2026 por estarem paradas
# (cobriam menos de 2% dos currículos). O código de gravação continua aqui, desligado por padrão.
# NOTA PARA LLMs/AGENTES: para vínculos, publicações, palavras-chave e áreas de atuação de um pesquisador,
# PRIORIZE consultas diretas ao lattes_json (índices idx_lattes_fts e idx_lattes_identificacao). Não ligue as
# opções legadas de atualiza() nem recrie estas tabelas sem pedido explícito do usuário.
# Atenção: o catálogo oficial de áreas do conhecimento do CNPq é outra tabela, cnpq_areas_conhecimento.
TABELAS_LEGADAS = ('lattes_palavras_chave', 'lattes_areas_conhecimento', 'lattes_publicacoes', 'lattes_vinculos')


class Indicadores:

    def __init__(self,
                 id_lattes=None,
                 json=None,
                 filename=None,
                 path=r'C:\Lattes_Excrator',
                 show_execution_time=False,
                 show_sql=False,
                 auto_save_json_to_bd=False,
                 on_conflic_update=True,
                 verbose=False,
                 read_from_soap=True,
                 dbparams=None,
                 config_file=None,
                 auto_load_Lattes = False,
                 get_from_zip = False,
                 get_from_lattes_json = False,
                 get_from_soap = False,
                 database_instance = None,
                 verificar_gravacao = True,
                 lattes = None
                 ):
        """
        Inicializa a classe Indicadores para extração e análise de dados do Lattes.

        Args:
            id_lattes (str, optional): ID do currículo Lattes.
            json (dict, optional): JSON do currículo.
            filename (str, optional): Nome do arquivo para carregar.
            show_execution_time (bool): Se True, mostra o tempo de execução das funções.
            show_sql (bool): Se True, mostra os comandos SQL.
            auto_save_json_to_bd (bool): Se True, salva o JSON no BD automaticamente.
            on_conflic_update (bool): Se True, atualiza registros existentes no BD em caso de conflito.
            verbose (bool): Se True, imprime mensagens de log.
            read_from_soap (bool): Se True, tenta ler do SOAP se não encontrar localmente.
            dbparams (dict, optional): Parâmetros de conexão com o banco de dados.
            config_file (str): Caminho do arquivo de configuração do banco de dados.
        """
        self.id_lattes = id_lattes
        self.path = path
        self.erro = False
        self.filename = filename
        self.show_execution_time = show_execution_time
        self.on_conflic_update = on_conflic_update
        self.show_sql = show_sql
        self.dbparams = dbparams
        self.auto_save_json_to_bd = auto_save_json_to_bd
        self.config_file = config_file
        self.read_from_soap = read_from_soap
        self.salva_no_bd_se_inexistente = True
        self.verificar_gravacao = verificar_gravacao
        
        if database_instance:
            self.db = database_instance
            # Se for uma instância externa, assumimos que a conexão é gerenciada externamente
            # mas podemos ajustar parâmetros locais se necessário
            self.db.show_sql = self.show_sql
        else:
            self.db = Database(
                show_sql=self.show_sql, 
                on_conflict_do_update=self.on_conflic_update,
                config_file=self.config_file,
                dbparams=self.dbparams)
                    
        self.lattes_indicadores = []
        self.lattes_lista_indicadores = {}
        self.id_para_nome_map = {}
        self.lista_de_publicações = []
        self.lattes_palavras_chave = []
        self.lattes_areas_conhecimento = []
        self.areas = {
            'grande_area': [],
            'area': [],
            'sub-area': [],
            'especialidade': []
        }
        self.lattes_dados_gerais = None
        self.publicações = []
        self.verbose = verbose

        # Pegando o Lattes
        if lattes:
            self.lattes = lattes
            self.lattes.get_xml() # Garantindo que o XML seja carregado para extrair os dados gerais
            if self.lattes.json:
                self.get_dados_gerais()
                self.get_lista_indicadores()     
            self.id_lattes = self.lattes.id_lattes   
        elif auto_load_Lattes:
            if self.id_lattes is not None:
                self.lattes = extratorlattes.Lattes(
                    id_lattes,
                    connect_to_db=True,
                    auto_save_json_to_bd=self.auto_save_json_to_bd,
                    verbose=self.verbose,
                    path=self.path,)
                if get_from_zip and self.lattes.read_zip_from_disk():
                    self.lattes.get_xml()
                    self.get_dados_gerais()
                elif get_from_lattes_json and self.lattes.get_json_bd():
                    self.lattes.get_xml()
                    self.get_dados_gerais()
                elif get_from_soap and self.lattes.check_if_can_get_soap():
                    if self.lattes.get_lattes():
                        self.get_dados_gerais()
            elif not filename == None:
                load = False
                if filename[:-3].lower() == "zip":
                    load = self.lattes.read_zip_from_disk(filename=filename)
                    if load:
                        self.lattes.get_xml()
                        self.lattes.get_lattes()
                        self.get_dados_gerais()
                elif filename[:-3].lower() == "json":
                    load = load and self.lattes.read_json_from_disk(
                        filename=filename)
                if not load:
                    raise Exception(
                        'Não foi possível recuperar o currículo do arquivo informado.')
            else:
                raise Exception(
                    'Ao menos um deve ser especificado, id_lattes, json ou filename.')
            self.get_lista_indicadores()
        else:
            self.lattes = extratorlattes.Lattes

    def __enter__(self):
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        """
        Exit the runtime context and close the connection.
        The parameters describe the exception that caused the context to be exited.
        If the context was exited without an exception, all three arguments will be None.
        """
        if exc_type is not None:
            # An exception occurred, rollback transaction
            self.db.rollback()
        else:
            # No exception, commit transaction
            self.db.close()


    @staticmethod
    def timing(f):
        """
        Decorador para medir e imprimir o tempo de execução de métodos.
        """
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

    def _exige_tabela_legada(self, tabela):
        """
        Interrompe a gravação com erro claro se a tabela legada não existir no banco.
        Nunca cria a tabela automaticamente: recriá-la é decisão do usuário (ver TABELAS_LEGADAS).
        """
        existe, _ = self.db.query("SELECT to_regclass(%s)", (tabela,), many=False)
        if existe is None or existe[0] is None:
            raise RuntimeError(
                f"A tabela legada '{tabela}' não existe neste banco (foi removida por estar desatualizada). "
                f"Use os dados direto do lattes_json ou recrie a tabela conscientemente com extratorlattes.schema.")

    def get_list(self, d):
        """
        Garante que o retorno seja uma lista. Se d não for lista, retorna [d].
        """
        if isinstance(d, list):
            return d
        return [d]

    def inteiro(self, n):
        """
        Converte n para inteiro de forma segura. Retorna None se falhar.
        """
        if type(n) == int:
            return n
        elif type(n) == float:
            return int(n)
        elif not type(n) == str:
            return None
        elif type(n) == str and len(n) > 0:
            for x in n:
                if not x in ['0', '1', '2', '3', '4', '5', '6', '7', '8', '9']:
                    return None
            return int(n)
        else:
            return None

    @timing
    def get_indicadores(self):
        """
        Extrai lattes_indicadores numéricos (com @ANO) do JSON do Lattes.

        Returns:
            bool: True se sucesso.
        """
        if self.lattes == None or self.lattes.json == None:
            return False
        self.lattes_indicadores = []

        def procura_todos_anos(d, path=None):
            if not path:
                path = []
            for k, v in d.items():
                if isinstance(v, dict) or isinstance(v, OrderedDict):
                    procura_todos_anos(v, path + [k])
                elif isinstance(v, list):
                    num = 0
                    for item in v:
                        procura_todos_anos(item, path + [k] + [num])
                        num += 1
                else:
                    valor = self.inteiro(v)
                    if not valor == None:
                        if k[:4] == '@ANO':
                            nome_indicador = k + ' '
                            for num in range(-1, -len(path), -1):
                                num_entrada_indicador = 0
                                if not str(path[num]).isnumeric() and not str(path[num])[:5] == 'DADOS':
                                    nome_indicador += path[num] + ' '
                                    break
                            indicador = {
                                'id_lattes': self.id_lattes,
                                'ano': valor,
                                # 'path': path + [k], #Não usado mais
                                'tipo': self.get_num_indicador(nome_indicador.strip()),
                                'qty': 1
                            }
                            self.lattes_indicadores.append(indicador)
                            # self.lattes_lista_indicadores.append(
                            #     (self.id_lattes, valor, json.dumps(path + [k]), nome_indicador, 1))
                    else:
                        pass
            return self.lattes_indicadores

        # Reitera sobre todo o Lattes e pega qualquer indicador com ANO
        self.lattes_indicadores = procura_todos_anos(self.lattes.json)

        # Agrupa e soma os lattes_indicadores para reduzir o número de linhas
        if len(self.lattes_indicadores )> 0:
            data = pandas.DataFrame(self.lattes_indicadores)
            data = data.groupby(["id_lattes", "ano", "tipo"]).agg(
                qty=('qty', 'sum')
            ).reset_index()
            self.lattes_indicadores = json.loads(data.to_json(orient='records'))

        return True

    @timing
    def salva_indicadores_no_bd(self):
        """
        Salva os lattes_indicadores extraídos no banco de dados.

        Returns:
            int/bool: Número de registros inseridos ou True/False.
        """
        if self.lattes_indicadores == None or len(self.lattes_indicadores) == 0:
            return False
        sql = ''

        # Apgando se for para atualizar
        if self.on_conflic_update:
            sql = 'delete from lattes_indicadores where id_lattes = %s;'
            sql = self.db.cursor.mogrify(sql, (self.id_lattes,))
            self.db.execute(sql)
            self.db.registrar_log(self.id_lattes, 'Indicadores antigos apagados para atualização.')

        # # Inserindo
        # args_str = ','.join((self.db.cur.mogrify("(%s,%s,%s,%s)", x).decode(
        #     "utf-8")) for x in self.lattes_indicadores)
        # sql = '''
        #         insert into lattes_indicadores
        #             (id_lattes, ano, tipo, qty)
        #         VALUES ''' + (args_str) + ';'
        # sql = self.db.cursor.mogrify(sql)
        # return self.db.execute(sql)

        # if self.lattes_indicadores == None or len(self.lattes_indicadores) == 0:
        #     if self.verbose:
        #         print('A lista de lattes_indicadores é inexistente ou está vazia.')
        #     return False
        # sql = ''

        # Inserindo
        if self.verbose:
            print("Inserindo Indicadores no BD.")

        chaves_inseridas = self.db.insert_list_of_dicts(
            table_name='lattes_indicadores',
            list_of_dicts=self.lattes_indicadores,
            id_columns=['id_lattes', 'ano', 'tipo'])
        self.db.registrar_log(self.id_lattes, f'Indicadores inseridos/atualizados no BD: {len(self.lattes_indicadores)} registros.')

        # if chaves_inseridas and (isinstance(chaves_inseridas, list) or isinstance(chaves_inseridas, int)) and len(chaves_inseridas) > 0:
        #     return len(chaves_inseridas)
        return chaves_inseridas

    # REVER ESSA FUNÇÃO DEPOIS!
    @timing
    def verifica_indicadores_no_bd(self):
        """
        Verifica se os lattes_indicadores presentes na memória (self.lattes_indicadores)
        correspondem aos que estão no banco de dados para o ID atual.

        Retorna:
            bool: True se os lattes_indicadores na memória e no BD correspondem, False caso contrário.
        """
        if not self.id_lattes:
            if self.verbose:
                print("ID do Lattes não está definido. Não é possível verificar.")
            return False

        if not self.db or not self.db.connected:
            if self.verbose:
                print("Não há conexão com o banco de dados.")
            return False

        # 1. Pega os lattes_indicadores do BD
        sql = "SELECT ano, tipo, qty FROM lattes_indicadores WHERE id_lattes = %s ORDER BY ano, tipo"
        try:
            # O ID é char(16), então passamos como string
            indicadores_bd_raw = self.db.query(sql, [self.id_lattes])[0][0]
            if indicadores_bd_raw is None:
                indicadores_bd_raw = []
        except Exception as e:
            if self.verbose:
                print(f"Erro ao consultar lattes_indicadores no BD: {e}")
            return False

        indicadores_bd = [{'ano': r[0], 'tipo': r[1], 'qty': r[2]} for r in indicadores_bd_raw]

        # 2. Prepara os lattes_indicadores da memória para comparação
        # Remove a chave 'id_lattes' e ordena da mesma forma que a query do BD
        indicadores_memoria = sorted(
            [{'ano': ind['ano'], 'tipo': ind['tipo'], 'qty': ind['qty']} for ind in self.lattes_indicadores],
            key=lambda x: (x['ano'], x['tipo'])
        )
        if self.verbose:
            print(f"Indicadores na memória: {len(indicadores_memoria)}")
            print(f"Indicadores no BD: {len(indicadores_bd)}")

        # 3. Compara as duas listas de dicionários
        if indicadores_memoria == indicadores_bd:
            if self.verbose:
                if indicadores_memoria:
                    print(f"Verificação bem-sucedida: {len(indicadores_memoria)} lattes_indicadores correspondem entre memória e BD.")
                else:
                    print("Verificação bem-sucedida: Nenhum indicador na memória e nenhum no BD para este ID.")
            return True
        else:
            if self.verbose:
                print("Verificação falhou: os dados dos lattes_indicadores não correspondem.")
                print(f"  - Indicadores na memória: {len(indicadores_memoria)}")
                print(f"  - Indicadores no BD: {len(indicadores_bd)}")
            return False
    
    def return_first_element_of(self, x):
        """
        Retorna o primeiro elemento de x, seja lista, dict ou valor simples.
        """
        if x == None:
            return None
        elif x == '':
            return None
        elif isinstance(x, list):
            return x[0]
        elif isinstance(x, OrderedDict):
            return x[x.keys()[0]]
        elif isinstance(x, dict):
            for key in x.keys():
                return x[key]
        else:
            return x

    @timing
    def get_lista_indicadores(self):
        """
        Carrega a lista de tipos de lattes_indicadores do banco de dados.

        Returns:
            dict: Mapa de nome do indicador para ID.
        """
        sql = 'SELECT id, nome_indicador from lattes_lista_indicadores'
        if self.show_sql:
            print(f'SQL: {sql}')
        result = self.db.query(sql)[0]

        # Limpa os dicionários para garantir que não haja dados antigos ao recarregar
        self.lattes_lista_indicadores.clear()
        self.id_para_nome_map.clear()

        for num_id, nome_indicador in result:
            self.lattes_lista_indicadores[nome_indicador] = num_id
            self.id_para_nome_map[num_id] = nome_indicador
        if self.verbose:
            print("SQL executado.")
        return self.lattes_lista_indicadores

    #@timing
    def get_num_indicador(self, nome_indicador, salva_no_bd_se_inexistente=None):
        """
        Obtém o ID numérico de um tipo de indicador, criando-o se não existir.

        Args:
            nome_indicador (str): Nome do indicador.

        Returns:
            int: ID do indicador.
        """
        if salva_no_bd_se_inexistente == None:
            salva_no_bd_se_inexistente = self.salva_no_bd_se_inexistente
        nome_indicador = nome_indicador.strip()

        if nome_indicador not in self.lattes_lista_indicadores:
            if not salva_no_bd_se_inexistente:
                return None
            sql = 'INSERT INTO lattes_lista_indicadores (nome_indicador) VALUES (%s) RETURNING id_lattes'
            new_id = self.db.query(sql, (nome_indicador,), many=False)[0][0]
            self.db.commit()
            self.lattes_lista_indicadores[nome_indicador] = new_id
            self.id_para_nome_map[new_id] = nome_indicador
        return self.lattes_lista_indicadores[nome_indicador]


    @timing
    def get_nome_indicador(self, num_indicador):
        """
        Obtém o nome do indicador a partir do seu ID numérico.

        Args:
            num_indicador (int): ID do indicador.

        Returns:
            str: Nome do indicador.
        """
        # Se o mapa reverso ainda não foi populado, popule-o.
        # A verificação `if not self.id_para_nome_map:` é mais eficiente e pythonica.
        if not self.id_para_nome_map:
            self.get_lista_indicadores()

        # Usa o método .get() do dicionário para uma busca segura (O(1) em média).
        # Retorna '' se a chave (num_indicador) não for encontrada.
        return self.id_para_nome_map.get(num_indicador, '')

    @timing
    def get_lista_indicadores_com_nomes(self):
        """
        Retorna a lista de lattes_indicadores extraídos enriquecida com os nomes dos tipos.

        Returns:
            list: Lista de dicionários de lattes_indicadores.
        """
        indicadores_com_nomes = []
        if self.lattes == None or self.lattes.json == None:
            return False
        if len(self.lattes_indicadores) == 0:
            self.get_indicadores()
        for indicador in self.lattes_indicadores:
            indicador['descricao'] = self.get_nome_indicador(indicador['tipo'])
            indicadores_com_nomes.append(indicador)
        return indicadores_com_nomes

    @timing
    def get_palavras_chave(self, json=None, path=None, lattes_palavras_chave=None):
        """
        Extrai palavras-chave do JSON do Lattes.

        Returns:
            bool: True se sucesso.
        """
        if lattes_palavras_chave == None:
            lattes_palavras_chave = self.lattes_palavras_chave
        if json == None:
            json = self.lattes.json
        if json == None:
            return False

        def procura_palavras_chave(json, path, lattes_palavras_chave):
            if not path:
                path = []
            for k, v in json.items():
                if isinstance(v, dict) or isinstance(v, OrderedDict):
                    procura_palavras_chave(
                        json=v, path=path + [k], lattes_palavras_chave=lattes_palavras_chave)
                elif isinstance(v, list):
                    num = 0
                    for item in v:
                        procura_palavras_chave(
                            json=item, path=path + [k] + [num], lattes_palavras_chave=lattes_palavras_chave)
                        num += 1
                else:
                    if not v == None:
                        if k[0:15] == '@PALAVRA-CHAVE-':
                            if (len(v) > 2):
                                if not v in lattes_palavras_chave:
                                    lattes_palavras_chave.append(v)
                    else:
                        pass
            return lattes_palavras_chave

        self.lattes_palavras_chave = procura_palavras_chave(
            json=json, path=path, lattes_palavras_chave=lattes_palavras_chave)
        return True

    @timing
    def salva_palavras_chave_no_bd(self):
        """
        Salva as palavras-chave extraídas no banco de dados.
        LEGADO: tabela lattes_palavras_chave (ver TABELAS_LEGADAS). Prefira o lattes_json.
        """
        if self.lattes_palavras_chave == None or len(self.lattes_palavras_chave) == 0:
            if self.verbose:
                print('A lista de palavras chaves é inexistente ou está vazia.')
            return False
        self._exige_tabela_legada('lattes_palavras_chave')
        if self.on_conflic_update:
            sql = "DELETE from lattes_palavras_chave WHERE id_lattes = %s;\n"
            sql = self.db.cursor.mogrify(sql, (self.id_lattes,)).decode("utf-8")
            self.db.registrar_log(self.id_lattes, 'Palavras-chave antigas apagadas para atualização.')
        else:
            sql = ''
        args_str = ','.join((self.db.cursor.mogrify(
            "(%s,%s)", (str(self.id_lattes), x)).decode("utf-8")) for x in self.lattes_palavras_chave)
        sql += '''INSERT into lattes_palavras_chave (id_lattes, palavra)
        VALUES
            ''' + args_str
        if self.show_sql:
            print(f'SQL a ser executado: {sql}')
        registros = self.db.execute(sql)
        self.db.registrar_log(self.id_lattes, f'Palavras-chave inseridas/atualizadas no BD: {registros} registros.')
        return registros

    @timing
    def get_areas_conhecimento(self):
        """
        Extrai áreas do conhecimento do JSON do Lattes.

        Returns:
            bool: True se sucesso.
        """
        if self.lattes.json == None or self.lattes.json.get("CURRICULO-VITAE") == None:
            return False
        áreas = self.lattes.json.get("CURRICULO-VITAE")
        if not áreas == None:
            áreas = áreas.get("DADOS-GERAIS")
            if not áreas == None:
                áreas = áreas.get('AREAS-DE-ATUACAO')
                if not áreas == None:
                    for area in áreas:
                        areas2 = self.get_list(
                            self.lattes.json["CURRICULO-VITAE"]["DADOS-GERAIS"]['AREAS-DE-ATUACAO'][area])
                        for area2 in areas2:
                            if area2.get('@NOME-GRANDE-AREA-DO-CONHECIMENTO') and len(area2['@NOME-GRANDE-AREA-DO-CONHECIMENTO']) > 1:
                                self.lattes_areas_conhecimento.append((
                                    self.id_lattes,
                                    "grande-area",
                                    area2['@NOME-GRANDE-AREA-DO-CONHECIMENTO'])
                                )
                                self.areas['grande_area'].append(
                                    area2['@NOME-GRANDE-AREA-DO-CONHECIMENTO'])
                            if area2.get('@NOME-DA-AREA-DO-CONHECIMENTO') and len(area2['@NOME-DA-AREA-DO-CONHECIMENTO']) > 1:
                                self.lattes_areas_conhecimento.append((
                                    self.id_lattes,
                                    "area",
                                    area2['@NOME-DA-AREA-DO-CONHECIMENTO']))
                                self.areas['area'].append(
                                    area2['@NOME-DA-AREA-DO-CONHECIMENTO'])
                            if area2.get('@NOME-DA-SUB-AREA-DO-CONHECIMENTO') and len(area2['@NOME-DA-SUB-AREA-DO-CONHECIMENTO']) > 1:
                                self.lattes_areas_conhecimento.append((
                                    self.id_lattes,
                                    "sub-area",
                                    area2['@NOME-DA-SUB-AREA-DO-CONHECIMENTO']))
                                self.areas['sub-area'].append(
                                    area2['@NOME-DA-SUB-AREA-DO-CONHECIMENTO'])
                            if area2.get('@NOME-DA-ESPECIALIDADE') and len(area2['@NOME-DA-ESPECIALIDADE']) > 1:
                                self.lattes_areas_conhecimento.append((
                                    self.id_lattes,
                                    "especialidade",
                                    area2['@NOME-DA-ESPECIALIDADE']))
                                self.areas['especialidade'].append(
                                    area2['@NOME-DA-ESPECIALIDADE'])
        return True

    @timing
    def salva_areas_do_conhecimento_no_bd(self):
        """
        Salva as áreas de atuação do currículo no banco de dados.
        LEGADO: tabela lattes_areas_conhecimento (id_lattes, tipo, area), ver TABELAS_LEGADAS. Prefira o lattes_json.
        Não confundir com o catálogo oficial de áreas do CNPq (cnpq_areas_conhecimento).
        """
        if self.lattes_areas_conhecimento == None or len(self.lattes_areas_conhecimento) == 0:
            return False
        self._exige_tabela_legada('lattes_areas_conhecimento')
        sql = ''
        if self.on_conflic_update:
            sql += self.db.cursor.mogrify(
                'delete from lattes_areas_conhecimento where id_lattes = %s;', (self.id_lattes,)).decode("utf-8")
        args_str = ','.join((self.db.cursor.mogrify("(%s,%s,%s)", x).decode(
            "utf-8")) for x in self.lattes_areas_conhecimento)
        sql = '''
        insert into lattes_areas_conhecimento (id_lattes, tipo, area) VALUES \n''' + args_str + '''
        ON CONFLICT (id_lattes, tipo, area) DO NOTHING'''
        sql = self.db.cursor.mogrify(sql)
        if self.show_sql:
            print(f'SQL a ser executado: {sql}')
        retorno = self.db.execute(sql)
        self.db.registrar_log(self.id_lattes, f'Áreas do conhecimento inseridas/atualizadas no BD: {retorno} registros.')
        return retorno

    @timing
    def get_publicações(self):
        """
        Extrai publicações (com DOI, ISBN, ISSN) do JSON do Lattes.

        Returns:
            bool: True se sucesso.
        """
        if self.lattes.json == None:
            return False

        def procura_todos_DOIs(d, path=None):
            if not path:
                path = []
            for k, v in d.items():
                if isinstance(v, dict) or isinstance(v, OrderedDict):
                    procura_todos_DOIs(v, path + [k])
                elif isinstance(v, list):
                    num = 0
                    for item in v:
                        procura_todos_DOIs(item, path + [k] + [num])
                        num += 1
                else:
                    if not v == None:
                        if (k[:4] == '@DOI'
                                    # or k[:5] == '@ISBN' or k[:5] == '@ISSN'
                                ):
                            nome_indicador = k + ' '
                            for num in range(-1, -len(path), -1):
                                if not str(path[num]).isnumeric() and not str(path[num])[:5] == 'DADOS':
                                    nome_indicador += path[num] + ' '
                                    break
                            titulo = ''
                            ano = 0
                            natureza = ''
                            for o, p in d.items():
                                if o.find('TIT') > -1 and o.find('ING') == -1:
                                    titulo = p
                                if o[:4] == '@ANO' and not self.inteiro(p) == None and self.inteiro(p) > 0:
                                    ano = self.inteiro(p)
                                if o[:9] == '@NATUREZA':
                                    natureza = p
                            if len(titulo) > 0:
                                publicação = {
                                    'id_lattes': self.id_lattes,
                                    'DOI': v,
                                    'path': path + [k],
                                    'tipo': nome_indicador,
                                    'titulo': titulo,
                                    'ano': ano,
                                    'natureza': natureza,
                                }
                                self.publicações.append(publicação)
                                self.lista_de_publicações.append((self.id_lattes, v, json.dumps(
                                    path + [k]), nome_indicador, titulo, ano, natureza))
                    else:
                        pass

        procura_todos_DOIs(self.lattes.json)
        return True

    @timing
    def salva_publicações_no_bd(self):
        """
        Salva as publicações extraídas no banco de dados.
        LEGADO: tabela lattes_publicacoes (ver TABELAS_LEGADAS). Prefira o lattes_json.
        """
        if self.lista_de_publicações == None or len(self.lista_de_publicações) == 0:
            return False
        self._exige_tabela_legada('lattes_publicacoes')
        sql = ''
        args_str = ','.join((self.db.cursor.mogrify("\n(%s, %s, %s, %s, %s, %s, %s)", x).decode(
            "utf-8")) for x in self.lista_de_publicações)
        if self.verbose:
            print(f'Argumentos: {args_str}')
        if self.on_conflic_update:
            delete_sql = '''DELETE from lattes_publicacoes WHERE id_lattes = %s;\n'''
            sql += self.db.cursor.mogrify(delete_sql,
                                          (self.id_lattes,)).decode("utf-8")
        sql += '''INSERT INTO lattes_publicacoes (id_lattes, doi, path, tipo, titulo, ano, natureza)
        VALUES ''' + args_str + ''';
        '''
        if self.show_sql:
            print(f'SQL a ser executado: {sql}')
        retorno = self.db.execute(sql)
        self.db.registrar_log(self.id_lattes, f'Publicações inseridas/atualizadas no BD: {retorno} registros.')
        return retorno

    @timing
    def get_dados_gerais(self):
        """
        Extrai dados gerais do Lattes.

        Returns:
            bool: True se sucesso.
        """
        self.lattes.get_dados_gerais()
        self.lattes_dados_gerais = self.lattes.lattes_dados_gerais
        return True

    @timing
    def salva_dados_gerais_no_bd(self):
        """
        Salva dados gerais no banco de dados.
        """
        if self.lattes_dados_gerais == None or len(self.lattes_dados_gerais) == 0:
            return False
        retorno = self.db.insert_dict('lattes_dados_gerais', self.lattes_dados_gerais, on_conflict=['id_lattes'])
        self.db.registrar_log(self.id_lattes, f'Dados gerais inseridos/atualizados no BD.')
        return retorno

    @timing
    def get_vinculos(self):
        """
        Extrai vínculos profissionais do JSON do Lattes.

        Returns:
            bool: True se sucesso.
        """
        self.lattes_vinculos = []
        if self.lattes.json == None:
            return False
        atuações_profissionais = self.lattes.json['CURRICULO-VITAE']['DADOS-GERAIS'].get(
            'ATUACOES-PROFISSIONAIS')
        if not atuações_profissionais == None:
            for lista_atuação in self.lattes.json['CURRICULO-VITAE']['DADOS-GERAIS']['ATUACOES-PROFISSIONAIS']:
                if self.verbose:
                    print(f'Lista de Vínculos: {lista_atuação}')
                for atuacao in self.get_list(self.lattes.json['CURRICULO-VITAE']['DADOS-GERAIS']['ATUACOES-PROFISSIONAIS'][lista_atuação]):
                    if not atuacao.get('VINCULOS') == None:
                        anos = []
                        instituição = None
                        num_anos = None
                        atual = False
                        enquadramento = None
                        tipo = None
                        lattes_vinculos = atuacao['VINCULOS']
                        instituição = atuacao.get('@NOME-INSTITUICAO')
                        for vinculo in self.get_list(lattes_vinculos):
                            ano_fim = None
                            ano_inicio = None
                            if self.verbose:
                                print(f'\nVículos: {vinculo}')
                            ano_fim = self.inteiro(vinculo.get('@ANO-FIM'))
                            ano_inicio = self.inteiro(
                                vinculo.get('@ANO-INICIO'))
                            if ano_fim == None and not ano_inicio == None and ano_inicio > 1800:
                                anos.extend([*range(ano_inicio, 2020, 1)])
                                atual = True
                                enquadramento = vinculo.get(
                                    '@ENQUADRAMENTO-FUNCIONAL')
                                tipo = vinculo.get('@TIPO-DE-VINCULO')
                            elif not ano_inicio == None and ano_inicio > 1800:
                                anos.extend(
                                    [*range(ano_inicio, ano_fim + 1, 1)])
                        num_anos = len(np.unique(np.array(anos)))
                        self.lattes_vinculos.append(
                            {
                                'id_lattes': self.id_lattes,
                                'instituicao': instituição,
                                'num_anos': num_anos,
                                'atual': atual,
                                'enquadramento': enquadramento,
                                'tipo': tipo,
                            }
                        )
        return True

    @timing
    def salva_vinculos_no_bd(self):
        """
        Salva vínculos profissionais no banco de dados.
        LEGADO: tabela lattes_vinculos (ver TABELAS_LEGADAS). Prefira o lattes_json.
        """
        if not self.lattes_vinculos == None and len(self.lattes_vinculos) > 0:
            self._exige_tabela_legada('lattes_vinculos')
            sql = '''
                INSERT INTO lattes_vinculos
                    (id_lattes, instituicao, num_anos, atual, enquadramento, tipo)
                VALUES
                    {params_list}
                '''
            if self.on_conflic_update:
                sql = "DELETE FROM lattes_vinculos WHERE id_lattes = '" + \
                    str(self.id_lattes) + "';\n" + sql
            data = []
            for chave in self.lattes_vinculos:
                linha = []
                for k, v in enumerate(chave):
                    linha.append(chave[v])
                data.append(linha)
            retorno = self.db.insert_many(sql, data)
            self.db.registrar_log(self.id_lattes, f'Vínculos profissionais inseridos/atualizados no BD: {retorno} registros.')
            return retorno
            

    @timing
    def get_sexo(self):
        """
        Tenta determinar o sexo do pesquisador (via dados, BD ou API externa).

        Returns:
            str: Sexo ('M' ou 'F') ou None.
        """
        if self.lattes_dados_gerais and 'sexo' in self.lattes_dados_gerais and self.lattes_dados_gerais['sexo'].lower() in ['m','f','masculino', 'feminino']:
            return self.lattes_dados_gerais['sexo']

        if self.verbose:
            print(
                f"Pegando o sexo do id_lattes {self.id_lattes} com o nome {self.lattes.lattes_dados_gerais['nome']}")

        # tabela en_recursos_humanos
        try:
            sexo = self.db.query(
                f'select cod_sexo from en_recurso_humano where id_lattes = {self.id_lattes}')
            sexo = sexo[0][0]
        except:
            sexo = None
        if self.verbose:
            print(
                'Resultado da tentativa de pegar sexo na tabela en_recurso_humano por id_lattes:', sexo)

        # Tabela lattes_dados_gerais
        if sexo == None:
            if self.verbose:
                print(
                    f"Pegando o sexo do id_lattes {self.id_lattes} com o nome {self.lattes.lattes_dados_gerais['nome']}")
            try:
                sexo = self.db.query(
                    f'select sexo from lattes_dados_gerais where id_lattes = {self.id_lattes}')
                sexo = sexo[0][0]
            except:
                sexo = None
            if self.verbose:
                print(
                    'Resultado da tentativa de pegar sexo na tabela lattes_dados_gerais:', sexo)

        if sexo == None:
            if len(self.lattes_dados_gerais['nome']) > 2:
                sql = f'''
                    select cod_sexo 
                    from en_recurso_humano 
                    where  UPPER(unaccent(split_part(nme_rh,' ',1))) = 
                            UPPER(unaccent(split_part('{self.lattes_dados_gerais['nome'].split(sep=" ", maxsplit=1)[0]}',' ',1)))
                    limit 1;
                    '''
                try:
                    sexo = self.db.query(sql)
                    sexo = sexo[0][0]
                except:
                    sexo = None
            if self.verbose:
                print(
                    'Resultado da tentativa de pegar sexo na tabela en_recurso_humano por nome:', sexo)
        if sexo == None:
            if len(self.lattes_dados_gerais['nome']) > 2:
                sexo = self.getGender(self.lattes_dados_gerais['nome'])[
                    0][0][0].upper()
                if sexo == 'E':
                    sexo = None
                if self.verbose:
                    print('Resultado da tentativa de pegar sexo no site:', sexo)
        self.lattes.lattes_dados_gerais['sexo'] = sexo
        self.lattes_dados_gerais['sexo'] = sexo
        if self.verbose:
            print('Sexo encontrado:', sexo)
        return sexo

    @timing
    def getGender(self, names=None):
        """
        Consulta a API genderize.io para inferir o gênero a partir do nome.

        Args:
            names (list/str, optional): Nome(s) para consulta.

        Returns:
            list: Lista de tuplas (genero, probabilidade, count).
        """
        if names == None:
            names = self.lattes.lattes_dados_gerais['nome']
        url = ""
        cnt = 0
        if not isinstance(names, list):
            names = [names,]

        for name in names:
            if url == "":
                url = "name[0]=" + name.split(sep=" ", maxsplit=1)[0]
            else:
                cnt += 1
                url = url + "&name[" + str(cnt) + "]=" + \
                    name.split(sep=" ", maxsplit=1)[0]
        url = "https://api.genderize.io?" + url
        if self.verbose:
            print(f'URL: {url}')
        req = requests.get(url)
        results = json.loads(req.text)

        retrn = []
        for result in results:
            if self.verbose:
                print(f'Resultado: {result}')
            try:
                if result["gender"] is not None:
                    retrn.append(
                        (result["gender"], result["probability"], result["count"]))
                else:
                    retrn.append((u'None', u'0.0', 0.0))
            except:
                retrn.append((u'erro', u'0.0', 0.0))
        return retrn

    @timing
    def atualiza(self,
                 cv_lattes=True,
                 lattes_indicadores=True,
                 lattes_palavras_chave=False,
                 lattes_areas_conhecimento=False,
                 publicações=False,
                 lattes_dados_gerais=True,
                 lattes_vinculos=False,
                 verificar_gravacao=None
                 ):
        """
        Executa o fluxo completo de extração e salvamento de dados (lattes_indicadores, palavras-chave, etc.).

        As opções lattes_palavras_chave, lattes_areas_conhecimento, publicações e lattes_vinculos gravam em tabelas
        LEGADAS (ver TABELAS_LEGADAS) e ficam desligadas por padrão. Se forem ligadas e a tabela não existir, a
        gravação lança RuntimeError (a tabela nunca é criada automaticamente).
        LLMs/agentes: priorizar o lattes_json para esses dados.

        Returns:
            bool: True se sucesso.
        """
        if verificar_gravacao != None:
            self.verificar_gravacao = verificar_gravacao
        if self.erro == True:
            if self.verbose:
                print('ERRRO - Lattes não está carregado. ID:', self.id_lattes)
            return False
        if self.lattes == None:
            if self.verbose:
                print('ERRRO - Lattes não está carregado. ID:', self.id_lattes)
            return False
        if cv_lattes:
            self.lattes.insert_json()
        if lattes_indicadores:
            if not self.get_indicadores():
                if self.verbose:
                    print('ERRRO - Não foi possível carregar Indicadores. ID:', self.id_lattes)
            else:
                self.salva_indicadores_no_bd()
                # Verifica apenas se solicitado
                if self.verificar_gravacao and self.verbose:
                    if self.verifica_indicadores_no_bd():
                        print(f"Sucesso: Indicadores verificados.")
                    else:
                        print(f"FALHA: Verificação falhou.")
        if lattes_palavras_chave:
            if not self.get_palavras_chave():
                if self.verbose:
                    print(
                        'ERRRO - Não foi possível carregar as Palavras Chaves. ID:', self.id_lattes)
            else:
                self.salva_palavras_chave_no_bd()
        if lattes_areas_conhecimento:
            if not self.get_areas_conhecimento():
                if self.verbose:
                    print(
                        'ERRRO - Não foi possível carregar Áreas do Conhecimento. ID:', self.id_lattes)
            else:
                self.salva_areas_do_conhecimento_no_bd()
        if publicações:
            if not self.get_publicações():
                if self.verbose:
                    print('ERRRO - Não foi possível carregar Publicações. ID:', self.id_lattes)
            else:
                self.salva_publicações_no_bd()
        if lattes_vinculos:
            if not self.get_vinculos():
                if self.verbose:
                    print('ERRRO - Não foi possível carregar Vínculos. ID:', self.id_lattes)
            else:
                self.salva_vinculos_no_bd()
        if lattes_dados_gerais:
            if not self.get_dados_gerais():
                if self.verbose:
                    print(
                        'ERRRO - Não foi possível carregar Dados Pessoais. ID:', self.id_lattes)
            else:
                self.salva_dados_gerais_no_bd()
                
        # Se NÃO for uma conexão externa injetada, commita aqui.
        # Se for externa, quem chamou que commita.
        if hasattr(self.db, 'is_external_connection') and not self.db.is_external_connection:
             self.db.commit()
             
        return True
