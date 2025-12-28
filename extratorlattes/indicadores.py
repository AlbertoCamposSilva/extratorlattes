import extratorlattes
from datetime import datetime
from shutil import ExecError
import pytz
import json
import time
from collections import OrderedDict
# from Lattes import Lattes
# from Database import Database
import psycopg2
import requests
import json
import pandas
from functools import wraps
import numpy as np


class Indicadores:

    def __init__(self,
                 id=None,
                 json=None,
                 filename=None,
                 show_execution_time=False,
                 show_sql=False,
                 auto_save_json_to_bd=False,
                 on_conflic_update=True,
                 verbose=False,
                 read_from_soap=True,
                 dbparams=None,
                 config_file=r'c:/Python/database.ini'
                 ):
        """
        Inicializa a classe Indicadores para extração e análise de dados do Lattes.

        Args:
            id (str, optional): ID do currículo Lattes.
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
        self.id = id
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
        self.db = extratorlattes.Database(
            show_sql=self.show_sql, 
            on_conflict_do_update=self.on_conflic_update,
            config_file=self.config_file,
            dbparams=self.dbparams)
        self.indicadores = []
        self.lista_indicadores = {}
        self.id_para_nome_map = {}
        self.lista_de_publicações = []
        self.palavras_chave = []
        self.areas_conhecimento = []
        self.areas = {
            'grande_area': [],
            'area': [],
            'sub-area': [],
            'especialidade': []
        }
        self.dados_gerais = None
        self.publicações = []
        self.verbose = verbose

        self.db = extratorlattes.Database(
            show_sql=self.show_sql,
            on_conflict_do_update=self.on_conflic_update,
            config_file=self.config_file,
            dbparams=self.dbparams)

        # Pegando o Lattes

        if not self.id == None:
            self.lattes = extratorlattes.Lattes(
                id,
                connect_to_db=True,
                auto_save_json_to_bd=self.auto_save_json_to_bd,
                verbose=self.verbose)
            if self.lattes.read_zip_from_disk():
                self.get_dados_gerais()
            elif read_from_soap and self.lattes.check_if_can_get_soap():
                if self.lattes.get_lattes():
                    self.get_dados_gerais()
                else:
                    self.db.execute(
                        f'update all_lattes set erro = true where id = {self.id}')
                    self.erro = True
        elif not filename == None:
            load = False
            if filename[:-3].lower() == "zip":
                load = self.lattes.read_zip_from_disk(filename=filename)
                if load:
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
                'Ao menos um deve ser especificado, id, json ou filename.')
        self.get_lista_indicadores()

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
        Extrai indicadores numéricos (com @ANO) do JSON do Lattes.

        Returns:
            bool: True se sucesso.
        """
        if self.lattes == None or self.lattes.json == None:
            return False
        self.indicadores = []

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
                                'id': self.id,
                                'ano': valor,
                                # 'path': path + [k], #Não usado mais
                                'tipo': self.get_num_indicador(nome_indicador.strip()),
                                'qty': 1
                            }
                            self.indicadores.append(indicador)
                            # self.lista_indicadores.append(
                            #     (self.id, valor, json.dumps(path + [k]), nome_indicador, 1))
                    else:
                        pass
            return self.indicadores

        # Reitera sobre todo o Lattes e pega qualquer indicador com ANO
        self.indicadores = procura_todos_anos(self.lattes.json)

        # Agrupa e soma os indicadores para reduzir o número de linhas
        if len(self.indicadores )> 0:
            data = pandas.DataFrame(self.indicadores)
            data = data.groupby(["id", "ano", "tipo"]).agg(
                qty=('qty', 'sum')
            ).reset_index()
            self.indicadores = json.loads(data.to_json(orient='records'))

        return True

    @timing
    def salva_indicadores_no_bd(self):
        """
        Salva os indicadores extraídos no banco de dados.

        Returns:
            int/bool: Número de registros inseridos ou True/False.
        """
        if self.indicadores == None or len(self.indicadores) == 0:
            return False
        sql = ''

        # Apgando se for para atualizar
        if self.on_conflic_update:
            sql = 'delete from indicadores where id = %s;'
            sql = self.db.cursor.mogrify(sql, (self.id,))
            self.db.execute(sql)

        # # Inserindo
        # args_str = ','.join((self.db.cur.mogrify("(%s,%s,%s,%s)", x).decode(
        #     "utf-8")) for x in self.indicadores)
        # sql = '''
        #         insert into indicadores
        #             (id, ano, tipo, qty)
        #         VALUES ''' + (args_str) + ';'
        # sql = self.db.cursor.mogrify(sql)
        # return self.db.execute(sql)

        # if self.indicadores == None or len(self.indicadores) == 0:
        #     if self.verbose:
        #         print('A lista de indicadores é inexistente ou está vazia.')
        #     return False
        # sql = ''

        # Inserindo
        if self.verbose:
            print("Inserindo Indicadores no BD.")

        chaves_inseridas = self.db.insert_list_of_dicts(
            table_name='indicadores',
            list_of_dicts=self.indicadores,
            id_columns=['id', 'ano', 'tipo'])

        if chaves_inseridas and (isinstance(chaves_inseridas, list) or isinstance(chaves_inseridas, int)) and len(chaves_inseridas) > 0:
            return len(chaves_inseridas)
        return True

    # REVER ESSA FUNÇÃO DEPOIS!
    @timing
    def verifica_indicadores_no_bd(self):
        """
        Verifica se os indicadores presentes na memória (self.indicadores)
        correspondem aos que estão no banco de dados para o ID atual.

        Retorna:
            bool: True se os indicadores na memória e no BD correspondem, False caso contrário.
        """
        if not self.id:
            if self.verbose:
                print("ID do Lattes não está definido. Não é possível verificar.")
            return False

        if not self.db or not self.db.connected:
            if self.verbose:
                print("Não há conexão com o banco de dados.")
            return False

        # 1. Pega os indicadores do BD
        sql = "SELECT ano, tipo, qty FROM indicadores WHERE id = %s ORDER BY ano, tipo"
        try:
            # O ID é char(16), então passamos como string
            indicadores_bd_raw = self.db.query(sql, [self.id])
            if indicadores_bd_raw is None:
                indicadores_bd_raw = []
        except Exception as e:
            if self.verbose:
                print(f"Erro ao consultar indicadores no BD: {e}")
            return False

        indicadores_bd = [{'ano': r[0], 'tipo': r[1], 'qty': r[2]} for r in indicadores_bd_raw]

        # 2. Prepara os indicadores da memória para comparação
        # Remove a chave 'id' e ordena da mesma forma que a query do BD
        indicadores_memoria = sorted(
            [{'ano': ind['ano'], 'tipo': ind['tipo'], 'qty': ind['qty']} for ind in self.indicadores],
            key=lambda x: (x['ano'], x['tipo'])
        )
        if self.verbose:
            print(f"Indicadores na memória: {len(indicadores_memoria)}")
            print(f"Indicadores no BD: {len(indicadores_bd)}")

        # 3. Compara as duas listas de dicionários
        if indicadores_memoria == indicadores_bd:
            if self.verbose:
                if indicadores_memoria:
                    print(f"Verificação bem-sucedida: {len(indicadores_memoria)} indicadores correspondem entre memória e BD.")
                else:
                    print("Verificação bem-sucedida: Nenhum indicador na memória e nenhum no BD para este ID.")
            return True
        else:
            if self.verbose:
                print("Verificação falhou: os dados dos indicadores não correspondem.")
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
        Carrega a lista de tipos de indicadores do banco de dados.

        Returns:
            dict: Mapa de nome do indicador para ID.
        """
        sql = 'SELECT id, nome_indicador from lista_indicadores'
        if self.show_sql:
            print(f'SQL: {sql}')
        result = self.db.query(sql)

        # Limpa os dicionários para garantir que não haja dados antigos ao recarregar
        self.lista_indicadores.clear()
        self.id_para_nome_map.clear()

        for num_id, nome_indicador in result:
            self.lista_indicadores[nome_indicador] = num_id
            self.id_para_nome_map[num_id] = nome_indicador
        if self.verbose:
            print("SQL executado.")
        return self.lista_indicadores

    #@timing
    def get_num_indicador(self, nome_indicador):
        """
        Obtém o ID numérico de um tipo de indicador, criando-o se não existir.

        Args:
            nome_indicador (str): Nome do indicador.

        Returns:
            int: ID do indicador.
        """
        if nome_indicador not in self.lista_indicadores:
            if not self.salva_no_bd_se_inexistente:
                return None
            sql = 'INSERT INTO lista_indicadores (nome_indicador) VALUES (%s) RETURNING id'
            new_id = self.db.query(sql, (nome_indicador,), many=False)[0]
            self.db.commit()
            self.lista_indicadores[nome_indicador] = new_id
            self.id_para_nome_map[new_id] = nome_indicador
        return self.lista_indicadores[nome_indicador]


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
        Retorna a lista de indicadores extraídos enriquecida com os nomes dos tipos.

        Returns:
            list: Lista de dicionários de indicadores.
        """
        indicadores_com_nomes = []
        if self.lattes == None or self.lattes.json == None:
            return False
        if len(self.indicadores) == 0:
            self.get_indicadores()
        for indicador in self.indicadores:
            indicador['descricao'] = self.get_nome_indicador(indicador['tipo'])
            indicadores_com_nomes.append(indicador)
        return indicadores_com_nomes

    @timing
    def get_palavras_chave(self, json=None, path=None, palavras_chave=None):
        """
        Extrai palavras-chave do JSON do Lattes.

        Returns:
            bool: True se sucesso.
        """
        if palavras_chave == None:
            palavras_chave = self.palavras_chave
        if json == None:
            json = self.lattes.json
        if json == None:
            return False

        def procura_palavras_chave(json, path, palavras_chave):
            if not path:
                path = []
            for k, v in json.items():
                if isinstance(v, dict) or isinstance(v, OrderedDict):
                    procura_palavras_chave(
                        json=v, path=path + [k], palavras_chave=palavras_chave)
                elif isinstance(v, list):
                    num = 0
                    for item in v:
                        procura_palavras_chave(
                            json=item, path=path + [k] + [num], palavras_chave=palavras_chave)
                        num += 1
                else:
                    if not v == None:
                        if k[0:15] == '@PALAVRA-CHAVE-':
                            if (len(v) > 2):
                                if not v in palavras_chave:
                                    palavras_chave.append(v)
                    else:
                        pass
            return palavras_chave

        self.palavras_chave = procura_palavras_chave(
            json=json, path=path, palavras_chave=palavras_chave)
        return True

    @timing
    def salva_palavras_chave_no_bd(self):
        """
        Salva as palavras-chave extraídas no banco de dados.
        """
        if self.palavras_chave == None or len(self.palavras_chave) == 0:
            if self.verbose:
                print('A lista de palavras chaves é inexistente ou está vazia.')
            return False
        if self.on_conflic_update:
            sql = "DELETE from palavras_chave WHERE id = %s;\n"
            sql = self.db.cursor.mogrify(sql, (self.id,)).decode("utf-8")
        else:
            sql = ''
        args_str = ','.join((self.db.cursor.mogrify(
            "(%s,%s)", (str(self.id), x)).decode("utf-8")) for x in self.palavras_chave)
        sql += '''INSERT into palavras_chave (id, palavra)
        VALUES
            ''' + args_str
        if self.show_sql:
            print(f'SQL a ser executado: {sql}')
        return self.db.execute(sql)

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
                                self.areas_conhecimento.append((
                                    self.id,
                                    "grande-area",
                                    area2['@NOME-GRANDE-AREA-DO-CONHECIMENTO'])
                                )
                                self.areas['grande_area'].append(
                                    area2['@NOME-GRANDE-AREA-DO-CONHECIMENTO'])
                            if area2.get('@NOME-DA-AREA-DO-CONHECIMENTO') and len(area2['@NOME-DA-AREA-DO-CONHECIMENTO']) > 1:
                                self.areas_conhecimento.append((
                                    self.id,
                                    "area",
                                    area2['@NOME-DA-AREA-DO-CONHECIMENTO']))
                                self.areas['area'].append(
                                    area2['@NOME-DA-AREA-DO-CONHECIMENTO'])
                            if area2.get('@NOME-DA-SUB-AREA-DO-CONHECIMENTO') and len(area2['@NOME-DA-SUB-AREA-DO-CONHECIMENTO']) > 1:
                                self.areas_conhecimento.append((
                                    self.id,
                                    "sub-area",
                                    area2['@NOME-DA-SUB-AREA-DO-CONHECIMENTO']))
                                self.areas['sub-area'].append(
                                    area2['@NOME-DA-SUB-AREA-DO-CONHECIMENTO'])
                            if area2.get('@NOME-DA-ESPECIALIDADE') and len(area2['@NOME-DA-ESPECIALIDADE']) > 1:
                                self.areas_conhecimento.append((
                                    self.id,
                                    "especialidade",
                                    area2['@NOME-DA-ESPECIALIDADE']))
                                self.areas['especialidade'].append(
                                    area2['@NOME-DA-ESPECIALIDADE'])
        return True

    @timing
    def salva_areas_do_conhecimento_no_bd(self):
        """
        Salva as áreas do conhecimento no banco de dados.
        """
        if self.areas_conhecimento == None or len(self.areas_conhecimento) == 0:
            return False
        sql = ''
        if self.on_conflic_update:
            sql += self.db.cursor.mogrify(
                'delete from areas_conhecimento where id = %s;', (self.id,)).decode("utf-8")
        args_str = ','.join((self.db.cursor.mogrify("(%s,%s,%s)", x).decode(
            "utf-8")) for x in self.areas_conhecimento)
        sql = '''
        insert into areas_conhecimento (id, tipo, area) VALUES \n''' + args_str + '''
        ON CONFLICT (id, tipo, area) DO NOTHING'''
        sql = self.db.cursor.mogrify(sql)
        if self.show_sql:
            print(f'SQL a ser executado: {sql}')
        self.db.execute(sql)
        return True

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
                                    'id': self.id,
                                    'DOI': v,
                                    'path': path + [k],
                                    'tipo': nome_indicador,
                                    'titulo': titulo,
                                    'ano': ano,
                                    'natureza': natureza,
                                }
                                self.publicações.append(publicação)
                                self.lista_de_publicações.append((self.id, v, json.dumps(
                                    path + [k]), nome_indicador, titulo, ano, natureza))
                    else:
                        pass

        procura_todos_DOIs(self.lattes.json)
        return True

    @timing
    def salva_publicações_no_bd(self):
        """
        Salva as publicações extraídas no banco de dados.
        """
        if self.lista_de_publicações == None or len(self.lista_de_publicações) == 0:
            return False
        sql = ''
        args_str = ','.join((self.db.cursor.mogrify("\n(%s, %s, %s, %s, %s, %s, %s)", x).decode(
            "utf-8")) for x in self.lista_de_publicações)
        if self.verbose:
            print(f'Argumentos: {args_str}')
        if self.on_conflic_update:
            delete_sql = '''DELETE from publicacoes WHERE id = %s;\n'''
            sql += self.db.cursor.mogrify(delete_sql,
                                          (self.id,)).decode("utf-8")
        sql += '''INSERT INTO publicacoes (id, doi, path, tipo, titulo, ano, natureza)
        VALUES ''' + args_str + ''';
        '''
        if self.show_sql:
            print(f'SQL a ser executado: {sql}')

        return self.db.execute(sql)

    @timing
    def get_dados_gerais(self):
        """
        Extrai dados gerais do Lattes.

        Returns:
            bool: True se sucesso.
        """
        self.lattes.get_dados_gerais()
        self.dados_gerais = self.lattes.dados_gerais
        #self.get_sexo()
        return True

    @timing
    def salva_dados_gerais_no_bd(self):
        """
        Salva dados gerais no banco de dados.
        """
        if self.dados_gerais == None or len(self.dados_gerais) == 0:
            return False
        return self.db.insert_dict('dados_gerais', self.dados_gerais, on_conflict=['id'])

    @timing
    def get_vinculos(self):
        """
        Extrai vínculos profissionais do JSON do Lattes.

        Returns:
            bool: True se sucesso.
        """
        self.vinculos = []
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
                        vinculos = atuacao['VINCULOS']
                        instituição = atuacao.get('@NOME-INSTITUICAO')
                        for vinculo in self.get_list(vinculos):
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
                        self.vinculos.append(
                            {
                                'id': self.id,
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
        """
        if not self.vinculos == None and len(self.vinculos) > 0:
            sql = '''
                INSERT INTO vinculos
                    (id, instituicao, num_anos, atual, enquadramento, tipo)
                VALUES
                    {params_list}
                '''
            if self.on_conflic_update:
                sql = "DELETE FROM vinculos WHERE id = '" + \
                    str(self.id) + "';\n" + sql
            data = []
            for chave in self.vinculos:
                linha = []
                for k, v in enumerate(chave):
                    linha.append(chave[v])
                data.append(linha)
            self.db.insert_many(sql, data)

    @timing
    def get_sexo(self):
        """
        Tenta determinar o sexo do pesquisador (via dados, BD ou API externa).

        Returns:
            str: Sexo ('M' ou 'F') ou None.
        """
        if self.dados_gerais and 'sexo' in self.dados_gerais and self.dados_gerais['sexo'].lower() in ['m','f','masculino', 'feminino']:
            return self.dados_gerais['sexo']

        if self.verbose:
            print(
                f"Pegando o sexo do id {self.id} com o nome {self.lattes.dados_gerais['nome']}")

        # tabela en_recursos_humanos
        try:
            sexo = self.db.query(
                f'select cod_sexo from en_recurso_humano where id = {self.id}')
            sexo = sexo[0][0]
        except:
            sexo = None
        if self.verbose:
            print(
                'Resultado da tentativa de pegar sexo na tabela en_recurso_humano por id:', sexo)

        # Tabela dados_gerais
        if sexo == None:
            if self.verbose:
                print(
                    f"Pegando o sexo do id {self.id} com o nome {self.lattes.dados_gerais['nome']}")
            try:
                sexo = self.db.query(
                    f'select sexo from dados_gerais where id = {self.id}')
                sexo = sexo[0][0]
            except:
                sexo = None
            if self.verbose:
                print(
                    'Resultado da tentativa de pegar sexo na tabela dados_gerais:', sexo)

        if sexo == None:
            if len(self.dados_gerais['nome']) > 2:
                sql = f'''
                    select cod_sexo 
                    from en_recurso_humano 
                    where  UPPER(unaccent(split_part(nme_rh,' ',1))) = 
                            UPPER(unaccent(split_part('{self.dados_gerais['nome'].split(sep=" ", maxsplit=1)[0]}',' ',1)))
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
            if len(self.dados_gerais['nome']) > 2:
                sexo = self.getGender(self.dados_gerais['nome'])[
                    0][0][0].upper()
                if sexo == 'E':
                    sexo = None
                if self.verbose:
                    print('Resultado da tentativa de pegar sexo no site:', sexo)
        self.lattes.dados_gerais['sexo'] = sexo
        self.dados_gerais['sexo'] = sexo
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
            names = self.lattes.dados_gerais['nome']
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
                retrn.append((u'Erro', u'0.0', 0.0))
        return retrn

    @timing
    def atualiza(self,
                 indicadores=True,
                 palavras_chave=True,
                 areas_conhecimento=True,
                 publicações=True,
                 dados_gerais=True,
                 vinculos=True,
                 ):
        """
        Executa o fluxo completo de extração e salvamento de dados (indicadores, palavras-chave, etc.).

        Returns:
            bool: True se sucesso.
        """
        if self.erro == True:
            if self.verbose:
                print('ERRRO - Lattes não está carregado. ID:', self.id)
            return False
        if self.lattes == None:
            if self.verbose:
                print('ERRRO - Lattes não está carregado. ID:', self.id)
            return False
        if indicadores:
            if not self.get_indicadores():
                if self.verbose:
                    print('ERRRO - Não foi possível carregar Indicadores. ID:', self.id)
            else:
                self.salva_indicadores_no_bd()
                if self.verbose:
                    if self.verifica_indicadores_no_bd():
                        print(f"Sucesso: Indicadores para o ID {self.id} foram salvos e verificados no BD.")
                    else:
                        print(f"FALHA: Verificação falhou após salvar indicadores para o ID {self.id}.")
        if palavras_chave:
            if not self.get_palavras_chave():
                if self.verbose:
                    print(
                        'ERRRO - Não foi possível carregar as Palavras Chaves. ID:', self.id)
            else:
                self.salva_palavras_chave_no_bd()
        if areas_conhecimento:
            if not self.get_areas_conhecimento():
                if self.verbose:
                    print(
                        'ERRRO - Não foi possível carregar Áreas do Conhecimento. ID:', self.id)
            else:
                self.salva_areas_do_conhecimento_no_bd()
        if publicações:
            if not self.get_publicações():
                if self.verbose:
                    print('ERRRO - Não foi possível carregar Publicações. ID:', self.id)
            else:
                self.salva_publicações_no_bd()
        if vinculos:
            if not self.get_vinculos():
                if self.verbose:
                    print('ERRRO - Não foi possível carregar Vínculos. ID:', self.id)
            else:
                self.salva_vinculos_no_bd()
        if dados_gerais:
            if not self.get_dados_gerais():
                if self.verbose:
                    print(
                        'ERRRO - Não foi possível carregar Dados Pessoais. ID:', self.id)
            else:
                self.salva_dados_gerais_no_bd()
        self.db.commit()
        return True
