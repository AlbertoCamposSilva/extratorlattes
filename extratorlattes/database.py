import psycopg2
from configparser import ConfigParser
import pandas as pd
import io
import time
import logging

# Fazendo psycopg2 conseguir trabalhar com NP
# https://stackoverflow.com/questions/39564755/programmingerror-psycopg2-programmingerror-cant-adapt-type-numpy-ndarray
import numpy as np
from psycopg2.extensions import register_adapter, AsIs

# Configuração básica do logger
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger('Database')


def addapt_numpy_float64(numpy_float64):
    return AsIs(numpy_float64)
def addapt_numpy_int64(numpy_int64):
    return AsIs(numpy_int64)
def addapt_numpy_float32(numpy_float32):
    return AsIs(numpy_float32)
def addapt_numpy_int32(numpy_int32):
    return AsIs(numpy_int32)
def addapt_numpy_array(numpy_array):
    return AsIs(tuple(numpy_array))


register_adapter(np.float64, addapt_numpy_float64)
register_adapter(np.int64, addapt_numpy_int64)
register_adapter(np.float32, addapt_numpy_float32)
register_adapter(np.int32, addapt_numpy_int32)
register_adapter(np.ndarray, addapt_numpy_array)


class Database:
    def __init__(self, 
                 show_sql=False, 
                 on_conflict_do_update=True, 
                 config_file=r'e:/Python/database.ini', 
                 dbparams=None, 
                 connection=None, 
                 autocommit=True):
        """
        Inicializa a conexão com o banco de dados e define um atributo de status da conexão.
        """
        # 1. Inicializa atributos relacionados à conexão com um estado padrão "desconectado".
        self.conn = None
        self.cur = None
        self.connected = False
        self.config_file = config_file
        self.on_conflict_do_update = on_conflict_do_update
        self.show_sql = show_sql
        self.autocommit = autocommit
        self.is_external_connection = False

        try:
            if connection:
                self.conn = connection
                self.cur = self.conn.cursor()
                self.connected = True
                self.is_external_connection = True
            else:
                self.params = self.__class__.config_db_connection(config_file=self.config_file, dbparams=dbparams)
                self.conn = psycopg2.connect(**self.params)
                self.cur = self.conn.cursor()
                self.connected = True
                self.is_external_connection = False
                self.conn.rollback() # Limpa estado inicial

        except (Exception, psycopg2.Error) as error:
            print(f"Falha na conexão com o banco de dados: {error}")

    @staticmethod
    def db_engine(config_file=r'e:/Python/database.ini', dbparams=None):
        """
        Cria a string de conexão para o SQLAlchemy engine.

        Args:
            config_file (str): Caminho do arquivo de configuração.
            dbparams (dict, optional): Dicionário com parâmetros de conexão.

        Returns:
            str: String de conexão PostgreSQL.
        """
        if dbparams is None:
            params = Database.config_db_connection(config_file = config_file)
        elif isinstance(dbparams, dict):
            params = dbparams
        else:
            raise Exception('Arquivo de configuração não encontrado e dbparams deve ser um dicionário ou None')
        username = params['user']
        password = params['password']
        ipaddress = params['host']
        port = int(params['port'])
        dbname = params['database']
        return f'postgresql://{username}:{password}@{ipaddress}:{port}/{dbname}'

    @staticmethod
    def engine(config_file=r'e:/Python/database.ini'):
        """
        Retorna a string de conexão SQLAlchemy (alias para db_engine).
        """
        return Database.db_engine(config_file=config_file)

    @staticmethod
    def config_db_connection(config_file=r'e:/Python/database.ini', section='postgresql', dbparams=None):
        """
        Lê a configuração do banco de dados de um arquivo INI ou dicionário.
        """
        if dbparams is not None:
            if isinstance(dbparams, dict):
                return dbparams
        parser = ConfigParser()
        parser.read(config_file, 'UTF-8')
        db = {}
        if parser.has_section(section):
            params = parser.items(section)
            for param in params:
                db[param[0]] = param[1]
        else:
            raise Exception(
                'Section {0} not found in the {1} file'.format(section, config_file))
        return db

    def __enter__(self):
        self.open()  # Certifica-se de que a conexão foi estabelecida
        return self
    
    def __exit__(self, exc_type, exc_val, exc_tb):
            if exc_type is not None:
                self.rollback()
            else:
                if self.autocommit:
                    self.commit()
                
                # Se a conexão veio de fora (pool), não fechamos aqui.
                if not self.is_external_connection:
                    self.close(commit=self.autocommit)

    @property
    def connection(self):
        return self.conn

    @property
    def cursor(self):
        return self.cur

    def open(self):
        """
        Abre a conexão com o banco de dados se estiver fechada.
        """
        if not self.connected:
            if self.conn is None or self.conn.closed:
                self.params = self.__class__.config_db_connection(config_file=self.config_file)
                self.conn = psycopg2.connect(**self.params)
            self.cur = self.conn.cursor()
            self.connected = True
        return self.connection, self.cursor
    
    def commit(self):
        """
        Realiza commit na transação atual.
        """
        self.connection.commit()

    def rollback(self):
        """
        Realiza rollback na transação atual.
        """
        if getattr(self, 'connection', None) is not None:
            if not self.connection.closed:
                self.connection.rollback()

    def close(self, commit=True):
        # Se é conexão externa, apenas limpamos a referência interna
        if self.is_external_connection:
            self.connected = False
            self.conn = None
            self.cur = None
            return

        if hasattr(self, 'connection') and not self.connection.closed:
            if commit:
                self.commit()
            self.cursor.close()
            self.connection.close()
        self.connected = False

    def execute(self, sql, params=None):
        if not self.connected:
            raise psycopg2.InterfaceError("A conexão com o banco de dados não está aberta.")
        if params is not None and isinstance(params, (str, int, float)):
            params = [params]
        sql_to_execute = self.cursor.mogrify(sql, params or None)
        if self.show_sql:
            print(sql_to_execute.decode())
        try:
            self.cursor.execute(sql_to_execute)
            resultado = self.cursor.rowcount
            if self.autocommit:
                self.commit()
            return resultado
                
        except Exception as error:
            self.rollback()
            logger.error(f"Erro na execução do SQL: {error} | SQL: {sql_to_execute.decode()[:500]}")
            raise error

    def fetchall(self):
        """
        Retorna todas as linhas do último resultado.
        """
        return self.cursor.fetchall()

    def fetchone(self):
        """
        Retorna a próxima linha do último resultado.
        """
        return self.cursor.fetchone()

            
    def registrar_log(self, id_lattes, status):
            """
            Insere um registro de log com o ID do Lattes, o Timestamp atual e o status.
            """
            sql = """
                INSERT INTO log_processamento_lattes (id_lattes, data_hora, status) 
                VALUES (%s, NOW(), %s)
            """
            # Converte para string para garantir compatibilidade com text
            return self.execute(sql, (str(id_lattes), status,))    

    def query(self, sql, params=None, many=True):
        """
        Executa uma consulta SQL e retorna os resultados.

        Args:
            sql (str): Consulta SQL.
            params (list/tuple, optional): Parâmetros.
            many (bool): Se True, retorna fetchall(), senão fetchone().

        Returns:
            tuple: (linhas, nomes_colunas).
        """
        if not self.connected:
            raise psycopg2.InterfaceError("Database connection is not open.")
        if params is not None and isinstance(params, (str, int, float)):
            params = [params]
        rows = None
        sql_to_execute = self.cursor.mogrify(sql, params or None)
        if self.show_sql:
            print(sql_to_execute.decode())
        try:
            self.cursor.execute(sql_to_execute)
            if many:
                rows = self.fetchall()
            else:
                rows = self.fetchone()
            colnames = [desc[0] for desc in self.cursor.description]
            return rows, colnames
        except Exception as error:
            self.rollback()
            logger.error(f"Erro na consulta SQL: {error}")
            raise error # Garante que o motor de seleção pare ou trate a falha

    def insert_many(self, sql, params_list = None, params=None):
        """
        Insere múltiplos registros de uma vez.
        """
        if params_list is not None and len(params_list) > 0:
            params_list_string = '(' + ','.join(['%s'] * len(params_list[0])) + ')'
            args_str = ','.join(
                (self.cur.mogrify(params_list_string, x).decode("utf-8")) for x in params_list)
            sql = sql.replace('{params_list}', args_str)
        if self.show_sql:
            print(sql)
        self.execute(sql, params)

    def insert_list_of_dicts(self, table_name, list_of_dicts, id_columns):
        '''
Insere uma lista contendo dicionários na tabela.
    Exemplo de uso:
        db.insert_list_of_dicts (table_name = 'indicadores',
            list_of_dicts = ind.indicadores, 
            id_columns = ['id', 'ano', 'tipo'])
Os parâmetros são:
table_name: nome da tebela no banco de dados. Exemplo: 
    indicadores
list_of_dicts: Uma lista de dicionários a serem inseridos. Exemplo: 
    ind.indicadores
id_columns: Uma lista indicando quais colunas são índices. 
Mesmo se houver apenas um índice, deve ser uma lista. 
    Exemplo: 
        ['id', 'ano', 'tipo']
        ['id']
        []

Não esquecer que show_sql e on_conflic_update pode ser alterado.
    Por exemplo: 
        db = lt.Database('cnpq')
        db.show_sql=True
        bd.on_conflic_update = False
    Ou:
        db = lt.Database('cnpq', db.show_sql=True, bd.on_conflic_update = False)

    show_sql: se True, mostrará os SQL gerados por motivo de Debug
        Padrão é False
    on_conflict_do_update: se houver conflito de identidade, se haverá atualização ou não. 
        Padrão é True.

O retorno é uma lista contendo as chaves inseridas. 
    Útil para:
        len(retorno) dá a quantidade de linhas inseridas
        retorno para saber um novo id criado numa columa serial (que incrementa automaticamente)

        '''
        if not list_of_dicts:
            return []

        keys = list_of_dicts[0].keys()
        not_keys = []
        for key in keys:
            if not key in id_columns:
                not_keys.append(key)

        on_conflict = ''
        if len(id_columns) > 0:
            on_conflict_keys = ', '.join(id_columns)
            on_conflict = f'ON CONFLICT ({on_conflict_keys}) DO UPDATE SET'
            if not self.on_conflict_do_update:
                on_conflict = ' DO NOTHING '
            else:
                x = 0
                for not_key in not_keys:
                    x += 1
                    if x == len(not_keys):
                        on_conflict += f' {not_key} = EXCLUDED.{not_key} '
                    else:
                        on_conflict += f' {not_key} = EXCLUDED.{not_key}, '

        sql = "INSERT INTO {} ({}) VALUES".format(
            table_name,
            ', '.join(
                keys),
        )
        sql += ' {params_list} '
        sql += "{} RETURNING {}".format(
            on_conflict,
            ', '.join(id_columns)
        )

        data = [tuple(v.values()) for v in list_of_dicts]
        params_list_string = '(' + ','.join(['%s'] * len(data[0])) + ')'
        args_str = ','.join(
            (self.cur.mogrify(params_list_string, x).decode("utf-8")) for x in data)
        sql = sql.replace('{params_list}', args_str)

        if self.show_sql:
            print(sql)
        if len(id_columns) > 0:
            return self.query(sql, params=None, many=True)
        else:
            return self.execute(sql)



    def insert_dict(self, column_name, dict, on_conflict=[], on_conflict_do_nothing=False):
        '''Constrói um SQL a partir de um dicionário, com a opção de atualiar em caso de conflito.

Argumentos:

column: Nome da coluna a ser atualizada;
dict: O dicionário. Nome das chaves devo coincidir com o nome das colunas.
on_conflict: Uma lista com o nome das chaves primárias da tabela. 
on_conflict_do_nothing: Se False -> quando houver conflito nas chaves acima mencionadas, atualizará a tabela.
    Se true -> quandou houver conflito, não atualizará a tabela (DO NOTHING)

        '''
        sql = 'insert into ' + column_name
        # sql = self.cursor.mogrify(sql, (column_name,)).decode("utf-8")
        sql += '(%s) values %s'
        columns = dict.keys()
        if len(on_conflict) > 0:
            sql += f"\nON CONFLICT ({','.join(on_conflict)}) DO "
            if on_conflict_do_nothing:
                sql += 'NOTHING'
            else:
                sql += "UPDATE SET\n"
                for column in columns:
                    sql += (f'\n{column} = EXCLUDED.{column},')
                sql = sql[:-1]+';'
        values = [dict[column] for column in columns]
        new_sql = self.cursor.mogrify(
            sql, (AsIs(','.join(columns)), tuple(values)))
        if self.show_sql:
            print(new_sql)
        return self.execute(new_sql)

    def constroi_tabelas(self, drop_if_exists=False):
        """
        Cria as tabelas necessárias no banco de dados.

        Args:
            drop_if_exists (bool): Se True, apaga tabelas existentes antes de criar.
        """
        drop = ''
        if drop_if_exists:
            drop = '''
DROP TABLE IF EXISTS indicadores;
DROP TABLE IF EXISTS indicadores_nomes;
DROP TABLE IF EXISTS all_lattes;
drop table IF EXISTS lista_indicadores;
DROP TABLE IF EXISTS palavras_chave;
DROP TABLE IF EXISTS areas_conhecimento;
DROP TABLE IF EXISTS publicacoes;
DROP TABLE IF EXISTS vinculos;
DROP TABLE IF EXISTS dados_gerais;
'''

        sql = f'''
        -- Table: indicadores

        {drop}

        CREATE TABLE IF NOT EXISTS indicadores
        (
            id text NOT NULL,
            ano smallint NOT NULL,
            tipo smallint NOT NULL,
            qty smallint NOT NULL,
            CONSTRAINT indicadores_pkey PRIMARY KEY (id, ano, tipo)
        )

        TABLESPACE pg_default;

        -- Table: indicadores_nomes

        CREATE TABLE IF NOT EXISTS indicadores_nomes
        (
            tipo text COLLATE pg_catalog."default" NOT NULL,
            nome text COLLATE pg_catalog."default" NOT NULL,
            grupo text COLLATE pg_catalog."default",
            path json,
            CONSTRAINT indicadores_nomes_pkey PRIMARY KEY (tipo)
        )

        TABLESPACE pg_default;

        -- Table: all_lattes

        CREATE TABLE IF NOT EXISTS all_lattes
        (
            id text NOT NULL,
            sgl_pais text COLLATE pg_catalog."default",
            dt_atualizacao date,
            cod_area integer,
            cod_nivel integer,
            dta_carga date,
            erro varcha(15),
            CONSTRAINT all_lattes_pkey PRIMARY KEY (id)
        )

        TABLESPACE pg_default;


        -- DROP INDEX IF EXISTS all_lattes_dt_atualizacao;

        CREATE INDEX IF NOT EXISTS all_lattes_dt_atualizacao
            ON all_lattes USING btree
            (dt_atualizacao ASC NULLS LAST)
            INCLUDE(id)
            TABLESPACE pg_default;
        -- Index: all_lattes_id

        -- DROP INDEX IF EXISTS all_lattes_id;

        CREATE INDEX IF NOT EXISTS all_lattes_id
            ON all_lattes USING btree
            (dt_atualizacao ASC NULLS LAST)
            INCLUDE(id)
            TABLESPACE pg_default;

        CREATE TABLE IF NOT EXISTS lista_indicadores
        (
            nome_indicador varchar not null
                constraint lista_indicadores_pk
                    unique,
            id             integer generated always as identity
        );

        CREATE TABLE IF NOT EXISTS palavras_chave
        (
            id      text,
            palavra varchar
        );

        CREATE TABLE IF NOT EXISTS areas_conhecimento
        (
            id   text not null,
            tipo varchar,
            area varchar
        );

        CREATE TABLE IF NOT EXISTS log_processamento_lattes (
            id_lattes BIGINT NOT NULL,
            data_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
            status TEXT NOT NULL,
            PRIMARY KEY (id_lattes, data_hora)
        );

        alter table areas_conhecimento
            add constraint areas_conhecimento_pk
                unique (id, tipo, area);

        CREATE TABLE IF NOT EXISTS publicacoes
        (
            id       text not null,
            tipo     varchar,
            titulo   varchar,
            doi      varchar,
            path     varchar,
            ano      integer,
            natureza varchar
        );

        CREATE TABLE IF NOT EXISTS vinculos
        (
            id            text not null,
            instituicao   varchar,
            num_anos      integer,
            atual         boolean,
            enquadramento varchar,
            tipo          varchar
        );

        CREATE TABLE IF NOT EXISTS dados_gerais (
            id text PRIMARY KEY,
            data_atualizacao TIMESTAMP,
            nome_completo VARCHAR,
            nomes_citacao VARCHAR,
            nacionalidade VARCHAR,
            cpf VARCHAR,
            pais_nascimento VARCHAR,
            uf_nascimento VARCHAR,
            cidade_nascimento VARCHAR,
            data_nascimento DATE,
            sexo VARCHAR,
            numero_identidade VARCHAR,
            orgao_emissor_identidade VARCHAR,
            uf_orgao_emissor_identidade VARCHAR,
            data_emissao_identidade DATE,
            numero_passaporte VARCHAR,
            nome_pai VARCHAR,
            nome_mae VARCHAR,
            permissao_divulgacao BOOLEAN,
            data_falecimento DATE,
            raca_cor VARCHAR,
            resumo_cv_rh VARCHAR,
            resumo_cv_rh_en VARCHAR,
            outras_informacoes_relevantes VARCHAR,
            email VARCHAR,
            sigla_pais_nacionalidade VARCHAR,
            pais_nacionalidade VARCHAR,
            orcid VARCHAR,
            pcd BOOLEAN
        );
            
        COMMENT ON TABLE dados_gerais IS 'Armazena os dados gerais e cadastrais dos currículos Lattes.';
        COMMENT ON COLUMN dados_gerais.id IS 'Identificador único do Lattes (16 caracteres), chave primária.';
        COMMENT ON COLUMN dados_gerais.data_atualizacao IS 'Data e hora da última atualização do currículo na plataforma Lattes.';
        COMMENT ON COLUMN dados_gerais.nome_completo IS 'Nome completo do titular do currículo.';
        COMMENT ON COLUMN dados_gerais.nomes_citacao IS 'Nomes utilizados em citações bibliográficas, separados por ponto e vírgula.';
        COMMENT ON COLUMN dados_gerais.cpf IS 'Cadastro de Pessoas Físicas (CPF) do titular.';
        COMMENT ON COLUMN dados_gerais.pcd IS 'Indica se a pessoa tem alguma deficiência (Pessoa com Deficiência).';

        create table lattes_xml
            (
                id  text not null
                    primary key,
                xml xml      not null
            );



            CREATE EXTENSION IF NOT EXISTS pg_trgm;

        CREATE INDEX idx_lattes_identificacao
        ON lattes_json
        USING GIN (
            (json -> 'CURRICULO-VITAE' -> 'DADOS-GERAIS' ->> '@NOME-COMPLETO') gin_trgm_ops,
            (json -> 'CURRICULO-VITAE' -> 'DADOS-GERAIS' ->> '@CPF') gin_trgm_ops,
            (json -> 'CURRICULO-VITAE' -> 'DADOS-GERAIS' ->> '@SEXO') gin_trgm_ops,
            (json -> 'CURRICULO-VITAE' -> 'DADOS-GERAIS' ->> '@RACA-OU-COR') gin_trgm_ops,
            (json -> 'CURRICULO-VITAE' -> 'DADOS-GERAIS' ->> '@ORCID-ID') gin_trgm_ops,
            (json -> 'CURRICULO-VITAE' -> 'DADOS-GERAIS' -> 'ENDERECO' -> 'ENDERECO-PROFISSIONAL' ->> '@UF') gin_trgm_ops
        );



            '''
        return self.execute(sql)

    def check_if_table_exists(self, table_name):
        """
        Verifica se uma tabela existe no esquema public.
        """
        sql = f'''
            SELECT EXISTS (
            SELECT FROM information_schema.tables 
            WHERE  table_schema = 'public'
            AND    table_name   = '{table_name}'
            );
            '''
        return self.query(sql)[0][0]


    def upsert_dataframe(self, df: pd.DataFrame, table_name: str, primary_key_col: str, verbose: bool = True):
        """
        Realiza um 'upsert' (insert/update) utilizando a própria instância da classe Database.
        """
        if not isinstance(df, pd.DataFrame):
            raise TypeError("O argumento 'df' deve ser um DataFrame do pandas.")

        if primary_key_col not in df.columns:
            raise ValueError(f"A coluna de chave primária '{primary_key_col}' não foi encontrada no DataFrame.")
            
        # Remoção de duplicatas no DataFrame
        df_cleaned = df.drop_duplicates(subset=[primary_key_col], keep='last')
        if verbose and len(df_cleaned) < len(df):
            print(f"Aviso: {len(df) - len(df_cleaned)} linhas duplicadas foram removidas do DataFrame de entrada.")
        
        # Nomes das colunas formatados para SQL
        cols = [f'"{c}"' for c in df_cleaned.columns]
        cols_str = ", ".join(cols)
        
        # Cláusula UPDATE SET, excluindo a chave primária
        update_cols_str = ", ".join([f'{c}=EXCLUDED.{c}' for c in cols if c.replace('"', '') != primary_key_col])

        # Nome único para tabela temporária
        temp_table_name = f"temp_{table_name}_{int(time.time())}"

        try:
            # Garante que a conexão está aberta
            if not self.connected:
                self.open()

            # 1. Criar tabela temporária
            create_temp_sql = f'CREATE TEMP TABLE {temp_table_name} (LIKE {table_name} INCLUDING DEFAULTS);'
            result_create = self.execute(create_temp_sql)
            
            if isinstance(result_create, Exception):
                raise result_create

            if verbose:
                print(f"Tabela temporária '{temp_table_name}' criada.")

            # 2. Usar COPY EXPERT para carga em massa
            buffer = io.StringIO()
            df_cleaned.to_csv(buffer, index=False, header=False, sep='\t', na_rep='\\N')
            buffer.seek(0)
            
            sql_copy = f"COPY {temp_table_name} ({cols_str}) FROM STDIN WITH (FORMAT CSV, DELIMITER E'\\t', NULL '\\N')"
            self.cur.copy_expert(sql_copy, buffer)
            
            if verbose:
                print(f"{len(df_cleaned)} linhas carregadas na tabela temporária.")

            # 3. Mesclar dados (Upsert)
            merge_sql = f"""
                INSERT INTO {table_name} ({cols_str})
                SELECT {cols_str} FROM {temp_table_name}
                ON CONFLICT ("{primary_key_col}") DO UPDATE SET
                {update_cols_str};
            """
            
            result_merge = self.execute(merge_sql)
            
            if isinstance(result_merge, Exception):
                raise result_merge

            if verbose:
                print(f"Sucesso: {len(df_cleaned)} linhas processadas na tabela '{table_name}'.")
            
            return df_cleaned

        except Exception as e:
            self.rollback()
            print(f"Erro durante o upsert: {e}")
            raise e
        
    def get_lattes_attribute(self, id_lattes, path_list):
        """
        Extrai um único valor (escalar) do JSON do Lattes.
        
        Args:
            id_lattes (str): O ID do currículo (16 dígitos).
            path_list (list): Lista ordenada do caminho. 
                              Ex: ['CURRICULO-VITAE', 'DADOS-GERAIS', '@NOME-COMPLETO']
        
        Returns:
            str: O valor do atributo ou None se não existir.
        """
        # O operador #>> extrai texto de um caminho JSON especificado por um array de texto postgres
        sql = "SELECT json #>> %s FROM lattes_json WHERE id = %s"
        
        # Passamos path_list direto; o adaptador psycopg2 converterá para ARRAY[] do Postgres
        rows, _ = self.query(sql, (path_list, str(id_lattes)), many=False)
        
        if rows and rows[0]:
            return rows[0]
        return None            
        
    def get_lattes_collection(self, id_lattes, json_path_to_array, attributes_map):
        """
        Usa a função jsonb_array_or_object_elements para extrair uma lista de itens.
        
        Args:
            id_lattes (str): ID do pesquisador.
            json_path_to_array (str): Caminho até o nó (ex: 'CURRICULO-VITAE -> DADOS-GERAIS -> ATUACOES-PROFISSIONAIS -> ATUACAO-PROFISSIONAL')
            attributes_map (dict): {nome_coluna: 'chave_json'}
        """
        cols = ", ".join([f"elem ->> '{v}' AS {k}" for k, v in attributes_map.items()])
        
        sql = f"""
            SELECT {cols}
            FROM lattes_json,
            LATERAL jsonb_array_or_object_elements(json -> {json_path_to_array}) AS elem
            WHERE id = %s
        """
        
        # O método query da sua classe já lida com a execução e retorno
        rows, colnames = self.query(sql, (str(id_lattes),))
        return pd.DataFrame(rows, columns=colnames) if rows else pd.DataFrame()        
    
    def read_sql_to_df(self, sql, params=None, dtypes=None):
        """
        Lê grandes volumes de dados do banco de dados diretamente para um DataFrame Pandas.
        Utiliza o comando COPY TO STDOUT para máxima performance.
        """
        if not self.connected:
            self.open()

        # Prepara a query com os parâmetros
        sql_formatado = self.cursor.mogrify(sql, params).decode('utf-8')
        
        # Cria o comando COPY envolvendo a query original
        copy_query = f"COPY ({sql_formatado.rstrip(';')}) TO STDOUT WITH (FORMAT CSV, HEADER, DELIMITER ',')"

        # Buffer de memória para receber os dados
        buffer = io.StringIO()
        
        try:
            # Executa o streaming do banco para o buffer
            self.cursor.copy_expert(copy_query, buffer)
            buffer.seek(0)
            
            # Carrega o DataFrame a partir do buffer
            # O parâmetro dtype é crucial para não truncar IDs Lattes ou CPFs
            df = pd.read_csv(buffer, dtype=dtypes)
            
            return df
        except Exception as e:
            self.rollback()
            logger.error(f"Erro na leitura massiva: {e}")
            raise e