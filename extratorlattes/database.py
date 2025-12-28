from ntpath import join
import psycopg2
from psycopg2.extensions import AsIs
from configparser import ConfigParser

# Fazendo psycopg2 conseguir trabalhar com NP
# https://stackoverflow.com/questions/39564755/programmingerror-psycopg2-programmingerror-cant-adapt-type-numpy-ndarray
import numpy as np
from psycopg2.extensions import register_adapter, AsIs


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
    def __init__(self, show_sql=False, on_conflict_do_update=True, config_file=r'c:/Python/database.ini', dbparams=None):
        """
        Inicializa a conexão com o banco de dados e define um atributo de status da conexão.
        """
        # 1. Inicializa atributos relacionados à conexão com um estado padrão "desconectado".
        self.conn = None
        self.cur = None
        self.connected = False

        # 2. Define outros atributos da instância.
        self.on_conflict_do_update = on_conflict_do_update
        self.show_sql = show_sql

        try:
            # 3. Tenta ler a configuração e estabelecer a conexão.
            #    O uso de self.__class__ torna o código mais robusto caso o nome da classe seja alterado.
            self.params = self.__class__.config_db_connection(config_file=config_file, dbparams=dbparams)
            self.conn = psycopg2.connect(**self.params)
            self.cur = self.conn.cursor()

            # 4. Se a conexão for bem-sucedida, o atributo de status é atualizado para True.
            self.connected = True

            # O rollback original é mantido para o caso de sucesso.
            self.conn.rollback()

        except (Exception, psycopg2.Error) as error:
            # 5. Se ocorrer um erro, self.connected permanece False.
            #    É uma boa prática registrar ou imprimir o erro para depuração.
            print(f"Falha na conexão com o banco de dados: {error}")

    @staticmethod
    def db_engine(config_file=r'c:/Python/database.ini', dbparams=None):
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
        elif type(dbparams) == dict:
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
    def engine(filename=r'c:/Python/database.ini'):
        """
        Retorna a string de conexão SQLAlchemy (alias para db_engine).
        """
        return Database.db_engine(filename=filename)

    @staticmethod
    def config_db_connection(config_file=r'c:/Python/database.ini', section='postgresql', dbparams=None):
        """
        Lê a configuração do banco de dados de um arquivo INI ou dicionário.
        """
        if dbparams is not None:
            if type(dbparams) == dict:
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
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Exit the runtime context and close the connection.
        The parameters describe the exception that caused the context to be exited.
        If the context was exited without an exception, all three arguments will be None.
        """
        if exc_type is not None:
            # An exception occurred, rollback transaction
            self.connection.rollback()
        else:
            # No exception, commit transaction
            self.close(commit=True)


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
            # self.conn = psycopg2.connect(**self.params)
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
        if hasattr(self, 'connection') and not self.connection.closed:
            self.connection.rollback()

    def close(self, commit=True):
        """
        Fecha a conexão com o banco de dados.

        Args:
            commit (bool): Se True, faz commit antes de fechar.
        """
        if hasattr(self, 'connection') and not self.connection.closed:
            if commit:
                self.commit()
            self.cursor.close()
            self.connection.close()
        self.connected = False

    def execute(self, sql, params=None):
        """
        Executa um comando SQL.

        Args:
            sql (str): Comando SQL.
            params (list/tuple, optional): Parâmetros para a query.

        Returns:
            int: Número de linhas afetadas ou o erro ocorrido.
        """
        if not self.connected:
            raise psycopg2.InterfaceError("Database connection is not open.")
        if not params == None and (
                type(params) == str or
                type(params) == int or
                type(params) == float):
            params = [params]
        sql_to_execute = self.cursor.mogrify(sql, params or None)
        if self.show_sql:
            print(sql_to_execute.decode())
        try:
            self.cursor.execute(sql_to_execute)
            resultado = self.cursor.rowcount
            self.commit()
        except Exception as error:
            self.rollback()
            resultado = error
        return resultado

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
        if not params == None and (
                type(params) == str or
                type(params) == int or
                type(params) == float):
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
        except Exception as error:
            print(f"Error during query: {error}")
            self.rollback()
        return rows, colnames

    def insert_many(self, sql, params_list = None, params=None):
        """
        Insere múltiplos registros de uma vez.
        """
        if not params_list == None or len(params_list) > 0:
            params_list_string = '(' + \
                ','.join(["%s" for _ in range(len(params_list[0]))]) + ')'
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
        params_list_string = '(' + \
            ','.join(["%s" for _ in range(len(data[0]))]) + ')'
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
            id char(16) NOT NULL,
            ano smallint NOT NULL,
            tipo smallint NOT NULL,
            qty smallint NOT NULL,
            CONSTRAINT indicadores_pkey PRIMARY KEY (id, ano, tipo)
        )

        TABLESPACE pg_default;

        ALTER TABLE IF EXISTS indicadores
            OWNER to {self.params['user']};

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

        ALTER TABLE IF EXISTS indicadores_nomes
            OWNER to  {self.params['user']};

        -- Table: all_lattes

        CREATE TABLE IF NOT EXISTS all_lattes
        (
            id char(16) NOT NULL,
            sgl_pais text COLLATE pg_catalog."default",
            dt_atualizacao date,
            cod_area integer,
            cod_nivel integer,
            dta_carga date,
            erro varcha(15),
            CONSTRAINT all_lattes_pkey PRIMARY KEY (id)
        )

        TABLESPACE pg_default;

        ALTER TABLE IF EXISTS all_lattes
            OWNER to  {self.params['user']};
        -- Index: all_lattes_dt_atualizacao

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

        alter table lista_indicadores
            owner to albertocampos;

        CREATE TABLE IF NOT EXISTS palavras_chave
        (
            id      char(16),
            palavra varchar
        );

        alter table palavras_chave
            owner to albertocampos;

        CREATE TABLE IF NOT EXISTS areas_conhecimento
        (
            id   char(16) not null,
            tipo varchar,
            area varchar
        );

        alter table areas_conhecimento
            add constraint areas_conhecimento_pk
                unique (id, tipo, area);


        alter table areas_conhecimento
            owner to albertocampos;

        CREATE TABLE IF NOT EXISTS publicacoes
        (
            id       char(16) not null,
            tipo     varchar,
            titulo   varchar,
            doi      varchar,
            path     varchar,
            ano      integer,
            natureza varchar
        );

        alter table publicacoes
            owner to albertocampos;

        CREATE TABLE IF NOT EXISTS vinculos
        (
            id            char(16) not null,
            instituicao   varchar,
            num_anos      integer,
            atual         boolean,
            enquadramento varchar,
            tipo          varchar
        );

        alter table vinculos
            owner to albertocampos;

        CREATE TABLE IF NOT EXISTS dados_gerais (
            id CHAR(16) PRIMARY KEY,
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

        alter table dados_gerais
            owner to albertocampos;
            
        COMMENT ON TABLE dados_gerais IS 'Armazena os dados gerais e cadastrais dos currículos Lattes.';
        COMMENT ON COLUMN dados_gerais.id IS 'Identificador único do Lattes (16 caracteres), chave primária.';
        COMMENT ON COLUMN dados_gerais.data_atualizacao IS 'Data e hora da última atualização do currículo na plataforma Lattes.';
        COMMENT ON COLUMN dados_gerais.nome_completo IS 'Nome completo do titular do currículo.';
        COMMENT ON COLUMN dados_gerais.nomes_citacao IS 'Nomes utilizados em citações bibliográficas, separados por ponto e vírgula.';
        COMMENT ON COLUMN dados_gerais.cpf IS 'Cadastro de Pessoas Físicas (CPF) do titular.';
        COMMENT ON COLUMN dados_gerais.pcd IS 'Indica se a pessoa tem alguma deficiência (Pessoa com Deficiência).';

        create table lattes_xml
            (
                id  char(16) not null
                    primary key,
                xml xml      not null
            );

            alter table lattes_xml
                owner to albertocampos;



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
