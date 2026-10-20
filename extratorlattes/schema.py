"""
Esquema do banco de dados do extratorlattes.

Específico deste projeto (tabelas ``lattes_*``); por isso não faz parte do
``acs_toolbox``, que fornece apenas a classe genérica ``Database``.

Tabelas LEGADAS (removidas do banco do CNPq em 10/2026; ver ``indicadores.TABELAS_LEGADAS``): ``lattes_xml``,
``lattes_palavras_chave``, ``lattes_areas_conhecimento`` (id_lattes, tipo, area), ``lattes_publicacoes`` e
``lattes_vinculos``. Rodar ``constroi_tabelas`` as recria vazias. Só faça isso por decisão explícita; LLMs/agentes
devem priorizar o ``lattes_json``. O catálogo oficial de áreas do CNPq é ``cnpq_areas_conhecimento``, que não é
criado nem apagado por este módulo.
"""


def constroi_tabelas(db, drop_if_exists=False):
    """
    Cria as tabelas necessárias no banco de dados.

    Args:
        db: instância de ``acs_toolbox.database.Database`` já conectada.
        drop_if_exists (bool): Se True, apaga tabelas existentes antes de criar.
    """
    drop = ''
    if drop_if_exists:
        drop = '''
DROP TABLE IF EXISTS lattes_indicadores;
DROP TABLE IF EXISTS indicadores_nomes;
DROP TABLE IF EXISTS all_lattes;
drop table IF EXISTS lattes_lista_indicadores;
DROP TABLE IF EXISTS lattes_palavras_chave;
DROP TABLE IF EXISTS lattes_areas_conhecimento;
DROP TABLE IF EXISTS lattes_publicacoes;
DROP TABLE IF EXISTS lattes_vinculos;
DROP TABLE IF EXISTS lattes_dados_gerais;
'''

    sql = f'''
    -- Table: lattes_indicadores

    {drop}

    CREATE TABLE IF NOT EXISTS lattes_indicadores
    (
        id_lattes text NOT NULL,
        ano smallint NOT NULL,
        tipo smallint NOT NULL,
        qty smallint NOT NULL,
        CONSTRAINT indicadores_pkey PRIMARY KEY (id_lattes, ano, tipo)
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
        id_lattes text NOT NULL,
        sgl_pais text COLLATE pg_catalog."default",
        dt_atualizacao date,
        cod_area integer,
        cod_nivel integer,
        dta_carga date,
        erro varchar(15),
        CONSTRAINT all_lattes_pkey PRIMARY KEY (id_lattes)
    )

    TABLESPACE pg_default;


    -- DROP INDEX IF EXISTS all_lattes_dt_atualizacao;

    CREATE INDEX IF NOT EXISTS all_lattes_dt_atualizacao
        ON all_lattes USING btree
        (dt_atualizacao ASC NULLS LAST)
        INCLUDE(id_lattes)
        TABLESPACE pg_default;
    -- Index: all_lattes_id

    -- DROP INDEX IF EXISTS all_lattes_id;

    CREATE INDEX IF NOT EXISTS all_lattes_id
        ON all_lattes USING btree
        (dt_atualizacao ASC NULLS LAST)
        INCLUDE(id_lattes)
        TABLESPACE pg_default;

    CREATE TABLE IF NOT EXISTS lattes_lista_indicadores
    (
        nome_indicador varchar not null
            constraint lista_indicadores_pk
                unique,
        id             integer generated always as identity
    );

    CREATE TABLE IF NOT EXISTS lattes_palavras_chave
    (
        id_lattes      text,
        palavra varchar
    );

    CREATE TABLE IF NOT EXISTS lattes_areas_conhecimento
    (
        id_lattes text not null,
        tipo      varchar,
        area      varchar
    );

    CREATE TABLE IF NOT EXISTS lattes_log_processamento (
        id_lattes BIGINT NOT NULL,
        data_hora TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
        status TEXT NOT NULL,
        PRIMARY KEY (id_lattes, data_hora)
    );

    CREATE UNIQUE INDEX IF NOT EXISTS areas_conhecimento_pk
        ON lattes_areas_conhecimento (id_lattes, tipo, area);

    CREATE TABLE IF NOT EXISTS lattes_publicacoes
    (
        id_lattes       text not null,
        tipo     varchar,
        titulo   varchar,
        doi      varchar,
        path     varchar,
        ano      integer,
        natureza varchar
    );

    CREATE TABLE IF NOT EXISTS lattes_vinculos
    (
        id_lattes            text not null,
        instituicao   varchar,
        num_anos      integer,
        atual         boolean,
        enquadramento varchar,
        tipo          varchar
    );

    CREATE TABLE IF NOT EXISTS lattes_dados_gerais (
        id_lattes text PRIMARY KEY,
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
        
    COMMENT ON TABLE lattes_dados_gerais IS 'Armazena os dados gerais e cadastrais dos currículos Lattes.';
    COMMENT ON COLUMN lattes_dados_gerais.id_lattes IS 'Identificador único do Lattes (16 caracteres), chave primária.';
    COMMENT ON COLUMN lattes_dados_gerais.data_atualizacao IS 'Data e hora da última atualização do currículo na plataforma Lattes.';
    COMMENT ON COLUMN lattes_dados_gerais.nome_completo IS 'Nome completo do titular do currículo.';
    COMMENT ON COLUMN lattes_dados_gerais.nomes_citacao IS 'Nomes utilizados em citações bibliográficas, separados por ponto e vírgula.';
    COMMENT ON COLUMN lattes_dados_gerais.cpf IS 'Cadastro de Pessoas Físicas (CPF) do titular.';
    COMMENT ON COLUMN lattes_dados_gerais.pcd IS 'Indica se a pessoa tem alguma deficiência (Pessoa com Deficiência).';

    CREATE TABLE IF NOT EXISTS lattes_xml
        (
            id_lattes  text not null
                primary key,
            xml xml      not null
        );

    CREATE TABLE IF NOT EXISTS lattes_json
        (
            id_lattes text not null
                primary key,
            json      json not null
        );


        CREATE EXTENSION IF NOT EXISTS pg_trgm;

    CREATE INDEX IF NOT EXISTS idx_lattes_identificacao
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
    return db.execute(sql)
