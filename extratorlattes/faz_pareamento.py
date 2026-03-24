# IMPORTAÇÕES
from Carga import Carga
from pandas import pandas as pd
import numpy as np
import unidecode
import Database
import json

#Carregar arquivo na memória
db = Database.Database('CNPq')
engine = Carga.db_engine()
dt = pd.read_sql("indicadores_pareamento", engine)
#size: 386.669.844

#Pegando lista de pareamentos jpa realizados
ids_já_feitos = []
#sql = 'SELECT DISTINCT id FROM pareamentos;'
#ids_já_feitos = db.query(sql)
#ids_já_feitos = [num[0] for num in ids_já_feitos] # Removendo lista dentro de lista por recuperar SQL

## Retirada dos não doutores - Existem 3 não doutores contemplados!!!
dt = dt[pd.notnull(dt.ano_doutorado)]

## Normalizando as Tabelas

#Preenchendo Nan com zeros nos indicadores
dt.qty_2012 = dt.qty_2012.fillna(0)
dt.qty_2013 = dt.qty_2013.fillna(0)
dt.qty_2014 = dt.qty_2014.fillna(0)
dt.qty_2015 = dt.qty_2015.fillna(0)
dt.qty_2016 = dt.qty_2016.fillna(0)
dt.qty_2017 = dt.qty_2017.fillna(0)
dt.qty_2018 = dt.qty_2018.fillna(0)
dt.qty_2019 = dt.qty_2019.fillna(0)

#Preenchendo Nan com "nenhum" nas áreas e vínculos
dt.area_demanda_bruta = dt.area_demanda_bruta.fillna('nenhum')
dt.areas_conhecimento = dt.areas_conhecimento.fillna('nenhum')
dt.tipos_vinculo = dt.tipos_vinculo.fillna('nenhum')
dt.enquadramento_vinculo = dt.enquadramento_vinculo.fillna('nenhum')

#Removendo acentos e caracteres especiais. Colocando tudo em minúsculas.
dt.area_demanda_bruta = dt.area_demanda_bruta.str.normalize('NFKD')\
       .str.encode('ascii', errors='ignore')\
       .str.decode('utf-8')\
       .str.lower()
dt.areas_conhecimento = dt.areas_conhecimento.str.normalize('NFKD')\
       .str.encode('ascii', errors='ignore')\
       .str.decode('utf-8')\
       .str.lower()
dt.tipos_vinculo = dt.tipos_vinculo.str.normalize('NFKD')\
       .str.encode('ascii', errors='ignore')\
       .str.decode('utf-8')\
       .str.lower()
dt.enquadramento_vinculo = dt.enquadramento_vinculo.str.normalize('NFKD')\
       .str.encode('ascii', errors='ignore')\
       .str.decode('utf-8')\
       .str.lower()              

## Criando tabelas parciais
financiados = dt[~pd.isnull(dt.pgtos)]
dt = dt[pd.isnull(dt.pgtos)]
#erros = dt[dt.id.isin(financiados.id.unique())] #-> Listagem dos erros encontrados
dt = dt[~dt.id.isin(financiados.id.unique())] #Percebi alguns erros, então tive a certeza de remover os financiados da tabela de pareamento

#
#
#
## Fazendo Pareamento
#
#
#

min_para_parear = 3 # -> número de pessoas pareadas de modo fixo, sem usar os erros
num_pareamentos = 3 # -> número de pessoas pareadas ao final -> vai usar erros quadráticos para chegar nesse número

lista_pareamentos = [] # -> Lista com todos os pareados -> Útil apenas para conferência depois
lista_pareamentos_describe = []  #  -> lista com os describe de todos os pareamentos

for id in financiados.id.unique():

    #Pegando dados do ID
    print('Fazendo id: ', id)
    financiado = financiados[financiados.id == id]
    
    #Pegando áreas do Conhecimento do Financiado
    financiado.areas_conhecimento.iloc[0].split(',')
    
    #Pegando Regiões do Financiado
    região = financiado.iloc[0].uf
    tipo_região = None
    if not região == None:
        if região in ('SP'):
            tipo_região = 1
        elif região in ('MG', 'RS', 'RJ', 'PR'):
            tipo_região = 2
        else:
            tipo_região = 3
    
    #Pegando ano do Indicador a ser usado do Financiado -> Útil para calcular erro -> Vai ser usado ao calcular o impacto
    if financiado.chamada.str.contains('2012', na=False).unique()[0] == True : ano = 2012
    elif financiado.chamada.str.contains('2013', na=False).unique()[0] == True : ano = 2013
    elif financiado.chamada.str.contains('2014', na=False).unique()[0] == True : ano = 2014
    else: ano = None

    #Pegando Faixa do Financiado do Financiado -> Inútil aqui -> Vai ser usado ao calcular o impacto
    if financiado.chamada.str.contains('Faixa A', na=False).unique()[0] == True : faixa = 'A'
    elif financiado.chamada.str.contains('Faixa B', na=False).unique()[0] == True : faixa = 'B'
    elif financiado.chamada.str.contains('Faixa c', na=False).unique()[0] == True : faixa = 'C'
    else: faixa = None
        
    #Pegando o vínculo do Financiado
    tipos_vinculo = financiado.tipos_vinculo.unique()
    if not tipos_vinculo == None and len(tipos_vinculo) > 0:   
        if tipos_vinculo[0].find('servidor_publico') > -1:
            é_servidor = 'servidor_publico'
        else:
            é_servidor = 'outros'   
    else: é_servidor = 'nenhum'
    
    #   
    #Fazendo o Pareamento
    #
    tipos_de_pareamento = []  #Variável que vai indicar quais os tipos de pareamento realizados
    pareados = dt
    
    #1. Pareando pela área de conhecimento
    pareados2 = pareados.loc[(pareados.areas_conhecimento.str.contains(financiado.area_demanda_bruta.unique()[0].split('(')[0].strip(), na=False))]
    if pareados2.id.unique().size > min_para_parear:
        tipos_de_pareamento.append('area_demanda_bruta')
        pareados = pareados2
                       
    #2. Pareando pelo Vínculo
    if é_servidor == 'servidor_publico':
        pareados2 = pareados.loc[(pareados.tipos_vinculo.str.contains('servidor_publico', na=False))]
    else:
        pareados2 = pareados.loc[~(pareados.tipos_vinculo.str.contains('servidor_publico', na=False))]
    if pareados2.id.unique().size > min_para_parear:
        pareados = pareados2
        tipos_de_pareamento.append('tipos_vinculo')
                       
    
    #3. Pareando por ano de doutorado -> Faixa de 20 anos
    pareados2 = pareados.loc[
        (pareados.ano_doutorado < financiado.ano_doutorado.unique()[0] + 10) &
        (pareados.ano_doutorado > financiado.ano_doutorado.unique()[0] - 10)
        ]
    if pareados2.id.unique().size > min_para_parear:
        pareados = pareados2
        tipos_de_pareamento.append('ano_doutorado_10')
                       
    #4. Pareando por ano de doutorado -> Faixa de 10 anos
    pareados2 = pareados.loc[
        (pareados.ano_doutorado < financiado.ano_doutorado.unique()[0] + 5) &
        (pareados.ano_doutorado > financiado.ano_doutorado.unique()[0] - 5)
        ]
    if pareados2.id.unique().size > min_para_parear:
        pareados = pareados2
        tipos_de_pareamento.append('ano_doutorado_05')
                       
    #5. Pareando por sexo
    pareados2 = pareados.loc[(pareados.sexo == financiado.sexo.unique()[0])]
    if pareados2.id.unique().size > min_para_parear:
        pareados = pareados2
        tipos_de_pareamento.append('sexo')
                       
    #6. Pareando por região
    if tipo_região == 1:
        pareados2 = pareados.loc[pareados.uf.isin(('SP',))]
    elif tipo_região == 2:
        pareados2 = pareados.loc[pareados.uf.isin(('MG', 'RS', 'RJ', 'PR'))]
    elif tipo_região == 3:
        pareados2 = pareados.loc[~pareados.uf.isin(('SP', 'MG', 'RS', 'RJ', 'PR'))]
    if pareados2.id.unique().size > min_para_parear:
        pareados = pareados2
        tipos_de_pareamento.append('uf')
                       
    #7. Pareando por erros quadráticos
    tipos_de_pareamento.append('erro')
                                            
    #Fezendo lista de erros quadráticos
    ids_pareados = []
    if not pareados.empty:

        #Calculando os erros dos grupos
        # "grupo" é um dos grupos de indicadores.
        for grupo in financiado.grupo.unique():

            #Pegando o valor do Financiado
            ind_financiado = financiado.loc[(financiado.grupo == grupo)]['qty_' + str(ano)].iloc[0]

            #Verificando o erro dos pareados restantes
            for id_pareado in pareados.id.unique():
                ind_pareado = pareados.loc[
                    (pareados.grupo == grupo)
                    & (pareados.id == id_pareado)
                    ]['qty_' + str(ano)]
                if len(ind_pareado) > 0:
                    ind_pareado = ind_pareado.iloc[0]
                else:
                    ind_pareado = 0
                erro = {
                    'id': id,
                    'id_pareado': id_pareado,
                    'grupo': grupo,
                    'valor': ind_pareado,
                    'erro2': (ind_financiado - ind_pareado)**2
                }
                ids_pareados.append(erro)

        #Acrescentando Anos de Doutor como um Indicador
        for id_pareado in pareados.id.unique():
            ind_pareado = pareados.ano_doutorado.loc[
                (pareados.id == id_pareado)
                ].unique()
            #print(ind_pareado, type(ind_pareado))
            if len(ind_pareado) > 0:
                ind_pareado = ind_pareado[0]
            else:
                ind_pareado = 0
            erro = {
                'id': id,
                'id_pareado': id_pareado,
                'grupo': "Anos de Doutor",
                'valor': ind_pareado,
                'erro2': (financiado.ano_doutorado.iloc[0] - ind_pareado)**2
            }
            ids_pareados.append(erro)

        #Acrescentando Áreas do Conhecimento em Comum como outro indicador
        areas_conhecimento_financiado = financiado.areas_conhecimento.iloc[0]
        if not areas_conhecimento_financiado == None:
            areas_conhecimento_financiado = areas_conhecimento_financiado.split(',')
            total_areas = len(areas_conhecimento_financiado)

            for id_pareado in pareados.id.unique():
                areas_em_comum = 0
                lista_areas_conhecimento = pareados.loc[pareados.id == id_pareado].areas_conhecimento
                if len(lista_areas_conhecimento) > 0 and not lista_areas_conhecimento.iloc[0] == None:
                    lista_areas_conhecimento = lista_areas_conhecimento.iloc[0].split(',')
                    for area in areas_conhecimento_financiado:
                        if area in lista_areas_conhecimento:
                            areas_em_comum += 1
                erro = {
                    'id': id,
                    'id_pareado': id_pareado,
                    'grupo': 'areas_conhecimento',
                    'valor': areas_em_comum,
                    'erro2': (total_areas - areas_em_comum)**2
                }
                ids_pareados.append(erro)    

    #Calculando o Desvio Padrão por id pareado
    erro = pd.DataFrame(ids_pareados)
    desvio_padrão = []
    if not erro.empty:
        desvio_padrão = []  # -> VARIÁVEL QUE VAI CONTER OS IDS PAREADOS
        for id_pareado in erro.id_pareado.unique():
            erro_id = erro.loc[erro.id_pareado == id_pareado]
            if not erro_id.empty:
                soma_erro = erro_id.erro2.sum()
                num_erros = len(erro_id.index)
                err = {
                    'id_pareado': id_pareado,
                    'erro': np.sqrt(soma_erro/num_erros),
                }
                
                desvio_padrão.append(err)
                lista_pareamentos.append(err)

                #db.insert_dict("pareamentos", err, on_conflict=['id', 'id_pareado'])
        pareados_por_desvio_padrão = pd.DataFrame(desvio_padrão)
        if not pareados_por_desvio_padrão.empty:
            lista_pareados = pareados_por_desvio_padrão.sort_values(by=['erro']).iloc[:num_pareamentos]
            lista_pareamentos_describe.append(pareados_por_desvio_padrão.describe().to_json())
            print(pareados_por_desvio_padrão.describe())
            #print(lista_pareados)
                      
    lista_pareado_para_incluir_no_bd = {
        'id': id,
        'ano': ano,
        'faixa': faixa,
        'tipos_pareamento': json.dumps(tipos_de_pareamento),
        'pareamento_describe': json.dumps(pareados_por_desvio_padrão.describe().to_json())
    }
    x = 1
    for pareado in desvio_padrão:
        lista_pareado_para_incluir_no_bd['pareado_' + str(x)] = pareado['id_pareado']
        lista_pareado_para_incluir_no_bd['erro_' + str(x)] = pareado['erro']
        x += 1
        if x > num_pareamentos: break
    db.insert_dict('pareamentos', lista_pareado_para_incluir_no_bd, on_conflict=['id'])
    print(lista_pareado_para_incluir_no_bd)
    