# IMPORTAÇÕES
from Carga import Carga
from pandas import pandas as pd
import numpy as np
import Database
import json
from datetime import datetime

class pareamentos ():
       
    def __init__ (self):
        # Variáveis de Controle
        
        self.min_para_parear = 3 # -> número de pessoas pareadas de modo fixo, sem usar os erros
        self.num_pareamentos = 3 # -> número de pessoas pareadas ao final -> vai usar erros quadráticos para chegar nesse número
        
        self.table_name = 'pareamentos2'
        self.drop_table_if_exists = True
        
        self.to_print = False
        
        self.parear_por_area_conhecimento = True
        self.parear_por_vinculo = False
        parear_por_ano_doutorado_20 = False
        parear_por_ano_doutorado_10 = False
        parear_por_sexo = False
        parear_por_regiao =  False
        parear_por_erros = True
        parear_por_grupo = [
            #"Graduações Iniciadas",
            "Orientações de Doutorado",
            "Premiações",
            #"Doutorados Iniciados",
            #"Pariticipação em Bancas de Graduação",
            #"Pós-Doutorados",
            #"Pariticipação em Bancas de Pós-Graduação",
            #"Cursos",
            "Atividades de Pesquisa e Desenvolvimento",
            "Produções Técnicas",
            "Serviços Técnicos/Especializados",
            "Atividades de Conselho, Direção ou Administração",
            #"Orientações de Graduação",
            #"Eventos",
            #"Pariticipação em Bancas - Outras",
            #"Livros Publicados",
            #"Atividades de Ensino",
            #"Produções Bibliográficas",
            "Publicações de Artigos",
            #"Vínculos Empregatícios ou Funcionais",
            #"Apresentações de Trabalhos",
            #"Mestrados",
            #"Mídias",
            "Patentes",
            #"Participação em Congressos",
            #"Orientações de Pós-Doutorado",
            #"Livre Docência Obtida",
            "Orientações de Mestrado",
            #"Orientações - Outras",
            #"Outras Pós-Graduações lato sensu",
            "Anos de Doutor",
            #'areas_conhecimento',
            ]
    
    def carrega_dados (self):
    
        #Construindo SQL para criar tabela
        sql = f'''
        -- Table: public.{self.table_name}
        '''
        if self.self.drop_table_if_exists:
            sql += f'''
        DROP TABLE IF EXISTS public.{self.table_name};
        '''
        
        sql += f'''
        
        CREATE TABLE IF NOT EXISTS public.{self.table_name}
        (
            id bigint NOT NULL,
            ano integer,
            faixa "char",
            tipos_pareamento json,
            pareado_1 bigint,
            erro_1 numeric,
            pareado_2 bigint,
            erro_2 numeric,
            pareado_3 bigint,
            erro_3 numeric,
            pareamento_describe json,
            CONSTRAINT {self.table_name}_pkey1 PRIMARY KEY (id)
        )
        
        TABLESPACE pg_default;
        
        ALTER TABLE IF EXISTS public.{self.table_name}
            OWNER to postgres;
            
        COMMENT ON TABLE public.pareamentos IS '
        self.min_para_parear = {self.min_para_parear}
        self.num_pareamentos = {self.num_pareamentos}
        
        self.parear_por_area_conhecimento = {self.parear_por_area_conhecimento}
        self.parear_por_vinculo = {self.parear_por_vinculo}
        parear_por_ano_doutorado_20 = {parear_por_ano_doutorado_20}
        parear_por_ano_doutorado_10 = {parear_por_ano_doutorado_10}
        parear_por_sexo = {parear_por_sexo}
        parear_por_regiao = {parear_por_regiao}
        
        parear_por_erros = {", ".join(parear_por_grupo)}
        
        self.table_name = {self.table_name}
        self.drop_table_if_exists = {self.drop_table_if_exists}
        ';
        '''
    
        #Criando table, se não existir
        self.db = Database.Database('CNPq')
        self.engine = Carga.db_engine()
        self.db.execute(sql)
    
        #Carregar arquivo na memória
        print('Carregando indicadores na memória. Início em: ', datetime.now())
        self.dt = pd.read_sql("select * from indicadores_pareamento where not ano_doutorado is null", self.engine)
        print(f'dt.size: {dt.size}')
        #size: 386.669.844      
    
    
        #Pegando lista de pareamentos já realizados
        print('Carregando lista de ids já feitos. Início em: ', datetime.now())
        ids_já_feitos = []
        sql = f'SELECT DISTINCT id FROM {self.table_name};'
        ids_já_feitos = self.db.query(sql)
        ids_já_feitos = [num[0] for num in ids_já_feitos] # Removendo lista dentro de lista por recuperar SQL
        print(f'len(ids_já_feitos): {len(ids_já_feitos)}')
    
        ## Removendo os ids já feitos
        print('Removendo os ids já feitos')
        self.self.dt = self.dt[~self.dt.id.isin(ids_já_feitos)]
    
    
        print('Realizando normalização da tabela. Início em: ', datetime.now())
    
    
        ## Retirada dos não doutores - Existem 3 não doutores contemplados!!!
        self.dt = self.dt[pd.notnull(self.dt.ano_doutorado)]
        
        
        ## Normalizando as Tabelas
        
        #Preenchendo Nan com zeros nos indicadores
        self.dt.qty_2012 = self.dt.qty_2012.fillna(0)
        self.dt.qty_2013 = self.dt.qty_2013.fillna(0)
        self.dt.qty_2014 = self.dt.qty_2014.fillna(0)
        self.dt.qty_2015 = self.dt.qty_2015.fillna(0)
        self.dt.qty_2016 = self.dt.qty_2016.fillna(0)
        self.dt.qty_2017 = self.dt.qty_2017.fillna(0)
        self.dt.qty_2018 = self.dt.qty_2018.fillna(0)
        self.dt.qty_2019 = self.dt.qty_2019.fillna(0)
        
        #Preenchendo Nan com "nenhum" nas áreas e vínculos
        self.dt.area_demanda_bruta = self.dt.area_demanda_bruta.fillna('nenhum')
        self.dt.areas_conhecimento = self.dt.areas_conhecimento.fillna('nenhum')
        self.dt.tipos_vinculo = self.dt.tipos_vinculo.fillna('nenhum')
        self.dt.enquadramento_vinculo = self.dt.enquadramento_vinculo.fillna('nenhum')
        
        #Removendo acentos e caracteres especiais. Colocando tudo em minúsculas.
        self.dt.area_demanda_bruta = self.dt.area_demanda_bruta.str.normalize('NFKD')\
               .str.encode('ascii', errors='ignore')\
               .str.decode('utf-8')\
               .str.lower()
        self.dt.areas_conhecimento = self.dt.areas_conhecimento.str.normalize('NFKD')\
               .str.encode('ascii', errors='ignore')\
               .str.decode('utf-8')\
               .str.lower()
        self.dt.tipos_vinculo = self.dt.tipos_vinculo.str.normalize('NFKD')\
               .str.encode('ascii', errors='ignore')\
               .str.decode('utf-8')\
               .str.lower()
        self.dt.enquadramento_vinculo = self.dt.enquadramento_vinculo.str.normalize('NFKD')\
               .str.encode('ascii', errors='ignore')\
               .str.decode('utf-8')\
               .str.lower()              
        
        ## Criando tabelas parciais
        print('Criando Tabelas Parciais.')
        financiados = self.dt[~pd.isnull(self.dt.pgtos)]
        self.dt = self.dt[pd.isnull(self.dt.pgtos)]
        
        print('Removendo colunas desnecessárias e duplicatas.')
        ## Alguns pesquisadores foram financiados em um ano, mas não em outro. Assim, aparece duas vezes, uma com pgtos=Nan, outra com pgtos > 0.
        ## Assim, é necessário retirar da lista de pareamento aqueles que foram financiados em algum momento.
        ## Da mesma forma, com certeza houveram aqueles financiados mais de um ano.
        self.dt = self.dt[~self.dt.id.isin(financiados.id.unique())] #Percebi alguns erros, então tive a certeza de remover os financiados da tabela de pareamento
        self.dt = self.dt.drop(['chamada', 'programa'], axis=1)
        self.dt = self.dt.drop_duplicates()
       


#
#
#
## Fazendo Pareamento
#
#
#


lista_pareamentos = [] # -> Lista com todos os pareados -> Útil apenas para conferência depois
lista_pareamentos_describe = []  #  -> lista com os describe de todos os pareamentos

número_ids_total = financiados.id.unique().size
número_ids_já_feitos = 0
tempo_início = datetime.now()
print(f'Iniciado em {tempo_início}.')

for id in financiados.id.unique():
    if not número_ids_já_feitos == 0:
        porcentagem_já_feita = (número_ids_já_feitos/número_ids_total)
        tempo_passado = datetime.now() - tempo_início
        tempo_por_id = tempo_passado / número_ids_já_feitos
        tempo_restante = (número_ids_total - número_ids_já_feitos) * tempo_por_id
        tempo_em_que_vai_acabar = datetime.now() + tempo_restante
        print(f'{número_ids_já_feitos}/{número_ids_total}. {porcentagem_já_feita * 100}% feitos. Fazendo id: {id}. Acabará em {tempo_em_que_vai_acabar}')
    else:
        print(f'{número_ids_já_feitos}/{número_ids_total}. Fazendo id: {id}.')
    número_ids_já_feitos += 1

    #Pegando dados do ID
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
    elif financiado.chamada.str.contains('Faixa C', na=False).unique()[0] == True : faixa = 'C'
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
    if self.parear_por_area_conhecimento:
        area_para_parear = financiado.area_demanda_bruta.unique()[0].split('(')[0].strip()
        if self.to_print: print('     1. Pareando pela área de conhecimento: ', area_para_parear)
        pareados2 = pareados.loc[(pareados.areas_conhecimento.str.contains(area_para_parear, na=False))]
        tamanho_pareados2 = pareados2.id.unique().size
        if tamanho_pareados2 > self.min_para_parear:
            tipos_de_pareamento.append('area_demanda_bruta')
            pareados = pareados2
            if self.to_print: print('         Pareado. Tamanho de Pareados: ', tamanho_pareados2)
        else: 
            if self.to_print: print('         NÃO Pareado. Tamanho de Pareados: ', tamanho_pareados2)
                       
    #2. Pareando pelo Vínculo
    if self.parear_por_vinculo:
        if self.to_print: print('     2. Pareando pelo Vínculo')
        if é_servidor == 'servidor_publico':
            pareados2 = pareados.loc[(pareados.tipos_vinculo.str.contains('servidor_publico', na=False))]
        else:
            pareados2 = pareados.loc[~(pareados.tipos_vinculo.str.contains('servidor_publico', na=False))]
        tamanho_pareados2 = pareados2.id.unique().size
        if tamanho_pareados2 > self.min_para_parear:
            pareados = pareados2
            tipos_de_pareamento.append('tipos_vinculo')
            if self.to_print: print('         Pareado. Tamanho de Pareados: ', tamanho_pareados2)
        else: 
            if self.to_print: print('         NÃO Pareado. Tamanho de Pareados: ', tamanho_pareados2)

                           
    
    #3. Pareando por ano de doutorado -> Faixa de 20 anos
    if parear_por_ano_doutorado_20:
        if self.to_print: print('     3. Pareando por ano de doutorado -> Faixa de 20 anos')
        pareados2 = pareados.loc[
            (pareados.ano_doutorado < financiado.ano_doutorado.unique()[0] + 10) &
            (pareados.ano_doutorado > financiado.ano_doutorado.unique()[0] - 10)
            ]
        tamanho_pareados2 = pareados2.id.unique().size
        if tamanho_pareados2 > self.min_para_parear:
            pareados = pareados2
            tipos_de_pareamento.append('ano_doutorado_10')
            if self.to_print: print('         Pareado. Tamanho de Pareados: ', tamanho_pareados2)
        else: 
            if self.to_print: print('         NÃO Pareado. Tamanho de Pareados: ', tamanho_pareados2)

                       
    #4. Pareando por ano de doutorado -> Faixa de 10 anos
    if parear_por_ano_doutorado_10:
        if self.to_print: print('     4. Pareando por ano de doutorado -> Faixa de 10 anos')
        pareados2 = pareados.loc[
            (pareados.ano_doutorado < financiado.ano_doutorado.unique()[0] + 5) &
            (pareados.ano_doutorado > financiado.ano_doutorado.unique()[0] - 5)
            ]
        tamanho_pareados2 = pareados2.id.unique().size
        if tamanho_pareados2 > self.min_para_parear:
            pareados = pareados2
            tipos_de_pareamento.append('ano_doutorado_05')
            if self.to_print: print('         Pareado. Tamanho de Pareados: ', tamanho_pareados2)
        else: 
            if self.to_print: print('         NÃO Pareado. Tamanho de Pareados: ', tamanho_pareados2)

                       
    #5. Pareando por sexo
    if parear_por_sexo:
        if self.to_print: print('     5. Pareando por sexo')
        pareados2 = pareados.loc[(pareados.sexo == financiado.sexo.unique()[0])]
        tamanho_pareados2 = pareados2.id.unique().size
        if tamanho_pareados2 > self.min_para_parear:
            pareados = pareados2
            tipos_de_pareamento.append('sexo')
            if self.to_print: print('         Pareado. Tamanho de Pareados: ', tamanho_pareados2)
        else: 
            if self.to_print: print('         NÃO Pareado. Tamanho de Pareados: ', tamanho_pareados2)

                       
    #6. Pareando por região
    if parear_por_regiao:
        if self.to_print: print('     6. Pareando por região')
        if tipo_região == 1:
            pareados2 = pareados.loc[pareados.uf.isin(('SP',))]
        elif tipo_região == 2:
            pareados2 = pareados.loc[pareados.uf.isin(('MG', 'RS', 'RJ', 'PR'))]
        elif tipo_região == 3:
            pareados2 = pareados.loc[~pareados.uf.isin(('SP', 'MG', 'RS', 'RJ', 'PR'))]
        tamanho_pareados2 = pareados2.id.unique().size
        if tamanho_pareados2 > self.min_para_parear:
            pareados = pareados2
            tipos_de_pareamento.append('uf')
            if self.to_print: print('         Pareado. Tamanho de Pareados: ', tamanho_pareados2)
        else: 
            if self.to_print: print('         NÃO Pareado. Tamanho de Pareados: ', tamanho_pareados2)

                       
    #7. Pareando por erros quadráticos
    if parear_por_erros:
        tipos_de_pareamento.append('erro')
        if self.to_print: print('     7. Pareando por erros quadráticos')
                                                
        #Fezendo lista de erros quadráticos
        ids_pareados = []
        anos_doutor_erro = {}
        len_pareados = pareados.size
        ano_doutorado_financiado = financiado.ano_doutorado.iloc[0]
        areas_conhecimento_financiado = financiado.areas_conhecimento.iloc[0]
        if not areas_conhecimento_financiado == None:
            areas_conhecimento_financiado = areas_conhecimento_financiado.split(',')
            total_areas = len(areas_conhecimento_financiado)
        areas_conhecimento_erro = {}
        
        x = 0
        for index, row in pareados.iterrows():
            x += 1
            if not row.grupo in parear_por_grupo: continue
            if self.to_print: print(f'\r         {x}/{len_pareados} - {int(100*x/len_pareados)}% - id: {row.id}. Grupo: {row.grupo}          ', end="", flush=True)
            #Pegando o valor do Financiado
            ind_financiado = financiado.loc[(financiado.grupo == row.grupo)]
            if not ind_financiado.empty:
                ind_financiado = ind_financiado['qty_' + str(ano)].iloc[0]
            else:
                ind_financiado = 0
            ind_pareado = row['qty_' + str(ano)]
            erro2 = (ind_financiado - ind_pareado)**2
            erro = {
                'id_pareado': row.id,
                'grupo': row.grupo,
                'erro2': erro2
            }
            ids_pareados.append(erro)
            
            if "Anos de Doutor" in parear_por_grupo:
                if row.id not in anos_doutor_erro:
                    anos_doutor_erro[row.id] = (ano_doutorado_financiado - row.ano_doutorado)**2
                    #print(anos_doutor_erro)
            
            if 'areas_conhecimento' in parear_por_grupo:
                if not areas_conhecimento_financiado == None:
                    if row.id not in areas_conhecimento_erro:
                        
                        areas_em_comum = 0
                        lista_areas_conhecimento = row.areas_conhecimento.split(',')
                        for area in areas_conhecimento_financiado:
                            if area in lista_areas_conhecimento:
                                areas_em_comum += 1
                        areas_conhecimento_erro[row.id] = (total_areas - areas_em_comum)**2
 
                
    
        #Acrescentando Anos de Doutor como um Indicador
        if "Anos de Doutor" in parear_por_grupo:
            for key,value in anos_doutor_erro.items():
                erro = {
                    'id_pareado': key,
                    'grupo': "Anos de Doutor",
                    'erro2': value
                }
                ids_pareados.append(erro)
                #print(erro)
                
        #Acrescentando Áreas do Conhecimento como um Indicador
        if "areas_conhecimento" in parear_por_grupo:
            for key,value in areas_conhecimento_erro.items():
                erro = {
                    'id_pareado': key,
                    'grupo': "areas_conhecimento",
                    'erro2': value
                }
                ids_pareados.append(erro)
 
    
        #Calculando o Desvio Padrão por id pareado
        erro = pd.DataFrame(ids_pareados)
        desvio_padrão = []
        if self.to_print: print('         Calculando o Desvio Padrão por id pareado            ')
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
    
                    #db.insert_dict(self.table_name, err, on_conflict=['id', 'id_pareado'])
                    
            pareados_por_desvio_padrão = pd.DataFrame(desvio_padrão)
            if not pareados_por_desvio_padrão.empty:
                lista_pareados = pareados_por_desvio_padrão.sort_values(by=['erro']).iloc[:self.num_pareamentos]
                #lista_pareamentos_describe.append(pareados_por_desvio_padrão.erro.describe().to_json())
                #print(pareados_por_desvio_padrão.describe())
                #print(lista_pareados)
                      
    if self.to_print: print('     Incluindo no BD')
    lista_pareado_para_incluir_no_bd = {
        'id': id,
        'ano': ano,
        'faixa': faixa,
        'tipos_pareamento': json.dumps(tipos_de_pareamento),
        'pareamento_describe': json.dumps(pareados_por_desvio_padrão.erro.describe().to_json())
    }
    x = 1
    for index, row in lista_pareados.iterrows():
        #print(row)
        lista_pareado_para_incluir_no_bd['pareado_' + str(x)] = row['id_pareado']
        lista_pareado_para_incluir_no_bd['erro_' + str(x)] = row['erro']
        x += 1
        if x > self.num_pareamentos: break
    db.insert_dict(self.table_name, lista_pareado_para_incluir_no_bd, on_conflict=['id'])
    
porcentagem_já_feita = (número_ids_já_feitos/número_ids_total)
print(f'''

Tarefa Terminada.

{número_ids_já_feitos}. {porcentagem_já_feita * 100}% feitos. 
{ids_já_feitos} pulados no início. 
Último feito: {id}. 
Iniciado em: {tempo_início}.
Acabou em {datetime.now()}.
Tempo por id analisado: {(datetime.now() - tempo_início)/número_ids_já_feitos} segundos.

Criando tabela Excel com desvios padrões:
''')

#Carregando pareamentos realizados
pareados = pd.read_sql(self.table_name, engine)

#Carregar arquivo na memória
dt = pd.read_sql(f"select * from public.indicadores_pareamento where not ano_doutorado is null and id in (SELECT distinct(unnest(array[id, pareado_1, pareado_2, pareado_3])) AS id FROM public.{self.table_name})", engine)

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


## Alguns pesquisadores foram financiados em um ano, mas não em outro. Assim, aparece duas vezes, uma com pgtos=Nan, outra com pgtos > 0.
## Assim, é necessário retirar da lista de pareamento aqueles que foram financiados em algum momento.
## Da mesma forma, com certeza houveram aqueles financiados mais de um ano.
dt = dt[~dt.id.isin(financiados.id.unique())] #Percebi alguns erros, então tive a certeza de remover os financiados da tabela de pareamento

## Criando Excel Writer
path = f"C:/Users/silva/CNPq/Lattes/Tabela_Melaine_{self.table_name}.xlsx"
writer = pd.ExcelWriter(path, engine = 'xlsxwriter')
    
# Pegando tabela com contagem de grupos
lista_indicadores = []
for grupo in dt.grupo.unique():
    dt_grupo = dt[dt.grupo == grupo]
    financiados_grupo = financiados[financiados.grupo == grupo]
    tabela = {
        'grupo': grupo,
        'financiados_média': financiados_grupo.qty_2012.mean(),
        'pareados_média': dt_grupo.qty_2012.mean(),
        'financiados_desvio_padrão': financiados_grupo.qty_2012.std(),
        'pareados_desvio_padrão': dt_grupo.qty_2012.std()
    }
    lista_indicadores.append (tabela)

## Tabela Ano Doutorado
financiados_dt = financiados[['id', 'ano_doutorado']].drop_duplicates() 
dt_dt = dt[['id', 'ano_doutorado']].drop_duplicates()  
tabela = {
    'grupo': 'ano_doutorado',
    'financiados_média': financiados_dt.ano_doutorado.mean(),
    'pareados_média': dt_dt.ano_doutorado.mean(),
    'financiados_desvio_padrão': financiados_dt.ano_doutorado.std(),
    'pareados_desvio_padrão': dt_dt.ano_doutorado.std()
}
lista_indicadores.append (tabela)


### Salvando a primeira planilha no Writer
tabela = pd.DataFrame(lista_indicadores)
tabela.to_excel(writer, sheet_name = 'Grupos')

## Tabela Sexo
financiados_sexo = financiados[['id', 'sexo']].drop_duplicates() 
dt_sexo = dt[['id', 'sexo']].drop_duplicates()  
fsm = financiados_sexo[financiados_sexo.sexo == 'M'].id.count()
fsf = financiados_sexo[financiados_sexo.sexo == 'F'].id.count()
dsm = dt_sexo[dt_sexo.sexo == 'M'].id.count()
dsf = dt_sexo[dt_sexo.sexo == 'F'].id.count()
tabela = [['Descrição', 'Valor']]
tabela.append(['grupo', 'sexo'])
tabela.append(['financiados: homens %', fsm/(fsm + fsf)])
tabela.append(['pareados: homens %', dsm/(dsm + dsf)])
tabela.append(['total_financiados', financiados_sexo.id.count()])
tabela.append(['total_pareados', dt_sexo.id.count()])
sexo = pd.DataFrame(tabela)
sexo.to_excel(writer, sheet_name = 'Sexo')

## Salvando o arquivo
writer.save()
writer.close()
print(f'Tabela criada. Fim dos serviços. Finalização em {datetime.now()}')