import extratorlattes as lt
import math
from pandas import pandas as pd
import statsmodels.api as sm
from urllib.parse import urlencode
from datetime import datetime
from urllib.parse import urlencode
import statsmodels.formula.api as sm2
from itertools import combinations


class Regressao:

    dt = pd.DataFrame()

    def __init__(self,
                 data_atualização='2020-01-01',
                 path='d:/Lattes/Linnear Regression Models/',
                 indicador=None,
                 programa=None,
                 CA=None,
                 área=None,
                 chamada=None,
                 ano_início=2014,
                 ano_fim=2019,
                 ):
        self.tipo_indicador = indicador
        self.ano_início = ano_início
        self.ano_fim = ano_fim
        self.data_atualização = data_atualização
        self.path = path
        self.programa = programa
        self.select_programa = True
        self.CA = CA
        self.select_CA = False
        self.área = área
        self.select_área = False
        self.chamada = chamada
        self.select_chamada = True
        self.include = []
        self.média_investimento = None
        self.total_investido = None
        self.lista_indicadores = []
        self.lista_cas = []
        self.lista_chamadas = []
        self.lista_programas = []
        self.est2 = None
        self.lista_regressões = []

        self.db = lt.Database('CNPq')

        # variáveis de inicialização
        self.variável_dependente = 'qty_2019'
        self.variáveis_independentes = ['qty_2014', 'porcent_pagos']
        self.confianças = [0.01, 0.05]
        self.confiança = 0.05
        self.indicador = 'Artigo Publicado'
        self.ano_início = 2014
        self.best_r2 = True
        # Verificou-se que não há motivos estatísticos para preencher os faltantes.
        self.drop_not_significant = False
        self.pareamento = True
        self.capes = False
        self.cnpq = True
        self.nenhum = True
        self.ambos = False
        self.preenche_todos_ids = True
        self.verbose = False
        self.confiança = 0.01

        # Variáveis da Regressão
        self.dt = pd.DataFrame()

        # Variáveis para controlar tempo para a regressão
        tempo_início = datetime.now()
        tempo_último = datetime.now()
        feitos = 0

        # Variáveis para bugs
        self.faz_apenas_os_indicadores = None
        # self.faz_apenas_os_indicadores = ['Artigo Publicado']

    def gera_sql(self,
                 ignore_filters=False
                 ):
        sql_select = f'''
        select all_lattes.uuid 
            ,indicadores_nomes.nome as tipo
            ,pgtos'''
        for ano in range(self.ano_início, self.ano_fim + 1):
            sql_select += f'\n            ,sum(qty) FILTER (WHERE ano <= {ano}) AS qty_{ano}'

        sql_from = '''
        from demanda_bruta
        left join
                (select "Processo" as processo, sum("Valor Pago") as pgtos from pagamentos
                where "Processo" in (select "Processo" from demanda_bruta)
                group by "Processo") AS pgtos
            on demanda_bruta."Processo" = pgtos.processo
        left join indicadores
            on CAST(demanda_bruta.id as bigint) = indicadores.id
        inner join all_lattes
            on CAST(demanda_bruta.id as bigint) = all_lattes.id
        inner join indicadores_nomes
            on indicadores.tipo = indicadores_nomes.tipo
        '''

        sql_where = f"""
        WHERE
            all_lattes.dt_atualizacao > '{self.data_atualização}'"""

        sql_group_by = '''
        GROUP BY
            indicadores.id 
            ,indicadores_nomes.nome
            ,demanda_bruta."Processo"
            ,demanda_bruta."Chamada"
            ,pgtos.pgtos'''

        if not self.tipo_indicador is None:
            if not ignore_filters:
                sql_where += f"\n            and indicadores_nomes.nome  = '{self.tipo_indicador}'"
        if not self.chamada is None:
            if not ignore_filters:
                sql_where += f'\n            and demanda_bruta.\"Chamada\" = \'{self.chamada}\''
        if not self.programa is None:
            self.select_programa = True
            if not ignore_filters:
                sql_where += f'\n            and demanda_bruta."Programa" = \'{self.programa}\''
        if not self.CA is None:
            self.select_CA = True
            if not ignore_filters:
                sql_where += f'\n            and demanda_bruta."CA" = \'{self.CA}\''
        if not self.área is None:
            self.select_área = True
            sql_from += '''
                        inner join areas_conhecimento
                            on CAST(demanda_bruta.id as bigint) = areas_conhecimento.id'''
            if not ignore_filters:
                sql_where += f"\n            and areas_conhecimento.area = \'{self.área}\'"

        if self.select_chamada:
            sql_select += '\n            ,"Chamada" as chamada'
        if self.select_área:
            sql_select += '\n            ,areas_conhecimento.area'
            sql_group_by += '\n            ,areas_conhecimento.area'
        if self.select_programa:
            sql_select += '\n            ,demanda_bruta."Programa" AS programa'
            sql_group_by += '\n            ,demanda_bruta."Programa"'
        if self.select_CA:
            sql_select += '\n            ,demanda_bruta."CA" AS ca'
            sql_group_by += '\n            ,demanda_bruta."CA"'

        self.sql = sql_select + sql_from + sql_where + sql_group_by

    def get_parâmetros(self):
        parâmetros = {
            'chamada': self.chamada,
            'programa': self.programa,
            'CA': self.CA,
            'área': self.área,
            'indicador': self.tipo_indicador,
            'data': self.data_atualização,
        }
        return parâmetros

    def grava_resultados(self):
        if not self.est2 is None:
            filename = self.path + urlencode(self.get_parâmetros(), doseq=True) + ".pickle"
            self.est2.save(filename)
            resultado = {
                'chamada': self.chamada,
                'programa': self.programa,
                'indicador': self.tipo_indicador,
                'data': self.data_atualização,
                'ca': self.CA,
                'area': self.área,
                'total_investido': self.total_investido,
                'media_investimento': self.média_investimento,
                'f_total': self.est2.f_pvalue,
                'f_pagtos': self.est2.pvalues.get('pgtos'),
                'parametro_pgtos': self.est2.params.get('pgtos'),
                'f_qty_i': self.est2.pvalues.get('qty_' + str(self.ano_início)),
                'parametro_qty_i': self.est2.params.get('qty_' + str(self.ano_início)),
                'f_const': self.est2.pvalues.get('const'),
                'parametro_const': self.est2.params.get('const'),
                'r2': self.est2.rsquared,
                'num_observacoes': self.est2.nobs,
            }
        else:
            resultado = {
                'chamada': self.chamada,
                'programa': self.programa,
                'indicador': self.tipo_indicador,
                'data': self.data_atualização,
                'ca': self.CA,
                'area': self.área,
                'total_investido': self.total_investido,
                'media_investimento': self.média_investimento,
                'f_total': None,
                'f_pagtos': None,
                'parametro_pgtos': None,
                'f_qty_i': None,
                'parametro_qty_i': None,
                'f_const': None,
                'parametro_const': None,
                'r2': None,
                'num_observacoes': None,
            }

        for key in resultado.keys():
            if pd.isnull(resultado[key]):
                resultado[key] = None

        self.db.insert_dict(column_name='resultados_regressao_linear',
                            dict=resultado, on_conflict=["chamada", "programa", "indicador"])
        self.db.commit()

    def já_feita(self):
        # filename = self.path + urlencode(self.get_parâmetros(), doseq=True)
        # if os.path.isfile(filename):
        #     return True
        existe = False
        SQL = '''
                SELECT count(*) 
                FROM resultados_regressao_linear
                WHERE
                    chamada = %s and
                    programa = %s and
                    indicador = %s and
                    data = %s
                '''
        data = (self.chamada, self.programa, self.tipo_indicador, self.data_atualização)
        num_indicadores = self.db.query(SQL, data)
        if int(num_indicadores[0][0]) > 0:
            existe = True
        return existe

    def get_pd(self, chamada=None, ignore_filters=False):
        if not chamada is None:
            self.chamada = chamada
        self.gera_sql(ignore_filters=ignore_filters)
        # print(SQL)
        self.pd = pd.read_sql(self.sql, self.db.engine())
        self.pd['pgtos'] = pd.to_numeric(self.pd['pgtos'].fillna(0))
        for ano in range(self.ano_início, self.ano_fim + 1):
            self.pd['qty_' +
                    str(ano)] = pd.to_numeric(self.pd['qty_' + str(ano)].fillna(0))
        self.pd.dropna(subset=["id"], inplace=True)

    def fit(self, refaz=False):
        data = self.pd
        if refaz or not self.já_feita():
            if not self.tipo_indicador is None:
                data = data[data.tipo == self.tipo_indicador]
            if not self.chamada is None:
                data = data[data.chamada == self.chamada]
            if not self.programa is None:
                data = data[data.programa == self.programa]
            if not self.CA is None:
                data = data[data.ca == self.CA]
            if not self.área is None:
                data = data[data.area == self.área]
            X = data[['pgtos', 'qty_' + str(self.ano_início)]]
            y = pd.DataFrame(data, columns=['qty_' + str(self.ano_fim)])
            self.média_investimento = float(data[data['pgtos'] != 0].pgtos.mean())
            self.total_investido = float(data[data['pgtos'] != 0].pgtos.sum())
            try:
                print('Começando estimativa...')
                X2 = sm.add_constant(X)
                est = sm.OLS(y, X2)
                self.est2 = est.fit()
                print('Regressão realizada. Gravando resultados.')
                self.grava_resultados()
                return self.est2.summary()
            except Exception as e:
                self.est2 = None
                self.grava_resultados()
                return 'Erro na regressão: ' + str(e)
        else:
            return 'Regressão já realizada - não será feita novamente.'

    def pega_lista_indicadores(self):
        if self.lista_indicadores == []:
            SQL = '''
                    select distinct nome from indicadores_nomes
                    '''
            self.lista_indicadores = [x[0] for x in self.db.query(SQL)]
        return self.lista_indicadores

    def pega_lista_programas(self):
        if self.lista_programas == []:
            SQL = '''
                    select distinct "Programa" from demanda_bruta
                    '''
            self.lista_programas = [x[0] for x in self.db.query(SQL)]
        return self.lista_programas

    def pega_lista_chamadas(self):
        if self.lista_chamadas == []:
            SQL = '''
                    select distinct "Chamada" from demanda_bruta
                    '''
            self.lista_chamadas = [x[0] for x in self.db.query(SQL)]
        return self.lista_chamadas

    def pega_lista_cas(self):
        if self.lista_cas == []:
            SQL = '''
                    select distinct "CA" from demanda_bruta
                    '''
            self.lista_cas = [x[0] for x in self.db.query(SQL)]
        return self.lista_cas

    def faz_regressoes(self,
                       atualiza_listas=True,
                       data_atualização='2020-01-01',
                       refaz=False):
        self.data_atualização = data_atualização
        if atualiza_listas:
            print('Pegando listas de regressões:')
            self.pega_lista_chamadas()
            self.pega_lista_programas()
            self.pega_lista_indicadores()
            print('Pegando lista de indicadores no BD')
            self.select_chamada = True
            self.select_programa = True
            self.get_pd(ignore_filters=True)
        print('Começando a regressão.')
        num_chamada = 0
        num_programa = 0
        num_indicador = 0
        for self.chamada in self.lista_chamadas:

            num_chamada += 1
            num_programa = 0
            for self.programa in self.lista_programas:
                num_programa += 1
                num_indicador = 0
                for self.tipo_indicador in self.lista_indicadores:
                    num_indicador += 1
                    self.est = None
                    print(f'''
Regressão Linear:
Chamada {num_chamada} / {len(self.lista_chamadas)}: {self.chamada}   
Programa {num_programa} / {len(self.lista_programas)}: {self.programa}                 
Indicador {num_indicador} / {len(self.lista_indicadores)}: {self.tipo_indicador}''')
                    resultado = self.fit(refaz=refaz)
                    # os.system('cls')
                    print(f'''
Resultado:
{resultado}

___
''')
        self.db.close()

    # FUNÇÕES PARA REALIZAR O PAREAMENTO DE DOUTORADONDOS - ARTIGO RBPAE

    def pega_tabela_para_regressão(self, sql=None):
        '''
        Pega os indicadores para fazer o pareamento e os armazena em self.data_frame        
        '''
        print('Carregando indicadores na memória. Início em: ', datetime.now())

        if sql is None:
            sql = '''
            select * from financiamentos_doutorandos_2014
                left join indicadores_doutorado
                    on indicadores_doutorado.id = financiamentos_doutorandos_2014.id
                inner join indicadores_nomes
                    on indicadores_nomes.tipo = indicadores_doutorado.indicador_tipo                 
            '''
        # Carregar arquivo na memória
        if self.verbose:
            print(f'SQL a ser rodada: {sql}')

        df = pd.read_sql(sql, self.db.engine())
        if self.verbose:
            print(f'df.size: {df.size}')

        if self.verbose:
            print('Realizando normalização da tabela. Início em: ', datetime.now())

        df['const'] = 1
        df = df.loc[:, ~df.columns.duplicated()]

        df['bool_cnpq'] = df.apply(lambda d: 1 if (d['capes'] == False and d['pagtos'] > 0)
                                   else 0, axis=1)
        df['bool_capes'] = df.apply(lambda d: 1 if (d['capes'] == True and d['pagtos'] == 0)
                                    else 0, axis=1)
        df['bool_ambos'] = df.apply(lambda d: 1 if (d['capes'] == True and d['pagtos'] > 0)
                                    else 0, axis=1)
        df['bool_nenhum'] = df.apply(lambda d: 1 if (d['capes'] == False and d['pagtos'] == 0)
                                     else 0, axis=1)

        # Preenchendo Nan com zeros nos indicadores
        df.qty_2012 = df.qty_2012.fillna(0)
        df.qty_2013 = df.qty_2013.fillna(0)
        df.qty_2014 = df.qty_2014.fillna(0)
        df.qty_2015 = df.qty_2015.fillna(0)
        df.qty_2016 = df.qty_2016.fillna(0)
        df.qty_2017 = df.qty_2017.fillna(0)
        df.qty_2018 = df.qty_2018.fillna(0)
        df.qty_2019 = df.qty_2019.fillna(0)
        df.capes = df.capes.fillna(0)
        df.pagtos = df.pagtos.fillna(0)

        valor_total_cnpq = df[['id', 'pagtos']
                              ].loc[df['pagtos'] > 0].drop_duplicates().sum().pagtos
        df['porcent_pagos'] = df['pagtos']/valor_total_cnpq

        df['grupo'] = df.apply(lambda d: 'Ambos' if (d['capes'] == True and d['pagtos'] > 0)
                               else 'CAPES' if (d['capes'] == True and d['pagtos'] == 0)
                               else 'CNPq' if d['pagtos'] > 0
                               else 'Nenhum', axis=1)

        # Apagando os financiados
        if not self.capes:
            if self.verbose:
                print('Dropping CAPES')
            df.drop(df.loc[df.grupo == 'CAPES'].index, inplace=True)
        if not self.cnpq:
            if self.verbose:
                print('Dropping CNPq')
            df.drop(df.loc[df.grupo == 'CNPq'].index, inplace=True)
        if not self.nenhum:
            if self.verbose:
                print('Dropping no financed')
            df.drop(df.loc[df.grupo == 'Nenhum'].index, inplace=True)
        if not self.ambos:
            if self.verbose:
                print('Dropping Ambos')
            df.drop(df.loc[df.grupo == 'Ambos'].index, inplace=True)

        # Apagando os que não foram escolhidos pelo pareamento
        if self.pareamento:
            if self.verbose:
                print('Dropping Não Pareados')
            pareados = self.pega_lista_de_pareados()
            df.drop(df.loc[~df.id.isin(pareados.id)
                           ].loc[df.pagtos == 0].index, inplace=True)

        self.dt = df
        print('Fim em: ', datetime.now())
        return self.dt

    def pega_lista_de_pareados(self, tabela='par_doutorandos_2014_sem_reposicao'):
        '''
        Pega lista dos ids que foram pareados e os armazena em self.ids_pareados
        '''
        sql_ids_pareados = f'''
            select pareado_1 as id from public.{tabela}
            UNION select pareado_2 as id from public.{tabela}
            UNION select pareado_3 as id from public.{tabela}
            '''
        if self.verbose:
            print(
                'Carregando indicadores dos pareados na memória. Início em: ', datetime.now())

        self.ids_pareados = pd.read_sql(sql_ids_pareados, self.db.engine())
        if self.verbose:
            print(f'pareados.size: {self.ids_pareados.size}')

        if self.verbose:
            print('Término em: ', datetime.now())
        return self.ids_pareados

    # Função que dá o tempo para terminar
    # para chamar a função:
    #   No começo da função:
    #
    #    print_time(self, feitos, número_total, tempo_início, tempo_último, atual='Nome do Indicador sendo Trabalhado')
    #    feitos += 1
    #    tempo_último = datetime.now()

    def print_time(self, feitos, número_total, tempo_início, tempo_último, atual=''):

        if not feitos == 0:
            porcentagem_já_feita = (feitos/número_total)
            print_percent = '{0:.{1}f}'.format(porcentagem_já_feita*100, 1)
            tempo_passado = datetime.now() - tempo_início
            tempo_por_id = tempo_passado / feitos
            tempo_restante = (número_total - feitos) * tempo_por_id
            tempo_em_que_vai_acabar = (
                datetime.now() + tempo_restante).strftime("%d/%m/%y %H:%M:%S")
            print(f'{feitos}/{número_total}. {print_percent}% feitos. Fazendo: {atual}. Acabará em {tempo_em_que_vai_acabar}. O Último demorou: {datetime.now() - tempo_último}.')
        else:
            print(f'{feitos}/{número_total}. Fazendo: {atual}.')

    def regressão(self,
                   dt=None,
                   indicador=None,
                   variável_dependente=None,
                   variáveis_independentes=None,
                   confiança=None,):

        if dt is None:
            dt = self.dt
        if indicador is None:
            indicador = self.indicador
        if variável_dependente is None:
            variável_dependente = self.variável_dependente
        if variáveis_independentes is None:
            variáveis_independentes = self.variáveis_independentes
        if confiança is None:
            confiança = self.confiança

        self.lista_formulas = []
        dados = self.dt.loc[self.dt.nome == self.indicador]

        if self.best_r2:
            for x in range(1, len(variáveis_independentes)+1):
                for a in combinations(variáveis_independentes, x):
                    formula = f'{variável_dependente} ~ '
                    if not a[0] == '-1':
                        formula = (formula + ' + '.join(a)
                                   ).replace('+ -1', ' -1')
                        self.lista_formulas.append(formula)
        else:
            self.lista_formulas.append(variável_dependente + ' ~ ' + ' + '.join(variáveis_independentes).replace('+ -1', ' -1'))

        if self.verbose:
            print(
                f'Lista de Fórmulas a fazer do indicador {indicador}: {self.lista_formulas}\n')

        self.list_results = []
        for formula in self.lista_formulas:
            if self.verbose:
                print('Fazendo: ', formula, '\n')
            try:
                # OLS vem de Ordinary Least Squares e o método fit irá treinar o modelo
                reg_ajustado = sm2.ols(formula, data=dados)
                reg = reg_ajustado.fit()
                # mostrando as estatísticas do modelo
                if self.verbose:
                    print(
                        f'\nSumário da Regressão {formula}:\n', reg.summary2())

                # guardando a regressão na memória

                if reg.f_pvalue < confiança:

                    result = {
                        'Indicador': self.indicador,
                        'reg': reg,
                        'Erro': False,
                        'Prob (F-statistic)': reg.f_pvalue,
                        'Parâmetros': reg.params,
                        "P>|t|": reg.pvalues,
                        'Standard Error': reg.bse,
                        "Covariância entre os pasrâmetros": reg.normalized_cov_params,
                        'Number of observations n.': reg.nobs,
                        "R2 ajustado": reg.rsquared_adj,
                        "Modelo": reg.model,
                        "Confiança": confiança,
                        "Variáveis": {
                            "variável_dependente": self.variável_dependente,
                            "variáveis_independentes": self.variáveis_independentes,
                            "confianças": self.confianças ,
                            "indicador": self.indicador,
                            "ano_início": self.ano_início,
                            "best_r2": self.best_r2,
                            "pareamento": self.pareamento,
                            "capes": self.capes,
                            "cnpq": self.cnpq,
                            "nenhum": self.nenhum,
                            "ambos": self.ambos,
                            "preenche_todos_ids": self.preenche_todos_ids,
                            "verbose": self.verbose,
                        },
                    }
                    self.list_results.append(result)
                else:
                    if self.verbose:
                        print("Regressão não significativa.")
            except:
                if self.verbose:
                    print("Erro ao realizar a regressão. Regressão ignorada.")
        try:
            maiorR = max(self.list_results, key=lambda x: x['R2 ajustado'])
            return maiorR
        except:
            return {
                'Indicador': indicador,
                'Erro': True,
                'Prob (F-statistic)': None,
                'Parâmetros': None,
                "P>|t|": None,
                'Standard Error': None,
                "Covariância entre os pasrâmetros": None,
                'Number of observations n.': None,
                "R2 ajustado": None,
                "Modelo": None,
                "Confiança": confiança,
                "Variáveis": {
                    "variável_dependente": self.variável_dependente,
                    "variáveis_independentes": self.variáveis_independentes,
                    "confianças": self.confianças ,
                    "indicador": self.indicador,
                    "ano_início": self.ano_início,
                    "best_r2": self.best_r2,
                    "pareamento": self.pareamento,
                    "capes": self.capes,
                    "cnpq": self.cnpq,
                    "nenhum": self.nenhum,
                    "ambos": self.ambos,
                    "preenche_todos_ids": self.preenche_todos_ids,
                    "verbose": self.verbose,
                },
            }

    def regressão_com_confiança(self,
                                  dt=None,
                                  indicador=None,
                                  variável_dependente=None,
                                  variáveis_independentes=None,
                                  confiança=None):

        if dt is None:
            dt = self.dt
        if indicador is None:
            indicador = self.indicador
        if variável_dependente is None:
            variável_dependente = self.variável_dependente
        if variáveis_independentes is None:
            variáveis_independentes = self.variáveis_independentes
        if confiança is None:
            confiança = self.confiança

        variáveis_independentes = list(variáveis_independentes)
        self.result = self.regressão(dt, indicador, variável_dependente, variáveis_independentes, confiança)
        if self.verbose:
            print(self.result)
        num_regressões = 0
        while self.drop_not_significant and self.result['Erro'] == False \
                and (math.isnan(self.result['P>|t|'].max())
                     or self.result['P>|t|'].max() > confiança):
            num_regressões += 1
            max_key = self.result['P>|t|'].idxmax()
            if max_key != max_key:
                if self.verbose:
                    print("P>|t|'].idxmax()) não é um número. Abortando.")
                break
            elif self.result['P>|t|'].idxmax() == 'Intercept':
                if '-1' in variáveis_independentes:
                    if self.verbose:
                        pass
                    print(
                        'Erro, regressão usou o Intercept mesmo com -1 na fórmula. Continuando.')
                    break
                else:
                    if self.verbose:
                        print("'P>|t|' é o Intercept. Removendo.")
                    variáveis_independentes.append('-1')
            elif num_regressões > 200:
                print('ERRO DE LOOP INFINITO. QUEBRANDO.')
                break
            else:
                if self.verbose:
                    print(
                        f"Removendo o 'P>|t|': {self.result['P>|t|'].idxmax()}")
                try:
                    chave = self.result['P>|t|'].idxmax()
                    variáveis_independentes.remove(chave)
                except:
                    if self.verbose:
                        pass
                    print(
                        f"Erro ao remover a chave {chave} da lista {variáveis_independentes}. Limite da regressão alcançado. Abortando. ")
                    break
            self.result = self.regressão(dt, indicador, variável_dependente, variáveis_independentes)
            if self.verbose:
                print(self.result)

        if self.result['Erro'] == False and self.result['Prob (F-statistic)'] < confiança and self.result['P>|t|'].max() < confiança:
            self.result['Confiança'] = confiança
            return self.result
        else:
            self.result['Confiança'] = 'Erro'
        return self.result

    def regressão_com_lista_confianças (self,
                                          dt=None,
                                          indicador=None,
                                          variável_dependente=None,
                                          variáveis_independentes=None,
                                          confianças = None):
        if dt is None:
            dt = self.dt
        if indicador is None:
            indicador = self.indicador
        if variável_dependente is None:
            variável_dependente = self.variável_dependente
        if variáveis_independentes is None:
            variáveis_independentes = self.variáveis_independentes
        if confianças is None:
            confianças = self.confianças

        confianças .sort()
        if self.verbose:
            print('\nLista de self.confianças: ', self.confianças )

        for confiança in confianças:
            if self.verbose:
                print('Fazendo confiança: ', confiança)
            self.result = self.regressão_com_confiança(dt, indicador, variável_dependente, variáveis_independentes, confiança)
            if not self.result['Confiança'] == 'Erro':
                break
        if self.verbose:
            print(self.result)
        return self.result

    def filtra_por_indicador(self,
                             dt=None,
                             indicador=None):

        if dt is None:
            dt = self.dt
        if indicador is None:
            indicador = self.indicador

        dados = dt.loc[dt.nome == indicador]

        if self.preenche_todos_ids == False:
            return dados

        lista_ids = dt.loc[~dt.id.isin(dados.id)].id.unique()

        feitos = 0
        tempo_início = datetime.now()
        tempo_último = datetime.now()

        try:
            indicador_tipo = dt.iloc[dt.loc[dt.nome ==
                                            indicador].first_valid_index()].indicador_tipo
        except:
            indicador_tipo = None
        try:
            indicador_grupo = dt.iloc[dt.loc[dt.nome ==
                                             indicador].first_valid_index()].indicador_grupo
        except:
            indicador_grupo = None

        if self.verbose:
            número_total = len(lista_ids)
            feitos = 0
            print('\nPreenchendo com zeros os Ids faltantes. Tamanho: ',
                  número_total)

        for id in lista_ids:
            if self.verbose:
                self.print_time(feitos, número_total, tempo_início, tempo_último, atual=id)
                feitos += 1
                tempo_último = datetime.now()

            row = {
                'id': id,
                'pagtos': 0,
                'capes': False,
                'indicador_tipo': indicador_tipo,
                'indicador_grupo': indicador_grupo,
                'indicador': self.indicador,
                'qty_2012': 0,
                'qty_2013': 0,
                'qty_2014': 0,
                'qty_2015': 0,
                'qty_2016': 0,
                'qty_2017': 0,
                'qty_2018': 0,
                'qty_2019': 0,
                'tipo': indicador_tipo,
                'nome': self.indicador,
                'grupo': 'Nenhum',
                'path': None,
                'const': 1,
                'bool_cnpq': 0,
                'bool_capes': 0,
                'porcent_pagos': 0
            }
            dados = dados.append(row, ignore_index=True)

        if self.verbose:
            print('Terminado.\n')
        return dados

    def faz_regressões(self):
        # inicializando variáveis

        feitos = 0
        tempo_início = datetime.now()
        tempo_último = datetime.now()
        print(f'Iniciado em {tempo_início}.')

        # pegando lista de todos os indicadores
        todos_indicadores = self.dt.nome.unique()

        # iterando sobre cada indicador
        número_total = len(todos_indicadores)
        if not self.faz_apenas_os_indicadores is None:
            todos_indicadores = self.faz_apenas_os_indicadores

        for self.indicador in todos_indicadores:

            # Realizando previsão de término
            self.print_time(feitos, número_total, tempo_início, tempo_último, atual=self.indicador)
            feitos += 1
            tempo_último = datetime.now()

            if self.verbose:
                print(f'\n\n\nFAZENDO INDICADOR {self.indicador.upper()}\n\n')

            dados = self.filtra_por_indicador()
            self.result = self.regressão_com_lista_confianças (dt=dados)

            self.lista_regressões.append(self.result)

        return self.lista_regressões

    def salva_regressões_csv(self, nome_arquivo=None):
        if nome_arquivo is None:
            nome_arquivo = f'Regressões variáveis_independentes {self.variáveis_independentes} confianças {self.confianças} {self.ano_início} Pareamento {self.pareamento} best_r2{self.best_r2} CNPq {self.cnpq} CAPES {self.capes} Ambos {self.ambos} Nenhum {self.nenhum} preenche_todos_ids{self.preenche_todos_ids}.xlsx'
        regressões = pd.DataFrame(self.lista_regressões)
        regressões = pd.concat([regressões,
                                regressões['Parâmetros'].apply(pd.Series),
                                regressões['P>|t|'].apply(pd.Series),
                                regressões['Standard Error'].apply(pd.Series)],
                                axis=1)
        regressões.to_excel(nome_arquivo)
