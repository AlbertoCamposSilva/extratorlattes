"""
Banco de dados do extratorlattes.

Estende a classe genérica ``acs_toolbox.database.Database`` com as operações
específicas do domínio Lattes (log de processamento, consultas ao JSON e
criação das tabelas ``lattes_*``).
"""
import pandas as pd

from acs_toolbox.database import Database

from extratorlattes.schema import constroi_tabelas


class LattesDatabase(Database):

    def constroi_tabelas(self, drop_if_exists=False):
        """Cria as tabelas do extratorlattes (ver ``extratorlattes.schema``)."""
        return constroi_tabelas(self, drop_if_exists=drop_if_exists)

    def registrar_log(self, id_lattes, status):
        """
        Insere um registro de log com o ID do Lattes, o Timestamp atual e o status.
        """
        sql = """
            INSERT INTO lattes_log_processamento (id_lattes, data_hora, status)
            VALUES (%s, NOW(), %s)
        """
        # Converte para string para garantir compatibilidade com text
        return self.execute(sql, (str(id_lattes), status,))

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
        sql = "SELECT json #>> %s FROM lattes_json WHERE id_lattes = %s"

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
            WHERE id_lattes = %s
        """

        rows, colnames = self.query(sql, (str(id_lattes),))
        return pd.DataFrame(rows, columns=colnames) if rows else pd.DataFrame()
