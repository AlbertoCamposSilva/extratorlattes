import sys
import os
import pandas as pd
import time

sys.path.append(os.path.abspath('.'))
from extratorlattes.lattesweb import LattesDownloader

def main():
    excel_path = r"tests\temp_excel.xlsx"
    
    print(f"Lendo arquivo Excel: {excel_path}")
    try:
        df = pd.read_excel(excel_path)
    except Exception as e:
        print(f"Erro ao ler arquivo Excel: {e}")
        return

    col_name = 'link_cvlattes_proponente'
    if col_name not in df.columns:
        print(f"Coluna {col_name} não encontrada no Excel.")
        return

    lista_ids = []
    for link in df[col_name].dropna():
        link_str = str(link).strip()
        id_str = link_str.split('/')[-1]
        id_str = ''.join(filter(str.isdigit, id_str)).zfill(16)
        
        if len(id_str) == 16:
            lista_ids.append(id_str)

    lista_ids = list(dict.fromkeys(lista_ids))
    total = len(lista_ids)
    print(f"Total de currículos únicos para baixar: {total}\n")
    
    if not lista_ids:
        print("Nenhum ID válido encontrado.")
        return

    # Instanciamos LattesDownloader uma única vez (sem verbose excessivo)
    downloader = LattesDownloader(verbose=False)
    
    start_time = time.time()
    sucessos = 0
    
    for idx, id_lattes in enumerate(lista_ids, 1):
        print(f"[{idx}/{total}] Baixando {id_lattes}...", end='', flush=True)
        downloader.id_lattes = id_lattes
        
        status = downloader.get_zip_from_REST()
        
        if "sucesso" in str(status).lower():
            print(" [OK]")
            sucessos += 1
        else:
            print(f" [FALHA: {status}]")
            
    end_time = time.time()
    
    print("\n--- Resultados Finais ---")
    print(f"Resumo: {sucessos} currículos baixados com sucesso de {total} totais.")
    print(f"Tempo total: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
