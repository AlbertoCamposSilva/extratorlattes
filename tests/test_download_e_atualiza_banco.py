import sys
import os
import shutil
import pandas as pd
import time

sys.path.append(os.path.abspath('.'))
from extratorlattes.lattesweb import LattesDownloader
from extratorlattes.indicadores import Indicadores

def main():
    excel_path = r"E:\OneDrive - CNPq\Chamadas\PCI\2026\Comitê Julgador\Demanda Bruta da Chamada 12_2026 - Programa PCI com pré-seleção.xlsx"
    temp_excel = r"tests\temp_excel.xlsx"
    
    # Faz cópia para evitar travamento por arquivo aberto no Excel
    try:
        shutil.copyfile(excel_path, temp_excel)
        excel_to_read = temp_excel
    except Exception as e:
        print(f"Aviso: não foi possível copiar o Excel ({e}), lendo diretamente...")
        excel_to_read = excel_path

    print(f"Lendo arquivo Excel: {excel_path}")
    try:
        df = pd.read_excel(excel_to_read)
    except Exception as e:
        print(f"Erro ao ler arquivo Excel: {e}")
        return

    col_name = 'link_cvlattes_proponente'
    if col_name not in df.columns:
        print(f"Coluna '{col_name}' não encontrada no Excel.")
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
    print(f"Total de currículos únicos para extrair e gravar no Banco: {total}\n")
    
    if not lista_ids:
        print("Nenhum ID válido encontrado.")
        return

    # Instanciamos LattesDownloader uma única vez (mantendo a sessão de cookies ativa)
    downloader = LattesDownloader(verbose=False)
    
    from tqdm import tqdm
    import datetime
    
    start_time = time.time()
    sucessos = 0
    erros_banco = 0
    erros_download = 0
    lista_erros_download = []
    
    print("\nIniciando processamento...")
    pbar = tqdm(lista_ids, desc="Processando", unit="cv")
    for id_lattes in pbar:
        eta_segundos = pbar.format_dict.get('eta')
        if eta_segundos:
            data_termino = (datetime.datetime.now() + datetime.timedelta(seconds=eta_segundos)).strftime("%d/%m %H:%M")
        else:
            data_termino = "Calculando..."
            
        pbar.set_postfix(id=id_lattes, status="Baixando...", termino=data_termino)
        try:
            # 1. Recupera o ZIP via REST (utiliza cookie logado de database.ini ou faz o relogin se necessário)
            downloader.id_lattes = id_lattes
            status = downloader.get_zip_from_REST()
            
            if "sucesso" in str(status).lower():
                try:
                    # 2. Instancia Indicadores com o objeto Lattes carregado e executa a atualização no BD
                    ind = Indicadores(lattes=downloader, verbose=False)
                    ind.atualiza()
                    pbar.set_postfix(id=id_lattes, status="OK BD", termino=data_termino)
                    sucessos += 1
                except Exception as e:
                    tqdm.write(f"[{id_lattes}] DOWNLOAD OK, MAS ERRO AO GRAVAR NO BD: {e}")
                    pbar.set_postfix(id=id_lattes, status="ERRO BD", termino=data_termino)
                    erros_banco += 1
            else:
                tqdm.write(f"[{id_lattes}] FALHA NO DOWNLOAD: {status}")
                pbar.set_postfix(id=id_lattes, status="FALHA DWN", termino=data_termino)
                lista_erros_download.append(id_lattes)
                erros_download += 1
        except Exception as e:
            tqdm.write(f"[{id_lattes}] ERRO FATAL NO LATTESWEB: {e}")
            pbar.set_postfix(id=id_lattes, status="ERRO FATAL")
            lista_erros_download.append(id_lattes)
            erros_download += 1
            
    if lista_erros_download:
        print(f"\n--- Iniciando retentativa para {len(lista_erros_download)} currículos que falharam no download ---")
        pbar_retry = tqdm(lista_erros_download, desc="Retentativa", unit="cv")
        for id_lattes in pbar_retry:
            pbar_retry.set_postfix(id=id_lattes, status="Baixando...")
            try:
                downloader.id_lattes = id_lattes
                status = downloader.get_zip_from_REST()
                
                if "sucesso" in str(status).lower():
                    try:
                        ind = Indicadores(lattes=downloader, verbose=False)
                        ind.atualiza()
                        pbar_retry.set_postfix(id=id_lattes, status="OK BD")
                        sucessos += 1
                        erros_download -= 1 # Remove from error count
                    except Exception as e:
                        tqdm.write(f"[{id_lattes}] DOWNLOAD OK, MAS ERRO AO GRAVAR NO BD (Retentativa): {e}")
                        pbar_retry.set_postfix(id=id_lattes, status="ERRO BD")
                        erros_banco += 1
                        erros_download -= 1 # Download succeeded but DB failed
                else:
                    tqdm.write(f"[{id_lattes}] FALHA NA RETENTATIVA: {status}")
                    pbar_retry.set_postfix(id=id_lattes, status="FALHA DWN")
            except Exception as e:
                tqdm.write(f"[{id_lattes}] ERRO FATAL NA RETENTATIVA: {e}")
                pbar_retry.set_postfix(id=id_lattes, status="ERRO FATAL")
                
    end_time = time.time()
    
    print("\n--- Resultados Finais do Processamento em Lote ---")
    print(f"Total de currículos no arquivo: {total}")
    print(f"Baixados e atualizados no BD com sucesso: {sucessos}")
    print(f"Erros definitivos de download (após retentativa): {erros_download}")
    print(f"Erros de gravação no Banco: {erros_banco}")
    print(f"Tempo total decorrido: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
