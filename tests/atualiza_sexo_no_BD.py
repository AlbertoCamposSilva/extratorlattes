import sys
import os
import time

sys.path.append(os.path.abspath('.'))
from extratorlattes.lattesweb import LattesDownloader
from extratorlattes.indicadores import Indicadores
from extratorlattes import Database

def main():
    print("Conectando ao banco de dados para buscar currículos com dados restritos ausentes (CPF e Data de Nascimento)...")
    try:
        db = Database()
        query = "SELECT id_lattes FROM lattes_dados_gerais WHERE cpf IS NULL AND data_nascimento IS NULL"
        resultados, _ = db.query(query)
        
        if not resultados:
            print("Nenhum currículo sem CPF/Data de Nascimento encontrado no banco de dados.")
            return
            
        lista_ids = [str(row[0]).zfill(16) for row in resultados]
            
    except Exception as e:
        print(f"Erro ao consultar o banco de dados: {e}")
        return

    lista_ids = list(dict.fromkeys(lista_ids))
    total = len(lista_ids)
    print(f"Total de currículos únicos sem dados restritos para atualizar: {total}\n")
    
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
            downloader.id_lattes = id_lattes
            status = downloader.get_zip_from_REST()
            
            if "sucesso" in str(status).lower():
                try:
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
            
        time.sleep(1)
            
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
                        erros_download -= 1
                    except Exception as e:
                        tqdm.write(f"[{id_lattes}] DOWNLOAD OK, MAS ERRO AO GRAVAR NO BD (Retentativa): {e}")
                        pbar_retry.set_postfix(id=id_lattes, status="ERRO BD")
                        erros_banco += 1
                        erros_download -= 1
                else:
                    tqdm.write(f"[{id_lattes}] FALHA NA RETENTATIVA: {status}")
                    pbar_retry.set_postfix(id=id_lattes, status="FALHA DWN")
            except Exception as e:
                tqdm.write(f"[{id_lattes}] ERRO FATAL NA RETENTATIVA: {e}")
                pbar_retry.set_postfix(id=id_lattes, status="ERRO FATAL")
                
            time.sleep(2)
                
    end_time = time.time()
    
    print("\n--- Resultados Finais do Processamento em Lote ---")
    print(f"Total de currículos a atualizar: {total}")
    print(f"Baixados e atualizados no BD com sucesso: {sucessos}")
    print(f"Erros definitivos de download (após retentativa): {erros_download}")
    print(f"Erros de gravação no Banco: {erros_banco}")
    print(f"Tempo total decorrido: {end_time - start_time:.2f} segundos.")

if __name__ == "__main__":
    main()
