import sys
import os

sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), '..')))

from extratorlattes.lattesweb import LattesDownloader
from extratorlattes.indicadores import Indicadores

def main():
    id_lattes = "8721994758453503"
    print(f"Baixando e atualizando CV {id_lattes}...")
    lattes = LattesDownloader(id_lattes=id_lattes, verbose=True)
    res_zip = lattes.get_zip_from_REST()
    print("Resultado do download:", res_zip)
    
    ind = Indicadores(lattes=lattes)
    res_ind = ind.atualiza()
    print("Resultado da atualizacao:", res_ind)

if __name__ == "__main__":
    main()
