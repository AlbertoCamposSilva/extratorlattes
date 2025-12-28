import os
# from Lattes import Lattes
# from Indicadores import Indicadores

def faz_carga(pasta):
    print('Carregando Pastas')
    caminhos = [os.path.join(pasta, nome) for nome in os.listdir(pasta)]
    print('Carergando aquivos')
    arquivos = [arq for arq in caminhos if os.path.isfile(arq)]
    print('Fazendo lista de arquivos a importar')
    zips = [arq for arq in arquivos if arq.lower().endswith(".zip")]
    número = 0
    for zip in zips:
        if os.path.basename(zip)[0:7]=='Lattes_':
            número += 1
            id = os.path.basename(zip)[7:-4]
            print(f'{número}: Recuperando dados do Currículo: {id}')
            lattes = Lattes()
            lattes.id = id

            if lattes.read_zip_from_disk():
                lattes.get_xml()
                lattes.save_json_to_disk(path='d:/Lattes_JSON/')
                lattes.save_xml_to_disk(path='d:/Lattes_XML/')
                lattes.get_indicadores()
                lattes.update_indicadores_bd()
            else:
                print('Erro ao carregar arquivo:', lattes.id)



class Carga:
    #Apenas um exemplo de como usar a função map em uma classe - desconsiderar

    def __init__(self, pasta = r'C:\Downloads'):
        self.pasta = pasta
        self.indicadores = []
        self.indicadores = list(map(self.get_indicators,self.pega_lista_zips()))


    def pega_lista_zips (self, pasta = None):
        # Pega Lista de Totos os Indicadores
        if not pasta == None:
            self.pasta = pasta
        caminhos = [os.path.join(self.pasta, nome) for nome in os.listdir(self.pasta)]
        arquivos = [arq for arq in caminhos if os.path.isfile(arq)]
        zips = [arq for arq in arquivos if arq.lower().endswith(".zip")]
        return zips

    def get_indicators (self, arq):
        print(f'calling get with arq {arq}')
        #Com a lista de indicadores, coloca todos na variável da classe Indicadores.Indicadores
        número = 0
        if os.path.basename(arq)[0:7]=='Lattes_':
            número += 1
            id = os.path.basename(arq)[7:-4]
            print(f'{número}: Recuperando dados do Currículo: {id}')
            lattes = Lattes()
            lattes.id = id
            lattes.read_zip_from_disk()
            indicadores = Indicadores(lattes.xml, id = id)
            indicadores.get_indicadores() 
            return indicadores.indicadores
