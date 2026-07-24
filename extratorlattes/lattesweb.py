#%% Importações

import requests
import os
import json
from configparser import ConfigParser
from urllib.parse import urlparse, parse_qs
from concurrent.futures import ThreadPoolExecutor
from extratorlattes.lattes import Lattes
import time
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from dotenv import load_dotenv
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry

#%% Código da classe LattesDownloader

class LattesDownloader(Lattes):
    """
    Extensão da classe Lattes para suporte a download via REST (HTTP),
    utilizada como alternativa ao serviço SOAP (WSCurriculo).
    """

    def __init__(self, max_workers=None, selenium_cookies=None, user_agent=None, *args, **kwargs):
        # Inicializa a classe base (Lattes) com todos os parâmetros originais
        super().__init__(*args, **kwargs)
        self.max_workers = kwargs.get('max_workers', 5)  # Configuração para download em lote
        self.session = requests.Session()
        
# Configura retentativas customizadas com base 4 (4s, 16s, 64s)
        class PowerOfFourRetry(Retry):
            def get_backoff_time(self):
                consecutive_errors_len = len(self.history)
                if consecutive_errors_len == 0:
                    return 0
                
                # Para consecutive_errors_len = 1 (primeira falha/1ª retentativa), será 4**1 = 4s
                # Para consecutive_errors_len = 2 (segunda falha/2ª retentativa), será 4**2 = 16s
                # Para consecutive_errors_len = 3 (terceira falha/3ª retentativa), será 4**3 = 64s
                backoff_value = 4 ** consecutive_errors_len
                return min(120, backoff_value) # Limite máximo de 120 segundos

        retry_strategy = PowerOfFourRetry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["HEAD", "GET", "OPTIONS", "POST"]
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        self.session.mount("http://", adapter)
        self.session.mount("https://", adapter)
        
        # Carrega dados do arquivo INI automaticamente se os parâmetros não forem fornecidos
        config_file = r'e:/Python/database.ini'
        if (selenium_cookies is None or user_agent is None) and os.path.exists(config_file):
            parser = ConfigParser()
            parser.read(config_file, encoding='utf-8')
            if parser.has_section('lattesweb'):
                if user_agent is None and parser.has_option('lattesweb', 'user_agent'):
                    user_agent = parser.get('lattesweb', 'user_agent')
                if selenium_cookies is None and parser.has_option('lattesweb', 'cookies'):
                    try:
                        selenium_cookies = json.loads(parser.get('lattesweb', 'cookies'))
                    except json.JSONDecodeError:
                        pass

        # Define o User-Agent idêntico ao do navegador que fez o login
        if user_agent:
            self.session.headers.update({"User-Agent": user_agent})
        else:
            self.session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })

        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Referer": "http://buscacv.cnpq.br/buscacv/"
        })

        # Tenta carregar as credenciais do .env para login interno silencioso
        env_path = r"E:\OneDrive - CNPq\Python\.env"
        load_dotenv(env_path)
        self.intranet_login = os.getenv("INTRANET-LOGIN")
        self.intranet_senha = os.getenv("INTRANET-SENHA")

        if selenium_cookies:
            self.set_cookies_from_selenium(selenium_cookies)

    def _realizar_login_interno(self, user_id, password):
        """Autentica no Lattes Interno via REST para liberar o download da versão completa."""
        url_base = "http://buscacv.cnpq.br/buscacv/"
        url_auth = "http://buscacv.cnpq.br/buscacv/rest/authenticate"
        payload = {"userId": user_id, "password": password}
        headers = {
            "Accept": "application/json, text/plain, */*",
            "Content-Type": "application/json;charset=UTF-8",
            "Referer": "http://buscacv.cnpq.br/buscacv/login.html",
            "Origin": "http://buscacv.cnpq.br",
            "X-Requested-With": "XMLHttpRequest"
        }
        try:
            # 1. Faz um GET inicial para adquirir os cookies de sessão (JSESSIONID)
            self.session.get(url_base, timeout=10)
            
            # 2. Envia o POST de autenticação (O servidor pode demorar até 2 minutos para responder)
            resp = self.session.post(url_auth, json=payload, headers=headers, timeout=300)
            if resp.status_code == 200:
                if self.verbose:
                    print("Login interno automático realizado com sucesso via HTTP POST.")
                self._salvar_cookies_no_ini()
                return True
            else:
                if self.verbose:
                    print(f"Aviso: O login interno retornou status {resp.status_code}. Resposta: {resp.text[:100]}")
                return False
        except Exception as e:
            if self.verbose:
                print(f"Erro ao tentar realizar login interno automático: {e}")
            return False

    def _salvar_cookies_no_ini(self):
        """Salva os cookies de sessão no database.ini para uso futuro, evitando login a cada requisição."""
        config_file = r'e:/Python/database.ini'
        if os.path.exists(config_file):
            try:
                parser = ConfigParser()
                parser.read(config_file, encoding='utf-8')
                if not parser.has_section('lattesweb'):
                    parser.add_section('lattesweb')
                
                cookies_list = []
                for c in self.session.cookies:
                    cookies_list.append({
                        'name': c.name,
                        'value': c.value,
                        'domain': c.domain,
                        'path': c.path
                    })
                
                parser.set('lattesweb', 'cookies', json.dumps(cookies_list))
                with open(config_file, 'w', encoding='utf-8') as f:
                    parser.write(f)
                if self.verbose:
                    print("Cookies salvos com sucesso no database.ini.")
            except Exception as e:
                if self.verbose:
                    print(f"Erro ao salvar cookies no database.ini: {e}")

    def set_cookies_from_selenium(self, cookies_list):
        """Converte o formato de cookies do Selenium para o Requests."""
        for cookie in cookies_list:
            self.session.cookies.set(
                name=cookie['name'],
                value=cookie['value'],
                domain=cookie.get('domain', '.cnpq.br'),
                path=cookie.get('path', '/')
            )
    
    def get_id_interno(self, id_16_digitos=None):
        """
        Recupera o ID alfanumérico interno (id_sistema) via redirecionador.
        """
        target_id = id_16_digitos if id_16_digitos else self.id_lattes
        if not target_id:
            return None

        url_curta = f"http://lattes.cnpq.br/{target_id}"
        try:
            # stream=True para ler apenas o cabeçalho de redirecionamento
            response = self.session.get(url_curta, allow_redirects=True, timeout=10, stream=True)
            url_final = response.url
            query = parse_qs(urlparse(url_final).query)
            return query.get('id', [None])[0]
        except Exception as e:
            if self.verbose:
                print(f"Erro ao resolver ID interno para {target_id}: {e}")
            return None

    def get_zip_from_REST(self, set_auto_save=None, path=None):
        """
        Acessa o serviço REST do CNPq e baixa o currículo compactado.
        Estrutura espelhada do método get_zip_from_SOAP.
        """
        if path is None:
            path = self.path

        if set_auto_save is None:
            set_auto_save = self.auto_save_zip_to_disk
        
        self.ocorrencia = None
        self.zip = None

        if self.verbose:
            print(f'Pegando Lattes compactado {self.id_lattes} via REST (HTTP)')

        # 1. Obter o ID interno (K...) necessário para o endpoint REST
        id_interno = self.get_id_interno()

        if id_interno:
            url_download = f"http://buscacv.cnpq.br/buscacv/rest/download/curriculo/{id_interno}"
            try:
                response = self.session.get(url_download, timeout=120)
                if response.status_code == 200:
                    self.zip = response.content
                else:
                    self.ocorrencia = f"Erro HTTP {response.status_code} no download REST."
            except Exception as e:
                self.ocorrencia = f"Falha na conexão REST: {str(e)}"
        else:
            self.ocorrencia = "Não foi possível converter o ID de 16 dígitos para o ID interno."

        # 2. Validação e Salvamento (Mantendo a lógica original da classe Lattes)
        if self.zip is not None:
            if self.verbose:
                print('    Lattes compactado recuperado via REST. Verificando validade dos dados.')
            
            # get_xml() é herdado da classe base
            if self.get_xml():
                
                # Verifica se o XML recuperado é a versão restrita, checando pela presença de atributos restritos que não falham
                if 'CPF=' not in self.xml and 'DATA-NASCIMENTO=' not in self.xml:
                    if self.verbose:
                        print("    Atenção: O XML retornado parece ser a versão pública (sem CPF). Tentando login interno...")
                    if self.intranet_login and self.intranet_senha:
                        sucesso_login = self._realizar_login_interno(self.intranet_login, self.intranet_senha)
                        if sucesso_login:
                            if self.verbose:
                                print('    Baixando novamente o Lattes via REST após o login...')
                            try:
                                response = self.session.get(url_download, timeout=120)
                                if response.status_code == 200:
                                    self.zip = response.content
                                    if not self.get_xml():
                                        self.zip = None
                                        raise RuntimeError("Erro desconhecido ao decodificar/re-validar o XML recuperado via REST.")
                                        
                                    # Checa novamente se o XML restrito veio com sucesso
                                    if 'CPF=' not in self.xml and 'DATA-NASCIMENTO=' not in self.xml:
                                        self.zip = None
                                        raise RuntimeError("Erro: Login reportou sucesso, mas o Lattes baixado continua sendo a versão pública (sem CPF/DATA-NASCIMENTO).")
                                else:
                                    self.ocorrencia = f"Erro HTTP {response.status_code} no re-download REST."
                                    self.zip = None
                                    return self.ocorrencia
                            except Exception as e:
                                if isinstance(e, RuntimeError):
                                    raise e
                                self.ocorrencia = f"Falha na conexão REST durante o re-download: {str(e)}"
                                self.zip = None
                                return self.ocorrencia
                        else:
                            self.zip = None
                            raise RuntimeError("Erro: Falha no login interno. Download abortado para evitar sobrescrita com a versão pública.")
                    else:
                        self.zip = None
                        raise RuntimeError("Erro: Credenciais INTRANET-LOGIN/SENHA não encontradas no .env. Download abortado.")
                
                if self.verbose:
                    print('    Lattes compactado válido.')
                
                self.ocorrencia = "Curriculo recuperado com sucesso!"
                
                if set_auto_save:
                    # save_zip_to_disk() é herdado da classe base
                    salvou_no_disco = self.save_zip_to_disk(path)
                    if salvou_no_disco:
                        if self.verbose:
                            print('Lattes salvo no disco.')
                    else:
                        self.ocorrencia = "Curriculo recuperado com sucesso, mas houve falha ao salvar no disco."
                        if self.verbose:
                            print('Lattes não salvo no disco.')
            else:
                if self.verbose:
                    print(f'    Lattes compactado inválido.')
                
                # Tratamento de erro herdado
                if self.ocorrencia is None:
                    self.ocorrencia = 'Erro desconhecido ao validar o XML recuperado via REST.'
                
                self.zip = None
                self.json = None
                self.xml = None
                
                if self.verbose:
                    print(f'        {self.ocorrencia}')
        
        return self.ocorrencia

    def baixar_lattes_lote(self, lista_ids, max_workers):
        """
        Realiza o download de múltiplos currículos em lote (16 dígitos -> XML).
        Retorna um dicionário com o status de cada ID.
        """
        if self.verbose:
            print(f"Iniciando download em lote de {len(lista_ids)} currículos.")

        # 1. Mapeamento paralelo de IDs de 16 dígitos para IDs internos (K...)
        mapa_ids = self.get_ids_internos_lote(lista_ids, max_workers)
        
        llm_resultados = {}
        # Recupera cookies da instância atual para replicar nas threads
        cookies_atuais = self.session.cookies.get_dict()

        # 2. Função auxiliar para processar o download individual dentro da thread
        def executar_download(id_16):
            id_interno = mapa_ids.get(id_16)
            if not id_interno:
                return id_16, "Falha: ID interno não localizado"
            
            # Criamos uma instância temporária para evitar conflitos de estado (self.id_lattes) 
            # entre as threads, herdando as configurações da instância atual.
            instancia_temp = LattesDownloader(
                id_lattes=id_16, 
                path=self.path, 
                verbose=self.verbose,
                max_workers=self.max_workers,
            )
            # Injetamos a sessão para aproveitar a conexão
            instancia_temp.session = self.session 
            
            status = instancia_temp.get_zip_from_REST()
            return id_16, status

        # 3. Execução do download em lote
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            futuros = [executor.submit(executar_download, i) for i in lista_ids]
            for f in futuros:
                id_orig, status = f.result()
                llm_resultados[id_orig] = status

        return llm_resultados

    def get_ids_internos_lote(self, lista_ids, max_workers=10):
        """
        Resolve uma lista de IDs de 16 dígitos para IDs internos em paralelo.
        """
        llm_resultados = {}
        with ThreadPoolExecutor(max_workers=max_workers) as executor:
            # Mapeia a função get_id_interno para cada item da lista
            futuros = {executor.submit(self.get_id_interno, i): i for i in lista_ids}
            for f in futuros:
                id_orig = futuros[f]
                llm_resultados[id_orig] = f.result()
        return llm_resultados

    @staticmethod
    def capturar_sessao_interativa(config_file=r'e:/Python/database.ini'):
        """
        Abre o navegador para login manual e captura os cookies após a conclusão.
        """
        print("--- INICIANDO CAPTURA DE SESSÃO ---")
        driver = webdriver.Chrome(service=Service(ChromeDriverManager().install()))
        
        try:
            # Navega para o portal
            driver.get("http://buscacv.cnpq.br/buscacv/")
            
            print("\nINSTRUÇÕES:")
            print("1. Realize o login manualmente no navegador que abriu.")
            print("2. Certifique-se de que você está na tela de busca ou visualizando um currículo.")
            print("3. Volte aqui no terminal e pressione ENTER.")
            
            input("\n[Pressione ENTER após concluir o login no navegador...]")
            
            # Captura os cookies e o User-Agent
            cookies = driver.get_cookies()
            user_agent = driver.execute_script("return navigator.userAgent")
            
            # Salva os cookies e user_agent no arquivo de configuração
            parser = ConfigParser()
            if os.path.exists(config_file):
                parser.read(config_file, encoding='utf-8')
                
            if not parser.has_section('lattesweb'):
                parser.add_section('lattesweb')
                
            parser.set('lattesweb', 'user_agent', user_agent)
            parser.set('lattesweb', 'cookies', json.dumps(cookies))
            with open(config_file, 'w', encoding='utf-8') as f:
                parser.write(f)
                
            print(f"Sucesso: {len(cookies)} cookies capturados e salvos em {config_file}.")
            return cookies, user_agent

        finally:
            driver.quit()