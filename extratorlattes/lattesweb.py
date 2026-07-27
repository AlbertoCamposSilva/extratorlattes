#%% Importações

import requests
import os
import json
from configparser import ConfigParser
from urllib.parse import urlparse, parse_qs
from extratorlattes.lattes import Lattes
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

    def __init__(self, selenium_cookies=None, user_agent=None, *args, **kwargs):
        # Inicializa a classe base (Lattes) com todos os parâmetros originais
        super().__init__(*args, **kwargs)
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
        
        if self.verbose:
            print("[LattesDownloader] Inicializando sessão HTTP e configurando política de retentativas (PowerOfFourRetry)...")

        # Carrega dados do arquivo INI automaticamente se os parâmetros não forem fornecidos
        config_file = r'e:/Python/database.ini'
        if (selenium_cookies is None or user_agent is None) and os.path.exists(config_file):
            if self.verbose:
                print(f"[LattesDownloader] Carregando User-Agent e cookies de sessão a partir do arquivo '{config_file}'...")
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
            if self.verbose:
                print(f"[LattesDownloader] Configurando User-Agent personalizado: {user_agent[:45]}...")
            self.session.headers.update({"User-Agent": user_agent})
        else:
            if self.verbose:
                print("[LattesDownloader] Configurando User-Agent padrão (Mozilla/5.0...)...")
            self.session.headers.update({
                "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36"
            })

        self.session.headers.update({
            "Accept": "application/json, text/plain, */*",
            "Referer": "http://buscacv.cnpq.br/buscacv/"
        })

        # Tenta carregar as credenciais do .env para login interno silencioso
        env_path = r"E:\OneDrive - CNPq\Python\.env"
        if self.verbose:
            print(f"[LattesDownloader] Verificando credenciais de acesso interno no arquivo '{env_path}'...")
        load_dotenv(env_path)
        self.intranet_login = os.getenv("INTRANET-LOGIN")
        self.intranet_senha = os.getenv("INTRANET-SENHA")

        if self.verbose:
            if self.intranet_login and self.intranet_senha:
                print("[LattesDownloader] Credenciais INTRANET-LOGIN/SENHA carregadas com sucesso.")
            else:
                print("[LattesDownloader] Aviso: Credenciais INTRANET não encontradas no .env (login interno ficará indisponível).")

        if selenium_cookies:
            if self.verbose:
                print(f"[LattesDownloader] Injetando {len(selenium_cookies)} cookies de sessão na instância Requests...")
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
            if self.verbose:
                print(f"[Login Interno] Etapa 1/2: Acessando '{url_base}' para aquisição do cookie de sessão (JSESSIONID)...")
            self.session.get(url_base, timeout=10)
            
            # 2. Envia o POST de autenticação (O servidor pode demorar até 2 minutos para responder)
            if self.verbose:
                print(f"[Login Interno] Etapa 2/2: Enviando requisição POST de autenticação para '{url_auth}' (pode levar até 2 min)...")
            resp = self.session.post(url_auth, json=payload, headers=headers, timeout=300)
            if resp.status_code == 200:
                if self.verbose:
                    print("[Login Interno] Sucesso: Login interno realizado com êxito (HTTP 200). Atualizando cookies...")
                self._salvar_cookies_no_ini()
                return True
            else:
                if self.verbose:
                    print(f"[Login Interno] Falha: O login retornou status HTTP {resp.status_code}. Resposta: {resp.text[:100]}")
                return False
        except Exception as e:
            if self.verbose:
                print(f"[Login Interno] Erro de conexão durante a tentativa de login interno: {e}")
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
            if self.verbose:
                print("[ID Interno] Aviso: Nenhum ID de 16 dígitos informado para conversão.")
            return None

        url_curta = f"http://lattes.cnpq.br/{target_id}"
        if self.verbose:
            print(f"[ID Interno] Etapa 1/2: Solicitando redirecionamento para resolver o ID alfanumérico em '{url_curta}'...")
        try:
            # stream=True para ler apenas o cabeçalho de redirecionamento
            response = self.session.get(url_curta, allow_redirects=True, timeout=10, stream=True)
            url_final = response.url
            if self.verbose:
                print(f"[ID Interno] Etapa 2/2: Redirecionado com sucesso. URL de destino: '{url_final}'")
            query = parse_qs(urlparse(url_final).query)
            id_interno = query.get('id', [None])[0]
            if self.verbose:
                if id_interno:
                    print(f"[ID Interno] ID alfanumérico interno resolvido com êxito: {id_interno}")
                else:
                    print("[ID Interno] Aviso: Parâmetro 'id' ausente na URL (currículo possivelmente inexistente ou oculto).")
            return id_interno
        except Exception as e:
            if self.verbose:
                print(f"[ID Interno] Erro ao tentar resolver ID interno para {target_id}: {e}")
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
            print(f"\n[Download REST] === Iniciando extração do currículo Lattes {self.id_lattes} ===")
            print(f"[Download REST] Etapa 1/4: Obtendo ID alfanumérico interno...")

        # 1. Obter o ID interno (K...) necessário para o endpoint REST
        id_interno = self.get_id_interno()

        if id_interno:
            url_download = f"http://buscacv.cnpq.br/buscacv/rest/download/curriculo/{id_interno}"
            if self.verbose:
                print(f"[Download REST] Etapa 2/4: Baixando arquivo ZIP compactado a partir da API REST ({url_download})...")
            try:
                response = self.session.get(url_download, timeout=120)
                if response.status_code == 200:
                    self.zip = response.content
                    if self.verbose:
                        print(f"[Download REST] Download do ZIP concluído com sucesso ({len(self.zip)} bytes recebidos).")
                else:
                    self.ocorrencia = f"Erro HTTP {response.status_code} no download REST."
                    if self.verbose:
                        print(f"[Download REST] Erro HTTP {response.status_code} ao tentar baixar o arquivo ZIP.")
            except Exception as e:
                self.ocorrencia = f"Falha na conexão REST: {str(e)}"
                if self.verbose:
                    print(f"[Download REST] Falha de conexão: {self.ocorrencia}")
        else:
            self.ocorrencia = "Não foi possível converter o ID de 16 dígitos para o ID interno."
            if self.verbose:
                print(f"[Download REST] Erro: {self.ocorrencia}")

        # 2. Validação e Salvamento (Mantendo a lógica original da classe Lattes)
        if self.zip is not None:
            if self.verbose:
                print('[Download REST] Etapa 3/4: Descompactando arquivo ZIP e validando estrutura do XML...')
            
            # get_xml() é herdado da classe base
            if self.get_xml():
                
                # Verifica se o XML recuperado é a versão restrita, checando pela presença de atributos restritos que não falham
                if 'CPF=' not in self.xml and 'DATA-NASCIMENTO=' not in self.xml:
                    if self.verbose:
                        print("[Download REST] Atenção: XML retornado corresponde à versão pública (ausência de CPF e DATA-NASCIMENTO).")
                        print("[Download REST] Tentando login interno para liberação do acesso à versão restrita...")
                    if self.intranet_login and self.intranet_senha:
                        sucesso_login = self._realizar_login_interno(self.intranet_login, self.intranet_senha)
                        if sucesso_login:
                            if self.verbose:
                                print('[Download REST] Re-executando o download via REST após a autenticação interna...')
                            try:
                                response = self.session.get(url_download, timeout=120)
                                if response.status_code == 200:
                                    self.zip = response.content
                                    if self.verbose:
                                        print(f"[Download REST] Re-download do ZIP concluído com sucesso ({len(self.zip)} bytes).")
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
                    print('[Download REST] Validação concluída com sucesso: XML restrito/válido identificado.')
                
                self.ocorrencia = "Curriculo recuperado com sucesso!"
                
                if set_auto_save:
                    if self.verbose:
                        print(f'[Download REST] Etapa 4/4: Salvando arquivo ZIP compactado no disco ({path})...')
                    # save_zip_to_disk() é herdado da classe base
                    salvou_no_disco = self.save_zip_to_disk(path)
                    if salvou_no_disco:
                        if self.verbose:
                            print('[Download REST] Arquivo ZIP gravado em disco com sucesso!')
                    else:
                        self.ocorrencia = "Curriculo recuperado com sucesso, mas houve falha ao salvar no disco."
                        if self.verbose:
                            print('[Download REST] Aviso: O download foi bem-sucedido, mas falhou ao gravar no disco.')
                else:
                    if self.verbose:
                        print('[Download REST] Etapa 4/4: Salvamento automático em disco desabilitado (set_auto_save=False).')
            else:
                if self.verbose:
                    print('[Download REST] Erro na validação: Falha ao extrair e converter o XML do ZIP recuperado.')
                
                # Tratamento de erro herdado
                if self.ocorrencia is None:
                    self.ocorrencia = 'Erro desconhecido ao validar o XML recuperado via REST.'
                
                self.zip = None
                self.json = None
                self.xml = None
                
                if self.verbose:
                    print(f'        {self.ocorrencia}')
        
        return self.ocorrencia

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