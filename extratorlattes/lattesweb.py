#%% Importações

import requests
import os
import json
from urllib.parse import urlparse, parse_qs
import time
from datetime import datetime, timedelta
from extratorlattes.lattes import Lattes
from selenium import webdriver
from selenium.webdriver.chrome.service import Service
from webdriver_manager.chrome import ChromeDriverManager
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from acs_toolbox.segredos import get_secret
from extratorlattes.sessao_cache import SessaoCache

#%% Código da classe LattesDownloader

class LattesDownloader(Lattes):
    """
    Extensão da classe Lattes para suporte a download via REST (HTTP),
    utilizada como alternativa ao serviço SOAP (WSCurriculo).
    """

    _ultima_falha_login = None
    _cooldown_login_segundos = 7200  # 2 horas de cooldown após falha

    def __init__(self, selenium_cookies=None, user_agent=None, modo_versao='fallback_publico', *args,
                 sessao_id=None, **kwargs):
        # sessao_id: identifica o cache local de cookies (um por sessão; ver extratorlattes.sessao_cache).
        # Sem sessao_id, cada instância tem cache próprio; passe o mesmo id para reaproveitar uma sessão
        # (ex.: sessao_id="interativa" após capturar_sessao_interativa()).
        # Inicializa a classe base (Lattes) com todos os parâmetros originais
        super().__init__(*args, **kwargs)
        self.modo_versao = modo_versao
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
                return min(600, backoff_value) # Limite máximo de 600 segundos

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

        # Cache local e temporário da sessão (cookies/User-Agent), válido por até 24 h
        SessaoCache.limpar_vencidos()
        self.sessao_cache = SessaoCache(sessao_id)
        if selenium_cookies is None or user_agent is None:
            cache = self.sessao_cache.carregar()
            if cache:
                if self.verbose:
                    print(f"[LattesDownloader] Reutilizando sessão em cache '{self.sessao_cache.sessao_id}'...")
                user_agent = user_agent or cache.get("user_agent")
                if selenium_cookies is None:
                    selenium_cookies = cache.get("cookies")

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

        # Credenciais para login interno silencioso (cofre de segredos, via acs_toolbox)
        if self.verbose:
            print("[LattesDownloader] Verificando credenciais de acesso interno no cofre de segredos...")
        self.intranet_login = get_secret("INTRANET-LOGIN", default=None) or get_secret("SEI_USER", default=None) or get_secret("CPF", default=None)
        self.intranet_senha = get_secret("INTRANET-SENHA", default=None) or get_secret("CNPQ", default=None)

        if self.verbose:
            if self.intranet_login and self.intranet_senha:
                print("[LattesDownloader] Credenciais INTRANET-LOGIN/SENHA carregadas com sucesso.")
            else:
                print("[LattesDownloader] Aviso: Credenciais INTRANET não encontradas no cofre de segredos (login interno ficará indisponível).")

        if selenium_cookies:
            if self.verbose:
                print(f"[LattesDownloader] Injetando {len(selenium_cookies)} cookies de sessão na instância Requests...")
            self.set_cookies_from_selenium(selenium_cookies)

    def _realizar_login_interno(self, user_id, password, max_tentativas=2):
        """Autentica no Lattes Interno via REST para liberar o download da versão completa (duas tentativas, timeout 600s)."""
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
        for tentativa in range(1, max_tentativas + 1):
            try:
                if self.verbose:
                    print(f"[Login Interno] Tentativa {tentativa}/{max_tentativas}: Acessando '{url_base}' para aquisição do cookie (JSESSIONID)...")
                self.session.get(url_base, timeout=10)

                if self.verbose:
                    print(f"[Login Interno] Tentativa {tentativa}/{max_tentativas}: Enviando autenticação para '{url_auth}' (timeout: 600s)...")
                resp = self.session.post(url_auth, json=payload, headers=headers, timeout=600)
                if resp.status_code == 200:
                    if self.verbose:
                        print("[Login Interno] Sucesso: Login interno realizado com êxito (HTTP 200). Atualizando cookies...")
                    self._salvar_cookies_na_sessao()
                    LattesDownloader._ultima_falha_login = None
                    return True
                else:
                    if self.verbose:
                        print(f"[Login Interno] Falha (Tentativa {tentativa}/{max_tentativas}): HTTP {resp.status_code}. Resposta: {resp.text[:100]}")
            except Exception as e:
                if self.verbose:
                    print(f"[Login Interno] Erro na tentativa {tentativa}/{max_tentativas}: {e}")

            if tentativa < max_tentativas:
                time.sleep(2)

        LattesDownloader._ultima_falha_login = datetime.now()
        return False

    def _salvar_cookies_na_sessao(self):
        """Salva os cookies no cache local desta sessão (até 24 h), evitando login a cada requisição."""
        try:
            cookies_list = [{'name': c.name, 'value': c.value, 'domain': c.domain, 'path': c.path}
                            for c in self.session.cookies]
            self.sessao_cache.salvar(cookies_list, self.session.headers.get("User-Agent"))
            if self.verbose:
                print(f"Cookies salvos no cache da sessão '{self.sessao_cache.sessao_id}'.")
        except Exception as e:
            if self.verbose:
                print(f"Erro ao salvar cookies no cache da sessão: {e}")

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

    def _resolver_id_interno_buscacv(self, id_16_digitos=None):
        """Tenta resolver o ID interno alfanumérico via API do BuscaCV corporativo autenticado."""
        target_id = id_16_digitos if id_16_digitos else self.id_lattes
        if not target_id:
            return None
        try:
            url_busca = "http://buscacv.cnpq.br/buscacv/rest/consultasimplificada/"
            payload = {
                "filtros": [{
                    "label": "nome",
                    "normalizador": "sufixoParcial",
                    "conteudo": str(target_id)
                }]
            }
            resp = self.session.post(url_busca, json=payload, timeout=15)
            if resp.status_code == 200:
                data = resp.json()
                if data and isinstance(data, list) and len(data) > 0:
                    docs = data[0].get("docs", [])
                    if docs and len(docs) > 0:
                        id_encontrado = docs[0].get("id") or docs[0].get("idCurriculo")
                        if id_encontrado:
                            if self.verbose:
                                print(f"[BuscaCV Interno] ID interno resolvido via busca simplificada: {id_encontrado}")
                            return id_encontrado
        except Exception as e:
            if self.verbose:
                print(f"[BuscaCV Interno] Falha na busca simplificada por {target_id}: {e}")
        return None

    def get_zip_from_REST(self, set_auto_save=None, path=None, modo_versao=None):
        """
        Acessa o serviço REST do CNPq e baixa o currículo compactado.
        Estrutura espelhada do método get_zip_from_SOAP.
        Suporta 3 modos: 'fallback_publico' (padrão), 'publico' e 'exigir_restrito'.
        """
        if modo_versao is None:
            modo_versao = getattr(self, 'modo_versao', 'fallback_publico')
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

        # Se não obteve o ID alfanumérico (ex: CV restrito/oculto) e o modo permite versão restrita, tenta autenticar antes
        if not id_interno and modo_versao in ['fallback_publico', 'exigir_restrito']:
            agora = datetime.now()
            em_cooldown = False
            if LattesDownloader._ultima_falha_login is not None:
                tempo_decorrido = (agora - LattesDownloader._ultima_falha_login).total_seconds()
                if tempo_decorrido < LattesDownloader._cooldown_login_segundos:
                    em_cooldown = True
                    minutos_restantes = int((LattesDownloader._cooldown_login_segundos - tempo_decorrido) / 60)
                    if self.verbose:
                        print(f"[Download REST] Currículo restrito/oculto detectado, mas login em cooldown (~{minutos_restantes} min restantes).")

            if not em_cooldown:
                if self.intranet_login and self.intranet_senha:
                    print(f"  [LATTES INTERNO] CV restrito/oculto (ID: {self.id_lattes}). Tentando login interno...")
                    sucesso_login = self._realizar_login_interno(self.intranet_login, self.intranet_senha, max_tentativas=2)
                    if sucesso_login:
                        print("  [LATTES INTERNO] Login interno realizado com sucesso (HTTP 200).")
                        id_interno = self.get_id_interno()
                        if not id_interno:
                            id_interno = self._resolver_id_interno_buscacv(self.id_lattes)
                else:
                    if self.verbose:
                        print("[Download REST] Aviso: Credenciais INTRANET não encontradas para tentar login em CV restrito.")

        if id_interno:
            url_download = f"http://buscacv.cnpq.br/buscacv/rest/download/curriculo/{id_interno}"
            if self.verbose:
                print(f"[Download REST] Etapa 2/4: Baixando arquivo ZIP compactado a partir da API REST ({url_download})...")
            try:
                response = self.session.get(url_download, timeout=600)
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
                xml_publico = ('CPF=' not in self.xml and 'DATA-NASCIMENTO=' not in self.xml)

                if xml_publico:
                    if modo_versao == 'publico':
                        if self.verbose:
                            print("[Download REST] Versão pública identificada e aceita diretamente (modo='publico').")

                    elif modo_versao == 'fallback_publico':
                        # Modo 2 (Padrão): Tenta autenticar 2 vezes se não estiver no cooldown de 2 horas
                        agora = datetime.now()
                        em_cooldown = False
                        if LattesDownloader._ultima_falha_login is not None:
                            tempo_decorrido = (agora - LattesDownloader._ultima_falha_login).total_seconds()
                            if tempo_decorrido < LattesDownloader._cooldown_login_segundos:
                                em_cooldown = True
                                minutos_restantes = int((LattesDownloader._cooldown_login_segundos - tempo_decorrido) / 60)
                                if self.verbose:
                                    print(f"[Download REST] Versão pública aceita. Cooldown de autenticação ativo (~{minutos_restantes} min restantes).")

                        if not em_cooldown:
                            if self.intranet_login and self.intranet_senha:
                                if self.verbose:
                                    print("[Download REST] Versão pública detectada. Tentando autenticação interna (2 tentativas, timeout 10 min)...")
                                sucesso_login = self._realizar_login_interno(self.intranet_login, self.intranet_senha, max_tentativas=2)
                                if sucesso_login:
                                    if self.verbose:
                                        print('[Download REST] Re-executando o download via REST após autenticação interna...')
                                    try:
                                        response = self.session.get(url_download, timeout=600)
                                        if response.status_code == 200:
                                            self.zip = response.content
                                            self.get_xml()
                                    except Exception:
                                        pass
                                else:
                                    if self.verbose:
                                        print("[Download REST] Autenticação falhou após 2 tentativas. Prosseguindo com versão pública (cooldown de 2 horas ativado).")
                            else:
                                if self.verbose:
                                    print("[Download REST] Credenciais INTRANET não encontradas. Prosseguindo com versão pública.")

                    elif modo_versao == 'exigir_restrito':
                        # Modo 3: Comportamento restrito estrito (aborta se não conseguir autenticar)
                        if self.intranet_login and self.intranet_senha:
                            sucesso_login = self._realizar_login_interno(self.intranet_login, self.intranet_senha, max_tentativas=2)
                            if sucesso_login:
                                if self.verbose:
                                    print('[Download REST] Re-executando o download via REST após autenticação interna...')
                                try:
                                    response = self.session.get(url_download, timeout=600)
                                    if response.status_code == 200:
                                        self.zip = response.content
                                        if not self.get_xml():
                                            self.zip = None
                                            raise RuntimeError("Erro desconhecido ao decodificar/re-validar o XML recuperado via REST.")
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
                            raise RuntimeError("Erro: Credenciais INTRANET-LOGIN/SENHA não encontradas no cofre de segredos. Download abortado.")

                if self.verbose:
                    print('[Download REST] Validação concluída com sucesso.')

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
    def capturar_sessao_interativa(sessao_id="interativa"):
        """
        Abre o navegador para login manual e captura os cookies após a conclusão.

        Os cookies ficam no cache local da sessão ``sessao_id`` (até 24 h). Para usá-los:
        ``LattesDownloader(..., sessao_id="interativa")``.
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

            # Salva os cookies e o user_agent no cache local da sessão
            cache = SessaoCache(sessao_id)
            cache.salvar(cookies, user_agent)
            print(f"Sucesso: {len(cookies)} cookies capturados na sessão '{cache.sessao_id}' (válida por até 24 h).")
            return cookies, user_agent

        finally:
            driver.quit()