"""
Cache local e temporário da sessão HTTP do Lattes (cookies e User-Agent).

- Um arquivo por sessão, em ``<pasta temporária>/extratorlattes/sessoes/<sessao_id>.json``:
  threads/processos com ``sessao_id`` diferentes nunca compartilham cookies.
- Validade máxima de 24 horas: caches mais antigos são ignorados e apagados.
- Escrita atômica (arquivo temporário + ``os.replace``), para que uma leitura
  concorrente nunca encontre um arquivo pela metade.

Não guarda senhas: só os cookies da sessão já autenticada.
"""
import json
import os
import re
import tempfile
import time
import uuid

VALIDADE_SEGUNDOS = 24 * 60 * 60
PASTA = os.path.join(tempfile.gettempdir(), "extratorlattes", "sessoes")


def _nome_seguro(sessao_id):
    return re.sub(r"[^A-Za-z0-9_.-]", "_", str(sessao_id))[:100] or "sessao"


class SessaoCache:
    def __init__(self, sessao_id=None, pasta=PASTA, validade_segundos=VALIDADE_SEGUNDOS):
        self.sessao_id = _nome_seguro(sessao_id) if sessao_id else uuid.uuid4().hex
        self.pasta = pasta
        self.validade_segundos = validade_segundos
        self.caminho = os.path.join(pasta, f"{self.sessao_id}.json")

    def carregar(self):
        """Devolve ``{'user_agent': ..., 'cookies': [...]}`` ou None (inexistente, inválido ou vencido)."""
        try:
            with open(self.caminho, encoding="utf-8") as f:
                dados = json.load(f)
        except (OSError, ValueError):
            return None
        if time.time() - float(dados.get("criado_em", 0)) > self.validade_segundos:
            self.apagar()
            return None
        return dados

    def salvar(self, cookies, user_agent=None):
        os.makedirs(self.pasta, exist_ok=True)
        dados = {"criado_em": time.time(), "user_agent": user_agent, "cookies": cookies}
        fd, tmp = tempfile.mkstemp(dir=self.pasta, prefix=f".{self.sessao_id}.", suffix=".tmp")
        try:
            with os.fdopen(fd, "w", encoding="utf-8") as f:
                json.dump(dados, f)
            # No Windows, a troca falha enquanto outra thread lê o arquivo: tenta de novo por até ~2 s.
            for tentativa in range(100):
                try:
                    os.replace(tmp, self.caminho)
                    break
                except PermissionError:
                    if tentativa == 99:
                        raise
                    time.sleep(0.02)
        except BaseException:
            try:
                os.remove(tmp)
            except OSError:
                pass
            raise

    def apagar(self):
        try:
            os.remove(self.caminho)
        except OSError:
            pass

    @staticmethod
    def limpar_vencidos(pasta=PASTA, validade_segundos=VALIDADE_SEGUNDOS):
        """Apaga caches (e temporários órfãos) com mais de ``validade_segundos``."""
        try:
            nomes = os.listdir(pasta)
        except OSError:
            return 0
        agora, apagados = time.time(), 0
        for nome in nomes:
            caminho = os.path.join(pasta, nome)
            try:
                if agora - os.path.getmtime(caminho) > validade_segundos:
                    os.remove(caminho)
                    apagados += 1
            except OSError:
                pass
        return apagados
