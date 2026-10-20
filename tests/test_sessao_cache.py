import os
import threading
import time

from extratorlattes.sessao_cache import SessaoCache


def test_sessoes_isoladas_e_reuso_por_id(tmp_path):
    a = SessaoCache("a", pasta=str(tmp_path))
    b = SessaoCache("b", pasta=str(tmp_path))
    a.salvar([{"name": "JSESSIONID", "value": "1"}], "UA-a")
    assert b.carregar() is None
    assert SessaoCache("a", pasta=str(tmp_path)).carregar()["user_agent"] == "UA-a"
    assert SessaoCache(pasta=str(tmp_path)).sessao_id != SessaoCache(pasta=str(tmp_path)).sessao_id


def test_expira_em_24h(tmp_path):
    c = SessaoCache("x", pasta=str(tmp_path), validade_segundos=1)
    c.salvar([], "UA")
    time.sleep(1.2)
    assert c.carregar() is None
    assert not os.path.exists(c.caminho)


def test_escrita_concorrente_nunca_le_arquivo_pela_metade(tmp_path):
    c = SessaoCache("compartilhada", pasta=str(tmp_path))
    erros = []

    def escreve(i):
        try:
            for _ in range(50):
                c.salvar([{"name": "k", "value": str(i) * 500}], f"UA{i}")
        except Exception as e:  # erro em thread não derruba o teste sozinho
            erros.append(e)

    def le():
        for _ in range(200):
            d = SessaoCache("compartilhada", pasta=str(tmp_path)).carregar()
            if d is not None and not d["user_agent"].startswith("UA"):
                erros.append(d)

    threads = [threading.Thread(target=escreve, args=(i,)) for i in range(4)] + [threading.Thread(target=le)]
    for t in threads:
        t.start()
    for t in threads:
        t.join()
    assert not erros
    assert not [n for n in os.listdir(tmp_path) if n.endswith(".tmp")]


def test_limpar_vencidos(tmp_path):
    velho = tmp_path / "velho.json"
    velho.write_text("{}")
    os.utime(velho, (time.time() - 90000, time.time() - 90000))
    SessaoCache("novo", pasta=str(tmp_path)).salvar([], "UA")
    assert SessaoCache.limpar_vencidos(pasta=str(tmp_path)) == 1
    assert os.listdir(tmp_path) == ["novo.json"]
