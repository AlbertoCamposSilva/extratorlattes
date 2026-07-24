# Extrator Lattes

O **Extrator Lattes** é uma ferramenta para automação de download, processamento e extração de currículos na plataforma Lattes do CNPq.

## LattesDownloader (Download via REST)

A classe `LattesDownloader` é a principal engrenagem para baixar currículos de forma segura e automatizada. Ela utiliza a API REST (`get_zip_from_REST`) com as seguintes características essenciais:

### 1. Sistema Antibloqueio e Retentativas (Exponential Backoff)
Como o Firewall do CNPq derruba ativamente conexões sequenciais muito rápidas (lançando `ConnectionResetError` 10054) como medida anti-DDoS, esta ferramenta conta com um `HTTPAdapter` embutido configurado com a classe `PowerOfFourRetry`.
- **Comportamento**: Quando a rede cai ou engasga, a ferramenta espera `4s`, depois `16s`, e depois `64s` (Base 4) antes de tentar refazer o acesso ao currículo (limite de 3 tentativas).
- Nunca execute downloads em massa sem usar `time.sleep(1)` entre os acessos!

### 2. Autenticação e Dados Restritos (CPF / Data de Nascimento)
A extração carrega automaticamente as credenciais da Intranet (cookies) presentes no arquivo `database.ini` (sessão `[lattesweb]`).
- O downloader checa internamente o XML baixado: **Se o currículo não contiver `CPF=` e também não contiver `DATA-NASCIMENTO=`**, a classe abortará a operação, ciente de que a sessão expirou e o CNPq entregou uma versão "pública" truncada.
- Isso garante que todos os currículos contabilizados como sucesso estão 100% íntegros e restritos no banco de dados.

### 3. Tratamento de IDs Ausentes / Ocultos
Se o pesquisador apagar o currículo, ou ocultá-lo, o CNPq não lançará um erro `404 Not Found`. Em vez disso, ele redirecionará para uma URL `cv?id=16_digitos` em vez de gerar a chave alfanumérica (`visualizacv.do?id=K47...`).
- O `LattesDownloader` detecta esse redirecionamento "vazio", ignorando a retentativa de rede (pois a rede está OK) e lançando a falha: *"Não foi possível converter o ID de 16 dígitos para o ID interno"*.

### 4. Padrão Ouro de Extração e Atualização do Banco
Como o acesso antigo via SOAP WSCurriculo não está mais disponível, o padrão ouro oficial da arquitetura agora consiste no seguinte fluxo em pipeline:
1. Extração na web usando `downloader = LattesDownloader(...)` e em seguida `downloader.get_zip_from_REST()`.
2. Geração dos indicadores (parse do XML) passando o downloader carregado: `ind = Indicadores(lattes=downloader)`.
3. Escrita final no banco de dados invocando `ind.atualiza()`.

---
**Dica de Uso Prático**: 
Para atualizações em massa, utilize o arquivo `atualiza_sexo_no_BD.py` no diretório `tests/`. Ele incorpora uma barra de progresso (`tqdm`) preditiva, log de retentativas no terminal, e já gerencia erros de banco de dados e problemas de rede separadamente.
