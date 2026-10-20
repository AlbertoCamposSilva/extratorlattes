---
name: extracao-lattes-cnpq
description: Diretrizes de comportamento e proteção antibloqueio para extração de currículos Lattes em lote usando a classe LattesDownloader.
---

# Extração de Lattes do CNPq (Regras de Segurança)

Este arquivo descreve as regras vitais e comportamentos esperados ao lidar com downloads de Lattes da plataforma do CNPq (`extratorlattes`).

## 1. Identificação de Lattes Ausentes/Excluídos
A conversão do ID de 16 dígitos para o ID alfanumérico interno (`K47...`) é feita capturando o cabeçalho de redirecionamento HTTP de `http://lattes.cnpq.br/<ID>`.
- **Regra**: Se a URL final de redirecionamento contiver `cv?id=`, isso significa que o currículo foi EXCLUÍDO da base pública ou está OCULTO pelo pesquisador. O script não pode extrair um ID alfanumérico e deve lançar um erro legível de não conversão. Esse comportamento é rápido, não-bloqueante e intencional.

## 2. Quedas de Conexão e Proteção Anti-DDoS
O Firewall do CNPq derrubará sumariamente conexões (`ConnectionResetError` 10054) se houverem downloads massivos sequenciais muito rápidos.
- **Regra**: Todo acesso à rede deve utilizar `requests.Session()` montado com um `HTTPAdapter` e a política de retentativas `PowerOfFourRetry`.
- **PowerOfFourRetry**: Política de retentativa (base 4) que aplica esperas de `4s`, `16s`, `64s` a cada falha de conexão subsequente. O limite máximo fixo absoluto deve ser `120` segundos e o número de retentativas não deve exceder 3. Somente após a terceira tentativa falha (ou se houver um tempo esgotado) o erro deve propagar.

## 3. Verificação de Integridade de Sessão Logada (Dados Restritos)
Currículos baixados com login de servidor do CNPq (credenciais no cofre de segredos; cookies no cache local de sessão) devem conter dados pessoais.
- **Regra**: A sessão expirou ou o Lattes foi retornado na versão pública SE E SOMENTE SE as marcações XML de **ambos** os atributos `CPF=` **e** `DATA-NASCIMENTO=` estiverem ausentes. Pesquisadores estrangeiros podem não ter `CPF`, mas terão `DATA-NASCIMENTO`.
- Se ambos faltarem, lance um erro (como `RuntimeError`), indicando que o Lattes baixado é a versão pública e precisa de relogin.

## 4. Processamento em Lote (Loops)
Nunca processe currículos Lattes em lote em um loop de velocidade total.
- **Regra**: Aplique um `time.sleep(1)` entre os processamentos.
- **Interface**: Utilize a biblioteca `tqdm` para exibir progresso. Recomenda-se aproveitar o `eta` do tqdm para injetar a previsão de término (`data_termino`) visualmente no `postfix`.

## 5. Padrão Ouro de Extração e Inserção no Banco de Dados
O processo oficial de baixar e gravar os dados DEVE ser feito via REST (HTTP) utilizando a classe `LattesDownloader` (`lattesweb.py`), visto que o serviço SOAP WSCurriculo (`extratorlattes` puro) não possui mais acesso autorizado.
- **O Fluxo Obrigatório**:
  1. Instancie o `downloader = LattesDownloader(...)` (mantendo-o fora do loop se for processamento em lote para preservar a sessão de cookies).
  2. Defina o ID alvo: `downloader.id_lattes = ID` e chame `downloader.get_zip_from_REST()`.
  3. Se retornar sucesso, extraia e grave os dados instanciando a classe `Indicadores`: `ind = Indicadores(lattes=downloader, verbose=False)`.
  4. Por fim, chame `ind.atualiza()` para que o banco de dados seja de fato atualizado.

## 6. Fonte de dados: priorize o `lattes_json`
- **Regra**: para vínculos, publicações, palavras-chave e áreas de atuação de um pesquisador, consulte **direto o
  `lattes_json`** (índices `idx_lattes_fts` e `idx_lattes_identificacao`).
- As tabelas `lattes_xml`, `lattes_palavras_chave`, `lattes_areas_conhecimento` (id_lattes, tipo, area),
  `lattes_publicacoes` e `lattes_vinculos` são **legadas**. Foram removidas do banco do CNPq em 10/2026 porque estavam
  paradas e cobriam menos de 2% dos currículos.
  - O código de gravação continua na biblioteca, **desligado por padrão** em `Indicadores.atualiza()` e em `Carga`.
  - Se uma opção legada for ligada e a tabela não existir, a gravação lança `RuntimeError`. A tabela **nunca** é criada
    automaticamente.
  - Não ligue essas opções nem rode `constroi_tabelas` para recriá-las sem pedido explícito do usuário.
- O catálogo oficial de áreas do conhecimento do CNPq é a tabela `cnpq_areas_conhecimento` (código, nome e hierarquia).
  Não é derivada dos currículos.
