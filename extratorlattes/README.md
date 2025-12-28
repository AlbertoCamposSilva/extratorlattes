# Extrator Lattes

Uma biblioteca em Python para extração e processamento de dados da Plataforma Lattes (CNPq).

## Descrição

O **Extrator Lattes** é uma ferramenta desenvolvida para facilitar a obtenção de informações de currículos Lattes, permitindo a análise de dados acadêmicos de forma programática.

## Instalação

Para instalar o pacote e suas dependências:

```bash
pip install extrator_lattes
```

## Documentação

https://www.albertocampos.com.br/extrator_lattes

## Uso Básico

```python
# Exemplo de uso (ajuste conforme a API real da biblioteca)
from extrator_lattes import Lattes

# Inicializa o extrator
lattes = Lattes('7281587998425548')
lattes.get_lattes()
lattes.pega_dados_lattes()



## Licença

Este projeto é distribuído sob a licença **GNU Lesser General Public License v3 (LGPLv3)**.

Isso significa que você pode usar esta biblioteca em projetos proprietários (fechados), desde que a biblioteca seja linkada dinamicamente e quaisquer modificações feitas na biblioteca em si sejam compartilhadas de volta sob a mesma licença.

Consulte o arquivo [LICENCE](LICENCE) para os termos da LGPL e o arquivo [COPYING](COPYING) para os termos da GPL (necessária como base para a LGPL).
```
