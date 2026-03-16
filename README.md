# Projeto Data Enrichment

Solução para consumir, higienizar e enriquecer os dados locais do `input.csv` e envia-los por HTTP post à uma API analítica. Para entender a arquitetura implementada (regras de validação e desempate logico), por favor leia o arquivo secundário `NOTAS_EXPLICATIVAS.md` incluso na pasta raiz.

## Pré-requisitos
Para o correto funcionamento do script base `solution.py`, você precisará de uma versão do **Python (3.8+)**

## Getting Started / Instalando

1. Clonagem 
```bash
git clone https://github.com/felipe0365/nasajon-desafio.git
cd nasajon-desafio
```

2. Instale as dependências externas do runtime
O núcleo utiliza puramente o SDK `urllib` nativo da linguagem para a maioria, mas requisita `requests` para os verbos http.
```bash
pip install -r requirements.txt
```

## Como Usar o Sistema

O script efetua três blocos simultâneos em sua única execução (Carga local, Parsing do IBGE, Download em `resultado.csv` + Upload para o Backend Cloud). Para não engatilhar a segurança por falta de um Access Token real, autenticamos o sistema definindo sua porta em um ENV:

### 1. Injetando a sua Token Autenticadora
O sistema lerá implicitamente os tokens carregados na session. Substitua copiando a Response que usou ou gerou nas APIs REST autenticadoras do projeto.

**Via Powershell (Windows):**
```powershell
$env:ACCESS_TOKEN="<SUA_BEARER_TOKEN_GIGANTE_AQUI>"
```

**Via Bash (Linux ou Windows WSL/GitBash):**
```bash
export ACCESS_TOKEN="<SUA_BEARER_TOKEN_GIGANTE_AQUI>"
```

### 2. Rodando de fato o Parser
Uma vez logado em linha com a variável preenchida, é bater e rodar:
```bash
python solution.py
```

O dashboard no próprio output informará ao usuário qual a pontuação estrita que ele obteve entre o motor do backend contra o cálculo rodado nativamente nesta máquina.
