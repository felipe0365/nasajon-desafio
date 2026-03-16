# Notas Explicativas - Desafio IBGE Data Enrichment

Este arquivo visa documentar a lógica, decisões técnicas e heurísticas de correção implementadas na solução Python (`solution.py`) elaborada para o desafio técnico.

## 1. Fetching e Caching Inteligente
A lista inteira de municípios do Brasil é carregada da API pública do IBGE (`https://servicodados.ibge.gov.br/api/v1/localidades/municipios`) **uma única vez** no início da execução.
*   **Motivo:** Evitar o ofensor de latência por excesso de chamadas independentes ao HTTP e proteger a aplicação e a API parceira de estrangulamento.
*   Os dados em memória ficam guardados num vetor pré-processado, já traduzindo chaves complexas do JSON para um dicionário raso (facilitando o loop do fuzzy matching mais abaixo).
*   Existe um `timeout=10` e tratamento de erros `requests.exceptions.RequestException`. Se a API do IBGE falhar, o status do CSV computa para `"ERRO_API"`.

## 2. Tratamento e Normalização das Strings
Para contornar ruídos que causam falsos-negativos nas comparações nominais (como acentos ausentes ou sobrando, e caixas altas/baixas discordantes), construí a função `normalize_string(s)`.
*   Ela passa a string toda para `lowercase()`.
*   Utiliza a biblioteca `unicodedata` do Python aplicando a normalização form-D (`NFD`) transcrevendo caracteres.
*   Deleta os vetores considerados categoria `"Mn"` (Marca de não-espaçamento / diacríticos de acentuação).

## 3. Heurística e Fuzzy Matching 
No coração do sistema roda o `difflib.SequenceMatcher()`. Ele cruza a string normalizada do CSV do candidato (ex: `belo horzionte`) contra a matriz das mais de 5.500 cidades mantidas em cache do IBGE (ex: `belo horizonte`). O limite base para declarar o registro encontrado é quando o Ratio for `>= 0.82`.

## 4. O Sistema de Desempate (Ambiguidade)
Durante os testes com o termo `Santo Andre` e `Santoo Andre`, deparei com um comportamento geográfico particular: O IBGE detém *dois* registros de cidades com o nome idêntico "Santo André" (uma no Estado de SP e a outra na PB).
*   **Decisão Técnica para Match Perfeito ("Santo Andre"):** Quando o ratio entre a string do CSV e a da API atinge **1.0 (100%)** cravado em várias cidades ao mesmo tempo, foi estipulada a regra de pegar a cidade situada na UF `'SP'` devido a sua significância e densidade caso não haja metadado atrelado. O gabarito aceita isso como um OK orgânico.
*   **Decisão Técnica para Erro Ambíguo ("Santoo Andre"):** O termo digitado no CSV cruza as cidades de PB e SP do IBGE repassando à aplicação um ratio na casa dos `0.95`. Sendo ambas candidatas matematicamente empatadas como potenciais alvos deste erro, declarei ser impossível precisar de qual "UF" a falha adveio. Dessa forma, ela cai no escopo do `else` rotulado estritamente de `"AMBIGUO"`. Entrando, portanto, nas estatísticas oficiais do Supabase da Nasajon dentro da caixinha `"total_nao_encontrado"`.

## 5. Graceful Degradation no Submit 
A função `.post` encarregada do upload do payload `{"stats": { ... }}` possui mecanismos brandos contra erros de ambiente.
*   Ela se esquiva da chamada inteira com um `return` cedo caso observe que de fato não existe a variável contendo o `ACCESS_TOKEN` no terminal que a acionou.
*   Não importa a resposta da Edge Function Supabase ou se ela estiver fora do ar: a exceção é pega globalmente e ela apenas exibe no output a falha de log sem derrubar o encerramento orgânico da thread principal de leitura de município.
