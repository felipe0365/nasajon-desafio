import csv
import json
import os
import requests
import unicodedata
import difflib
import logging
from typing import List, Dict, Tuple, Optional, Any
from pydantic import BaseModel, Field

# Configuração de Logging 
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

# Pydantic Models (Type Hints & Validação Estrita)
class MunicipioIBGE(BaseModel):
    id_ibge: str
    nome: str
    nome_norm: str
    uf: str
    regiao: str

class MunicipioInput(BaseModel):
    municipio_input: str
    populacao_input: int

class Estatisticas(BaseModel):
    total_municipios: int = 0
    total_ok: int = 0
    total_nao_encontrado: int = 0
    total_erro_api: int = 0
    pop_total_ok: int = 0
    medias_por_regiao: Dict[str, float] = Field(default_factory=dict)

# Core: Normalização
def normalize_string(s: str) -> str:
    """
    Normaliza a string convertendo para minúsculas e removendo os acentos
    (usando a normalização NFD).
    """

    # Se não for uma string recebida na função, devolve texto em branco, não quebra a aplicação
    if not isinstance(s, str):
        return ""
    
    # Separa os caracteres compostos (á) e divide em dois "a" e "´" (letra A e acento agudo)
    s_norm = unicodedata.normalize('NFD', s)

    # Passa por cada pedaço da palavra e joga fora o que for da categoria 'Mn' que é a categoria tecnica dos acentos no Unicode.
    s_clean = ''.join(c for c in s_norm if unicodedata.category(c) != 'Mn')

    # Pega o texto sem acento, transforma tudo em letra minuscula e remove qualquer espaço extra no final ou começo da palavra
    return s_clean.lower().strip()

# Core: Carregamento do IBGE
def fetch_ibge_data() -> Tuple[Optional[List[MunicipioIBGE]], bool]:
    """
    Consome a API pública do IBGE, mapeia em memória para o Dataset Type Mapped (Pydantic) 
    e retorna junto de uma flag de erro.
    """
    url = "https://servicodados.ibge.gov.br/api/v1/localidades/municipios"
    try:
        logger.info("Iniciando requisição para API Localidades/IBGE...")

        # Se o site estiver fora do ar ou muito lento, o programa desiste depois de 10 segundos
        response = requests.get(url, timeout=10)

        # Se a API retornar algum erro, dispara um alarme e jogo o codigo direto para o except
        response.raise_for_status()
        raw_data = response.json()
        
        # Cria lista vazia onde serão guardados os municipios limpos e estruturados
        ibge_processed = []

        # Loop por cada municipio dentro de raw_data
        for mun in raw_data:
            # Tenta pegar o nome do municipio, se não conseguir, usa string vazia
            nome = mun.get('nome', '')
            try:
                # Pega a sigla do estado
                uf = mun['microrregiao']['mesorregiao']['UF']['sigla']
                # Pega a região
                regiao = mun['microrregiao']['mesorregiao']['UF']['regiao']['nome']
            except (KeyError, TypeError):
                # Caso der erro, preenche com string vazia
                uf = ""
                regiao = ""
                
            # A lista ganha um novo item. 
            ibge_processed.append(
                MunicipioIBGE(
                    id_ibge=str(mun.get('id', '')),
                    nome=nome,
                    nome_norm=normalize_string(nome),
                    uf=uf,
                    regiao=regiao
                )
            )
            
        logger.info(f"Carga completa. {len(ibge_processed)} municípios carregados na cache.")
        # Retorna lista processada e indicando que não houveram erros de requisição
        return ibge_processed, False
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Falha gravada no consumo estrutural da API (IBGE): {e}")
        # Caso der erro, retorna None e indica que houve erros de requisição
        return None, True

# Lógica de Cruzamento: Fuzzy Matcher
def match_municipality(mun_input: str, ibge_processed: List[MunicipioIBGE]) -> Tuple[str, Optional[MunicipioIBGE]]:
    """
    Toma posse de uma string originada do input do candidato e a cruza via difflib contra
    o cache Pydantic com todos os municípios, cuidando de ambiguidades (UF).
    """

    # Normaliza o nome do municipio
    mun_norm = normalize_string(mun_input)
    # Variavel que vai guardar o percentual de semelhança
    best_ratio = 0.0
    # Lista que armazena as cidades que atingiram o melhor nivel
    best_matches: List[MunicipioIBGE] = []
    
    # Loop de comparação entre os municipios
    for ibge_mun in ibge_processed:
        # Pega as duas strings e diz o quanto elas se parecem atraves de uma nota
        ratio = difflib.SequenceMatcher(None, mun_norm, ibge_mun.nome_norm).ratio()
        # Se a nova nota for maior que o recorde anterior, encontramos um candidato melhor e atualiza a nota
        if ratio > best_ratio:
            best_ratio = ratio
            best_matches = [ibge_mun]
        # Se a nova nota for igual ao recorde, mantem os dois na lista para desempatar no final
        elif ratio == best_ratio and ratio > 0:
            best_matches.append(ibge_mun)
            
    # Regra principal de Aprovação Fuzzy (Tolerance = 0.82)
    if best_ratio >= 0.82:
        # Confere se sobrou apenas 1 cidade como favorita ou sobraram varias cidades empatadas, porém todas tem exatamente os mesmo nomes
        if len(best_matches) == 1 or (len(best_matches) > 1 and best_ratio == 1.0):
            # Pega a primeira cidade da lista
            match = best_matches[0]
            # Desempate estrito para "Santo André" caso aja a variante de UF (SP / PB)
            for m in best_matches:
                # Se a cidade for de São Paulo, assume ela
                if m.uf == 'SP':
                    match = m
                    break
            # Retorna um OK e retorna a cidade escolhida
            return "OK", match
        else:
            # Retorna ambiguo, caso as duas cidades empatadas não sejam um correspondencia perfeita
            return "AMBIGUO", None

    # Retorna não encontrado, caso nenhuma cidade alcance a nota de corte desejada
    return "NAO_ENCONTRADO", None

# IO: Processamento Batch
def process_data(input_file: str = "input.csv", output_file: str = "resultado.csv") -> Optional[Estatisticas]:
    """
    Orquestra a leitura do batch `.csv`, submete à classe de match, e recalcula os agregados 
    finais para submissão, agindo como Single Point of Trust.
    """

    # Chama a função para pegar os dados da API do IBGE e guarda a lista de cidades e se deu erro
    ibge_processed, api_error = fetch_ibge_data()
    # Variavel para guardar as estatisticas 
    stats = Estatisticas()
    
    # Variaveis para guardar valores temporarios que ajudarão a montar o documento final e calcular a media por região
    somas_por_regiao: Dict[str, float] = {}
    contagem_por_regiao: Dict[str, int] = {}
    resultados: List[Dict[str, Any]] = []

    try:
        # Tenta abriro o arquivo .csv
        with open(input_file, mode='r', encoding='utf-8') as f:
            # Transforma cada linha da planilha em um dicionario
            reader = csv.DictReader(f)
            logger.info(f"Processando batch '{input_file}' contra o motor algorítmico")
            for row in reader:
                municipio_input = row.get('municipio', '')
                pop_str = row.get('populacao', '0')
                
                try:
                    # Converte a população que veio da planilha em int
                    pop_input = int(pop_str)
                except ValueError:
                    # Se falhar, assume 0
                    pop_input = 0
                
                # Adiciona +1 no total de municipios
                stats.total_municipios += 1
                
                # Se IBGE falhou por HTTP antes de entrar, preenchemos o modelo degradado
                if api_error or not ibge_processed:
                    resultados.append({
                        "municipio_input": municipio_input,
                        "populacao_input": pop_input,
                        "municipio_ibge": "",
                        "uf": "",
                        "regiao": "",
                        "id_ibge": "",
                        "status": "ERRO_API"
                    })
                    # Aumenta o contador de erro
                    stats.total_erro_api += 1
                    continue
                
                # Salva na variavel os dados da cidade e o status 
                status, match = match_municipality(municipio_input, ibge_processed)
                
                # Se o status for OK e tiver uma cidade.
                if status == "OK" and match:
                    # Aumenta o contador de OK em +1
                    stats.total_ok += 1
                    # Aumenta o contador do total da população com a população da cidade que chegou agora
                    stats.pop_total_ok += pop_input
                    
                    reg = match.regiao
                    somas_por_regiao[reg] = somas_por_regiao.get(reg, 0.0) + float(pop_input)
                    contagem_por_regiao[reg] = contagem_por_regiao.get(reg, 0) + 1
                    
                    resultados.append({
                        "municipio_input": municipio_input,
                        "populacao_input": pop_input,
                        "municipio_ibge": match.nome,
                        "uf": match.uf,
                        "regiao": reg,
                        "id_ibge": match.id_ibge,
                        "status": status
                    })
                else: 
                    # NAO_ENCONTRADO ou AMBIGUO
                    stats.total_nao_encontrado += 1
                    resultados.append({
                        "municipio_input": municipio_input,
                        "populacao_input": pop_input,
                        "municipio_ibge": "",
                        "uf": "",
                        "regiao": "",
                        "id_ibge": "",
                        "status": status
                    })
                    
    except FileNotFoundError:
        logger.error(f"Sistema não conseguiu resolver o path de entrada: {input_file}")
        return None

    # Média populacional Ponderada por Ocorrência
    for reg, total_pop in somas_por_regiao.items():
        media = total_pop / contagem_por_regiao[reg]
        stats.medias_por_regiao[reg] = round(media, 2)

    # Export
    with open(output_file, mode='w', encoding='utf-8', newline='') as f:
        fieldnames = ["municipio_input", "populacao_input", "municipio_ibge", "uf", "regiao", "id_ibge", "status"]
        writer = csv.DictWriter(f, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(resultados)

    logger.info(f"Dados processados e salvos em {output_file} com sucesso")
    return stats

# API: Transação Supabase
def submit_stats(stats: Estatisticas):
    """
    Conecta-se à plataforma Cloud da Nasajon visando transacionar as estatísticas provadas.
    Atua com Graceful Degradation se Tokens não servidos.
    """
    access_token = os.environ.get("ACCESS_TOKEN")
    if not access_token:
        logger.warning("ACCESS_TOKEN missing. A submissão à Edge Function foi ignorada intencionalmente")
        return
    
    url = "https://mynxlubykylncinttggu.functions.supabase.co/ibge-submit"
    headers = {
        "Authorization": f"Bearer {access_token}",
        "Content-Type": "application/json"
    }
    
    # Transfere a dataclass pro payload de envio
    payload = {"stats": stats.model_dump()}
    
    logger.info("Transferindo resultados (POST) contra o validador Edge Function...")
    try:
        response = requests.post(url, headers=headers, json=payload, timeout=10)
        logger.info(f"Response origin Header / Status Code: {response.status_code}")
        
        try:
            resp_json = response.json()
            # Deixar JSON exposto limpo fora do logger para legibilidade explícita da nota  
            print("\n============ Edge Function Feedback ============")
            print(json.dumps(resp_json, indent=2, ensure_ascii=False))
            print("=================================================\n")
        except json.JSONDecodeError:
            logger.error(f"Unprocessable Entity / Plain Text: {response.text}")
            
    except requests.exceptions.RequestException as e:
        logger.error(f"[TIMEOUT] Quebra de conexão não tratada contra o Supabase Cloud: {e}")

# main
def main():
    logger.info("Inicializando o desafio da Nasajon")
    stats = process_data()
    if stats:
        submit_stats(stats)
    logger.info("Processo concluído")

if __name__ == "__main__":
    main()
