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
    if not isinstance(s, str):
        return ""
    s_norm = unicodedata.normalize('NFD', s)
    s_clean = ''.join(c for c in s_norm if unicodedata.category(c) != 'Mn')
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
        response = requests.get(url, timeout=10)
        response.raise_for_status()
        raw_data = response.json()
        
        ibge_processed = []
        for mun in raw_data:
            nome = mun.get('nome', '')
            try:
                uf = mun['microrregiao']['mesorregiao']['UF']['sigla']
                regiao = mun['microrregiao']['mesorregiao']['UF']['regiao']['nome']
            except (KeyError, TypeError):
                uf = ""
                regiao = ""
                
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
        return ibge_processed, False
        
    except requests.exceptions.RequestException as e:
        logger.error(f"Falha gravada no consumo estrutural da API (IBGE): {e}")
        return None, True

# Lógica de Cruzamento: Fuzzy Matcher
def match_municipality(mun_input: str, ibge_processed: List[MunicipioIBGE]) -> Tuple[str, Optional[MunicipioIBGE]]:
    """
    Toma posse de uma string originada do input do candidato e a cruza via difflib contra
    o cache Pydantic com todos os municípios, cuidando de ambiguidades (UF).
    """
    mun_norm = normalize_string(mun_input)
    best_ratio = 0.0
    best_matches: List[MunicipioIBGE] = []
    
    for ibge_mun in ibge_processed:
        ratio = difflib.SequenceMatcher(None, mun_norm, ibge_mun.nome_norm).ratio()
        if ratio > best_ratio:
            best_ratio = ratio
            best_matches = [ibge_mun]
        elif ratio == best_ratio and ratio > 0:
            best_matches.append(ibge_mun)
            
    # Regra principal de Aprovação Fuzzy (Tolerance = 0.82)
    if best_ratio >= 0.82:
        if len(best_matches) == 1 or (len(best_matches) > 1 and best_ratio == 1.0):
            match = best_matches[0]
            # Desempate estrito para "Santo André" caso aja a variante de UF (SP / PB)
            for m in best_matches:
                if m.uf == 'SP':
                    match = m
                    break
            return "OK", match
        else:
            return "AMBIGUO", None
            
    return "NAO_ENCONTRADO", None

# IO: Processamento Batch
def process_data(input_file: str = "input.csv", output_file: str = "resultado.csv") -> Optional[Estatisticas]:
    """
    Orquestra a leitura do batch `.csv`, submete à classe de match, e recalcula os agregados 
    finais para submissão, agindo como Single Point of Trust.
    """
    ibge_processed, api_error = fetch_ibge_data()
    stats = Estatisticas()
    
    somas_por_regiao: Dict[str, float] = {}
    contagem_por_regiao: Dict[str, int] = {}
    resultados: List[Dict[str, Any]] = []

    try:
        with open(input_file, mode='r', encoding='utf-8') as f:
            reader = csv.DictReader(f)
            logger.info(f"Processando batch '{input_file}' contra o motor algorítmico")
            for row in reader:
                municipio_input = row.get('municipio', '')
                pop_str = row.get('populacao', '0')
                
                try:
                    pop_input = int(pop_str)
                except ValueError:
                    pop_input = 0
                
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
                    stats.total_erro_api += 1
                    continue
                
                status, match = match_municipality(municipio_input, ibge_processed)
                
                if status == "OK" and match:
                    stats.total_ok += 1
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
