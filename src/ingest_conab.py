from google.cloud import bigquery

_STATE_NAME_TO_CODE = {
    "acre": "AC",
    "alagoas": "AL",
    "amapá": "AP",
    "amazonas": "AM",
    "bahia": "BA",
    "ceará": "CE",
    "distrito federal": "DF",
    "espírito santo": "ES",
    "goiás": "GO",
    "maranhão": "MA",
    "mato grosso": "MT",
    "mato grosso do sul": "MS",
    "minas gerais": "MG",
    "pará": "PA",
    "paraíba": "PB",
    "paraná": "PR",
    "pernambuco": "PE",
    "piauí": "PI",
    "rio de janeiro": "RJ",
    "rio grande do norte": "RN",
    "rio grande do sul": "RS",
    "rondônia": "RO",
    "roraima": "RR",
    "santa catarina": "SC",
    "são paulo": "SP",
    "sergipe": "SE",
    "tocantins": "TO",
}


def normalize_state_code(state) -> str:
    if state is None:
        raise ValueError("state code cannot be None")
    cleaned = str(state).strip()
    if not cleaned:
        raise ValueError("state code cannot be empty")
    upper = cleaned.upper()
    if len(upper) == 2:
        return upper
    mapped = _STATE_NAME_TO_CODE.get(cleaned.lower())
    if mapped:
        return mapped
    return upper


def load_to_bigquery(df, table_id: str) -> None:
    client = bigquery.Client()
    client.load_table_from_dataframe(df, table_id)
