import argparse
from pathlib import Path
import pandas as pd
from dotenv import load_dotenv
from sqlalchemy import text, Table, MetaData
from sqlalchemy.dialects.postgresql import insert as pg_insert

from utils_db import get_engine

ROOT = Path(__file__).resolve().parents[1]
load_dotenv(ROOT / ".env")

RAW = ROOT / "data" / "raw"
SQL_DIR = ROOT / "sql"
DDL_FILE = SQL_DIR / "database_schema.sql"
STAGING = ROOT / "data" / "staging"
STAGING.mkdir(parents=True, exist_ok=True)


def log(msg: str) -> None:
    print(msg, flush=True)


def read_csv_or_fail(filename: str) -> pd.DataFrame:
    path = RAW / filename
    if not path.exists():
        raise FileNotFoundError(f"Arquivo não encontrado: {path}")
    df = pd.read_csv(path)
    for c in df.columns:
        if df[c].dtype == "object":
            df[c] = df[c].astype(str).str.strip()
    return df


def coerce_dates(df: pd.DataFrame, cols) -> pd.DataFrame:
    for col in cols:
        if col in df.columns:
            df[col] = pd.to_datetime(df[col], errors="coerce").dt.date
    return df

def _create_min_schema(con) -> None:
    con.execute(text("""
        CREATE SCHEMA IF NOT EXISTS analytics;

        CREATE TABLE IF NOT EXISTS analytics.dim_cliente (
          id            BIGINT PRIMARY KEY,
          nome          TEXT,
          estado        CHAR(2),
          data_inicio   DATE
        );

        CREATE TABLE IF NOT EXISTS analytics.fact_contrato (
          id             BIGINT PRIMARY KEY,
          cliente_id     BIGINT REFERENCES analytics.dim_cliente(id),
          valor_mensal   NUMERIC(12,2),
          data_inicio    DATE,
          status         TEXT
        );

        CREATE TABLE IF NOT EXISTS analytics.fact_premio (
          id            BIGINT PRIMARY KEY,
          contrato_id   BIGINT REFERENCES analytics.fact_contrato(id),
          data_premio   DATE,
          valor         NUMERIC(12,2)
        );

        CREATE TABLE IF NOT EXISTS analytics.fact_resgate (
          id            BIGINT PRIMARY KEY,
          contrato_id   BIGINT REFERENCES analytics.fact_contrato(id),
          data_resgate  DATE,
          valor         NUMERIC(12,2)
        );
    """))
    log(" - Schema mínimo criado (fallback).")


def apply_schema(con) -> None:
    """Aplica schema SQL do arquivo (se existir) e garante colunas opcionais."""
    if DDL_FILE.exists():
        ddl = DDL_FILE.read_text(encoding="utf-8").strip()
        if ddl:
            con.exec_driver_sql(ddl)
            log(" - Schema aplicado (database_schema.sql).")
        else:
            log(" ! Aviso: database_schema.sql está vazio. Criando schema mínimo...")
            _create_min_schema(con)
    else:
        log(" ! Aviso: database_schema.sql não encontrado. Criando schema mínimo...")
        _create_min_schema(con)

    con.execute(text("""
        ALTER TABLE analytics.dim_cliente
        ADD COLUMN IF NOT EXISTS idade INT,
        ADD COLUMN IF NOT EXISTS faixa_etaria TEXT,
        ADD COLUMN IF NOT EXISTS renda_mensal NUMERIC(12,2);
    """))

    con.execute(text("""
        ALTER TABLE analytics.fact_contrato
        ADD COLUMN IF NOT EXISTS tipo_titulo TEXT,
        ADD COLUMN IF NOT EXISTS rentabilidade_estim NUMERIC(10,6);
    """))


def truncate_dev(con) -> None:
    """Limpa as tabelas antes da carga (modo dev)."""
    con.execute(text("""
        TRUNCATE TABLE
          analytics.fact_premio,
          analytics.fact_resgate,
          analytics.fact_contrato,
          analytics.dim_cliente
        RESTART IDENTITY CASCADE;
    """))
    log(" - Tabelas limpas (TRUNCATE + RESTART IDENTITY).")


def _columns_in_db(con, schema: str, table: str) -> set[str]:
    rows = con.execute(text("""
        SELECT column_name
        FROM information_schema.columns
        WHERE table_schema = :sch AND table_name = :tab
    """), {"sch": schema, "tab": table}).fetchall()
    return {r[0] for r in rows}


def _upsert_df(con, df: pd.DataFrame, schema: str, table: str, conflict_cols=("id",)) -> None:
    """
    Upsert (INSERT ... ON CONFLICT DO NOTHING) para evitar erro de PK duplicada.
    - Só insere colunas que existem no destino.
    - Ignora linhas cujo id já existe.
    """
    if df.empty:
        return

    cols_db = _columns_in_db(con, schema, table)
    cols_keep = [c for c in df.columns if c in cols_db]
    if not cols_keep:
        log(f" ! Aviso: nenhum campo de {schema}.{table} encontrado no DataFrame. Nada a inserir.")
        return

    md = MetaData()
    tbl = Table(table, md, schema=schema, autoload_with=con)

    records = df.loc[:, cols_keep].to_dict(orient="records")
    CHUNK = 1000

    for i in range(0, len(records), CHUNK):
        chunk = records[i:i + CHUNK]
        if not chunk:
            continue
        stmt = pg_insert(tbl).values(chunk).on_conflict_do_nothing(
            index_elements=list(conflict_cols)
        )
        con.execute(stmt)


def load_tables(con, clientes, contratos, premios, resgates) -> None:
    """Carrega as tabelas tratadas com UPSERT (evita duplicadas)."""
    _upsert_df(con, clientes,  "analytics", "dim_cliente",   conflict_cols=("id",))
    _upsert_df(con, contratos, "analytics", "fact_contrato", conflict_cols=("id",))
    _upsert_df(con, premios,   "analytics", "fact_premio",   conflict_cols=("id",))
    _upsert_df(con, resgates,  "analytics", "fact_resgate",  conflict_cols=("id",))
    log(" - Carga (UPSERT) concluída nas tabelas analytics.*")


# ----------------------------- KPI ----------------------------- #
def create_kpi_table(con) -> None:
    """Cria tabela de agregação mensal."""
    con.execute(text("DROP TABLE IF EXISTS analytics.kpi_contribuicoes_mensais"))
    con.execute(text("""
        CREATE TABLE analytics.kpi_contribuicoes_mensais AS
        SELECT date_trunc('month', data_inicio) AS mes,
               SUM(valor_mensal) AS total_mensal
        FROM analytics.fact_contrato
        GROUP BY 1
        ORDER BY 1;
    """))
    log(" - KPI mensal (analytics.kpi_contribuicoes_mensais) recriada.")


# ----------------------------- BCB ENRICH ----------------------------- #
def _save_macro_cache(cdi_df: pd.DataFrame, ipca_df: pd.DataFrame) -> None:
    if not cdi_df.empty:
        cdi_df.to_csv(STAGING / "cdi.csv", index=False)
    if not ipca_df.empty:
        ipca_df.to_csv(STAGING / "ipca.csv", index=False)


def _load_macro_cache() -> tuple[pd.DataFrame, pd.DataFrame]:
    cdi_path = STAGING / "cdi.csv"
    ipca_path = STAGING / "ipca.csv"
    cdi = pd.read_csv(cdi_path, parse_dates=["data"]) if cdi_path.exists() else pd.DataFrame()
    ipca = pd.read_csv(ipca_path, parse_dates=["data"]) if ipca_path.exists() else pd.DataFrame()
    return cdi, ipca


def enrich_with_bcb(contratos: pd.DataFrame) -> pd.DataFrame:
    """Adiciona 'rentabilidade_estim' baseada em CDI (usa cache se API falhar)."""
    try:
        from api_bcb import get_cdi, get_ipca
    except Exception:
        log(" ! Módulo api_bcb não encontrado. Pulei enriquecimento BCB.")
        contratos["rentabilidade_estim"] = None
        return contratos

    log(" - Buscando CDI/IPCA no BCB...")
    cdi = pd.DataFrame()
    ipca = pd.DataFrame()
    try:
        cdi = get_cdi()
        ipca = get_ipca()
    except Exception as e:
        log(f" ! Falha ao consultar BCB ({e}). Tentando cache local...")

    if cdi.empty:
        log(" ! CDI vazio (BCB fora do ar ou 406). Tentando cache local...")
        cdi, ipca = _load_macro_cache()
        if cdi.empty:
            log(" ! Cache local ausente. Pulei enriquecimento.")
            contratos["rentabilidade_estim"] = None
            return contratos

    _save_macro_cache(cdi, ipca)

    # CDI ao mês a partir do CDI ao ano
    cdi["cdi_am"] = (1 + cdi["cdi_aa"] / 100.0) ** (1 / 12) - 1
    cdi["competencia"] = cdi["data"].dt.to_period("M").dt.to_timestamp()

    contratos = contratos.copy()
    contratos["competencia"] = pd.to_datetime(
        contratos["data_inicio"], errors="coerce"
    ).to_period("M").dt.to_timestamp()

    contratos = contratos.merge(
        cdi[["competencia", "cdi_am"]],
        on="competencia",
        how="left"
    )

    contratos["rentabilidade_estim"] = (1 + contratos["cdi_am"].fillna(0)) ** 12 - 1
    contratos.drop(columns=["competencia", "cdi_am"], inplace=True, errors="ignore")

    log(" - Enriquecimento com CDI/IPCA concluído.")
    return contratos


# ----------------------------- RELATÓRIO ----------------------------- #
def maybe_generate_report(enable: bool) -> None:
    """Gera relatório PDF final se flag --report estiver ativa."""
    if not enable:
        return
    import subprocess, sys
    rel_path = ROOT / "src" / "gerar_relatorio.py"
    if rel_path.exists():
        log(" - Gerando relatório PDF...")
        try:
            subprocess.run([sys.executable, str(rel_path)], check=True)
            log(" - Relatório PDF gerado em report/report_brasilcap.pdf")
        except Exception as e:
            log(f" ! Aviso: falha ao gerar relatório PDF ({e})")
    else:
        log(" ! Script de relatório não encontrado (src/gerar_relatorio.py).")


# ----------------------------- MAIN ----------------------------- #
def main():
    parser = argparse.ArgumentParser(description="ETL Capitalização — Brasilcap Analytics")
    parser.add_argument("--truncate", action="store_true",
                        help="Limpa as tabelas antes de carregar (modo dev).")
    parser.add_argument("--report", action="store_true",
                        help="Gera relatório PDF ao final.")
    parser.add_argument("--bcb", action="store_true",
                        help="Enriquece contratos com CDI/IPCA do Banco Central.")
    args = parser.parse_args()

    print(">>> Iniciando ETL")

    eng = get_engine()
    with eng.begin() as con:
        apply_schema(con)
        if args.truncate:
            truncate_dev(con)

        # Leitura
        clientes  = read_csv_or_fail("clientes.csv")
        contratos = read_csv_or_fail("contratos.csv")
        premios   = read_csv_or_fail("premios.csv")
        resgates  = read_csv_or_fail("resgates.csv")

        # Datas
        clientes  = coerce_dates(clientes,  ["data_inicio"])
        contratos = coerce_dates(contratos, ["data_inicio"])
        premios   = coerce_dates(premios,   ["data_premio"])
        resgates  = coerce_dates(resgates,  ["data_resgate"])

        # Enriquecimento BCB (opcional)
        if args.bcb:
            contratos = enrich_with_bcb(contratos)

        # Carga (UPSERT)
        load_tables(con, clientes, contratos, premios, resgates)

        # KPIs
        create_kpi_table(con)

    print(">>> ETL finalizado")
    maybe_generate_report(args.report)


if __name__ == "__main__":
    main()
