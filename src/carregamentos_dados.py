import os
from pathlib import Path
import pandas as pd
from sqlalchemy import create_engine, text
from dotenv import load_dotenv

load_dotenv()

PG_HOST = os.getenv("PG_HOST", "localhost")
PG_PORT = os.getenv("PG_PORT", "5432")
PG_DB   = os.getenv("PG_DB", "brasilcap")
PG_USER = os.getenv("PG_USER", "cda_user")
PG_PASS = os.getenv("PG_PASSWORD", "cda_pass")

engine = create_engine(
    f"postgresql+psycopg2://{PG_USER}:{PG_PASS}@{PG_HOST}:{PG_PORT}/{PG_DB}"
)

RAW = Path(__file__).resolve().parents[1] / "data" / "raw"

def to_date(df, cols):
    for c in cols:
        if c in df.columns:
            df[c] = pd.to_datetime(df[c], errors="coerce").dt.date
    return df

def create_schema_and_tables():
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS bi AUTHORIZATION {PG_USER};

    CREATE TABLE IF NOT EXISTS bi.clientes (
        id BIGINT PRIMARY KEY,
        nome TEXT,
        estado TEXT,
        idade INT,
        faixa_etaria TEXT,
        renda_mensal NUMERIC(12,2),
        data_inicio DATE,
        cpf_cnpj TEXT
    );

    CREATE TABLE IF NOT EXISTS bi.contratos (
        id BIGINT PRIMARY KEY,
        cliente_id BIGINT,
        valor NUMERIC(12,2),
        data_inicio DATE,
        status TEXT,
        tipo_titulo TEXT
    );

    CREATE TABLE IF NOT EXISTS bi.premios (
        id BIGINT PRIMARY KEY,
        contrato_id BIGINT,
        data_premio DATE,
        valor_premio NUMERIC(12,2)
    );

    CREATE TABLE IF NOT EXISTS bi.resgates (
        id BIGINT PRIMARY KEY,
        contrato_id BIGINT,
        data_ref DATE,
        valor_resgate NUMERIC(12,2)
    );

    -- Dimensão calendário
    CREATE TABLE IF NOT EXISTS bi.dim_calendario (
      dt date PRIMARY KEY,
      ano int, mes_num int, dia int, trimestre int, semana_ano int,
      eh_fim_de_semana boolean,
      primeiro_dia_mes date, ultimo_dia_mes date,
      ano_mes text, mes_abrev text
    );
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("✅ Schema e tabelas 'bi' verificados/criados.")

def load_raw_csvs_into_bi():
    df = pd.read_csv(RAW / "clientes.csv")
    df = to_date(df, ["data_inicio"])
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE bi.clientes RESTART IDENTITY;"))
    df.to_sql("clientes", engine, schema="bi", if_exists="append", index=False)
    print(f"[ok] bi.clientes: {len(df)}")

    df = pd.read_csv(RAW / "contratos.csv")
    df = to_date(df, ["data_inicio"])
    if "valor_mensal" in df.columns:
        df = df.rename(columns={"valor_mensal": "valor"})
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE bi.contratos RESTART IDENTITY;"))
    df.to_sql("contratos", engine, schema="bi", if_exists="append", index=False)
    print(f"[ok] bi.contratos: {len(df)}")

    df = pd.read_csv(RAW / "premios.csv")
    df = to_date(df, ["data_premio"])
    if "valor" in df.columns:
        df = df.rename(columns={"valor": "valor_premio"})
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE bi.premios RESTART IDENTITY;"))
    df.to_sql("premios", engine, schema="bi", if_exists="append", index=False)
    print(f"[ok] bi.premios: {len(df)}")

    df = pd.read_csv(RAW / "resgates.csv")
    df = to_date(df, ["data_resgate"])
    df = df.rename(columns={"data_resgate": "data_ref", "valor": "valor_resgate"})
    with engine.begin() as conn:
        conn.execute(text("TRUNCATE bi.resgates RESTART IDENTITY;"))
    df.to_sql("resgates", engine, schema="bi", if_exists="append", index=False)
    print(f"[ok] bi.resgates: {len(df)}")

def refresh_dim_calendario():
    """
    Cria/popula o calendário cobrindo:
    - do menor entre data_inicio (contratos), data_premio (prêmios), data_ref (resgates)
    - até hoje + 365 dias
    """
    sql = """
    WITH limites AS (
      SELECT
        COALESCE(
          LEAST(
            (SELECT MIN(data_inicio) FROM bi.contratos),
            (SELECT MIN(data_premio) FROM bi.premios),
            (SELECT MIN(data_ref)    FROM bi.resgates)
          ),
          DATE '2018-01-01'
        )::date                                           AS start_date,
        (CURRENT_DATE + INTERVAL '365 days')::date        AS end_date
    )
    INSERT INTO bi.dim_calendario (dt, ano, mes_num, dia, trimestre, semana_ano, eh_fim_de_semana,
                                   primeiro_dia_mes, ultimo_dia_mes, ano_mes, mes_abrev)
    SELECT
      d::date                                           AS dt,
      EXTRACT(YEAR  FROM d)::int                        AS ano,
      EXTRACT(MONTH FROM d)::int                        AS mes_num,
      EXTRACT(DAY   FROM d)::int                        AS dia,
      EXTRACT(QUARTER FROM d)::int                      AS trimestre,
      EXTRACT(WEEK    FROM d)::int                      AS semana_ano,
      (EXTRACT(ISODOW FROM d) IN (6,7))                 AS eh_fim_de_semana,
      date_trunc('month', d)::date                      AS primeiro_dia_mes,
      (date_trunc('month', d) + INTERVAL '1 month - 1 day')::date AS ultimo_dia_mes,
      to_char(d, 'YYYY-MM')                             AS ano_mes,
      to_char(d, 'Mon')                                 AS mes_abrev
    FROM limites l
    CROSS JOIN generate_series(l.start_date, l.end_date, INTERVAL '1 day') AS gs(d)
    ON CONFLICT (dt) DO NOTHING;
    """
    with engine.begin() as conn:
        conn.execute(text(sql))
    print("🗓️  bi.dim_calendario atualizado.")

def create_analytics_views():
    ddl = f"""
    CREATE SCHEMA IF NOT EXISTS analytics AUTHORIZATION {PG_USER};

    CREATE OR REPLACE VIEW analytics.dim_calendario AS
    SELECT
      dt AS date,
      dt,
      ano, mes_num, dia, trimestre, semana_ano,
      eh_fim_de_semana, primeiro_dia_mes, ultimo_dia_mes,
      ano_mes, mes_abrev
    FROM bi.dim_calendario;

    CREATE OR REPLACE VIEW analytics.dim_cliente AS
    SELECT
      id AS cliente_id,
      nome, estado, idade, faixa_etaria, renda_mensal, data_inicio, cpf_cnpj
    FROM bi.clientes;

    CREATE OR REPLACE VIEW analytics.fact_contrato AS
    SELECT
      id AS contrato_id,
      cliente_id,
      data_inicio,
      valor,
      status,
      tipo_titulo
    FROM bi.contratos;

    CREATE OR REPLACE VIEW analytics.fact_premio AS
    SELECT
      contrato_id,
      data_premio,
      valor_premio
    FROM bi.premios;

    CREATE OR REPLACE VIEW analytics.fact_resgate AS
    SELECT
      contrato_id,
      data_ref AS data_resgate,
      valor_resgate
    FROM bi.resgates;

    GRANT USAGE ON SCHEMA analytics TO {PG_USER};
    GRANT SELECT ON ALL TABLES IN SCHEMA analytics TO {PG_USER};
    ALTER DEFAULT PRIVILEGES IN SCHEMA analytics GRANT SELECT ON TABLES TO {PG_USER};
    """
    with engine.begin() as conn:
        conn.execute(text(ddl))
    print("🧭 Views 'analytics.*' criadas/atualizadas.")

def main():
    create_schema_and_tables()
    load_raw_csvs_into_bi()
    refresh_dim_calendario()
    create_analytics_views()
    print("✅ Carga completa! bi.* + analytics.* prontos para o Power BI.")

if __name__ == "__main__":
    main()
