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
