import random
import pandas as pd
from faker import Faker
from pathlib import Path
from datetime import date

ROOT = Path(__file__).resolve().parents[1]
RAW = ROOT / "data" / "raw"
RAW.mkdir(parents=True, exist_ok=True)

fake = Faker("pt_BR")
random.seed(42)

ESTADOS = ["RJ", "SP", "MG", "RS", "BA", "PR", "PE", "SC", "DF", "GO"]
TIPOS_TITULO = ["Mensal", "Trimestral", "Anual"]

n_clientes = 250
clientes = []
for i in range(1, n_clientes + 1):
    nome = fake.name()
    estado = random.choice(ESTADOS)
    idade = random.randint(18, 75)
    if idade <= 25:
        faixa = "18–25"
    elif idade <= 35:
        faixa = "26–35"
    elif idade <= 45:
        faixa = "36–45"
    elif idade <= 60:
        faixa = "46–60"
    else:
        faixa = "60+"
    renda = random.choice([2000, 3000, 4000, 5000, 7000, 10000, 15000, 20000]) + random.randint(-500, 500)
    data_inicio = fake.date_between(start_date="-2y", end_date="today")
    clientes.append((i, nome, estado, idade, faixa, renda, data_inicio))

df_clientes = pd.DataFrame(
    clientes,
    columns=["id", "nome", "estado", "idade", "faixa_etaria", "renda_mensal", "data_inicio"],
)
df_clientes.to_csv(RAW / "clientes.csv", index=False)

n_contratos = 600
contratos = []
for i in range(1, n_contratos + 1):
    cliente_id = random.randint(1, n_clientes)
    valor_mensal = random.choice([50, 90, 120, 150, 200, 300, 400, 500, 800, 1000])
    data_inicio = fake.date_between(start_date="-18m", end_date="today")
    status = random.choices(["ATIVO", "RESGATADO", "CANCELADO"], weights=[0.65, 0.25, 0.1])[0]
    tipo = random.choice(TIPOS_TITULO)
    contratos.append((1000 + i, cliente_id, valor_mensal, data_inicio, status, tipo))

df_contratos = pd.DataFrame(
    contratos,
    columns=["id", "cliente_id", "valor_mensal", "data_inicio", "status", "tipo_titulo"],
)
df_contratos.to_csv(RAW / "contratos.csv", index=False)

premios = []
for i in range(1, 250):
    contrato_id = random.choice(df_contratos["id"].tolist())
    valor = round(random.uniform(1000, 150000), 2)
    data_premio = fake.date_between(start_date="-1y", end_date="today")
    premios.append((i, contrato_id, data_premio, valor))

df_premios = pd.DataFrame(premios, columns=["id", "contrato_id", "data_premio", "valor"])
df_premios.to_csv(RAW / "premios.csv", index=False)

resgates = []
contratos_resgatados = df_contratos[df_contratos["status"] == "RESGATADO"]["id"].tolist()
for i in range(1, 200):
    if not contratos_resgatados:
        break
    contrato_id = random.choice(contratos_resgatados)
    valor = round(random.uniform(200, 8000), 2)
    data_resgate = fake.date_between(start_date="-8m", end_date="today")
    resgates.append((i, contrato_id, data_resgate, valor))

df_resgates = pd.DataFrame(resgates, columns=["id", "contrato_id", "data_resgate", "valor"])
df_resgates.to_csv(RAW / "resgates.csv", index=False)

print(f"✅ CSVs gerados com sucesso em {RAW}")
print(f" - Clientes:  {len(df_clientes)} registros")
print(f" - Contratos: {len(df_contratos)} registros")
print(f" - Prêmios:   {len(df_premios)} registros")
print(f" - Resgates:  {len(df_resgates)} registros")