import os
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine, text
from reportlab.lib.pagesizes import A4
from reportlab.lib import colors
from reportlab.platypus import SimpleDocTemplate, Paragraph, Spacer, Image, Table, TableStyle
from reportlab.lib.styles import getSampleStyleSheet
from pathlib import Path
from dotenv import load_dotenv

ROOT = Path(__file__).resolve().parents[1]
REPORT_PATH = ROOT / "report" / "report_brasilcap.pdf"
IMG_DIR = ROOT / "report" / "imgs"
IMG_DIR.mkdir(parents=True, exist_ok=True)

load_dotenv(ROOT / ".env")

DB_URL = os.getenv("DATABASE_URL", "postgresql+psycopg2://postgres:postgres@localhost:5432/brasilcap")
engine = create_engine(DB_URL)

styles = getSampleStyleSheet()
style_h1 = styles["Heading1"]
style_h2 = styles["Heading2"]
style_body = styles["BodyText"]

def gerar_graficos():
    print(" - Gerando gráficos...")

    df_kpi = pd.read_sql("SELECT * FROM analytics.kpi_contribuicoes_mensais ORDER BY mes", engine)
    df_cli = pd.read_sql("SELECT id, faixa_etaria, renda_mensal FROM analytics.dim_cliente", engine)
    df_con = pd.read_sql("SELECT id, cliente_id, valor_mensal, tipo_titulo, status FROM analytics.fact_contrato", engine)

    plt.figure(figsize=(6, 3))
    plt.plot(df_kpi["mes"], df_kpi["total_mensal"], marker="o")
    plt.title("Evolução das Contribuições Mensais")
    plt.xlabel("Mês")
    plt.ylabel("Total Mensal (R$)")
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(IMG_DIR / "grafico_contribuicoes.png")
    plt.close()

    df_merge = df_con.merge(df_cli, left_on="cliente_id", right_on="id", how="left")
    faixa_counts = df_merge["faixa_etaria"].value_counts().sort_index()

    plt.figure(figsize=(5, 3))
    faixa_counts.plot(kind="bar", color="#3C8DBC")
    plt.title("Distribuição de Contratos por Faixa Etária")
    plt.xlabel("Faixa Etária")
    plt.ylabel("Quantidade de Contratos")
    plt.tight_layout()
    plt.savefig(IMG_DIR / "grafico_faixa_etaria.png")
    plt.close()

    tipo_media = df_con.groupby("tipo_titulo")["valor_mensal"].mean().round(2)
    plt.figure(figsize=(5, 3))
    tipo_media.plot(kind="bar", color="#FF9800")
    plt.title("Valor Médio Mensal por Tipo de Título")
    plt.xlabel("Tipo de Título")
    plt.ylabel("Valor Médio (R$)")
    plt.tight_layout()
    plt.savefig(IMG_DIR / "grafico_tipo_titulo.png")
    plt.close()

    return df_kpi, df_con, df_cli

def gerar_pdf():
    print(" - Montando PDF em", REPORT_PATH)
    df_kpi, df_con, df_cli = gerar_graficos()

    doc = SimpleDocTemplate(str(REPORT_PATH), pagesize=A4)
    story = []

    story.append(Paragraph("Relatório de Análise – Brasilcap Analytics", style_h1))
    story.append(Spacer(1, 12))
    story.append(Paragraph("Automação de capitalização e indicadores de desempenho", style_body))
    story.append(Spacer(1, 18))

    total_clientes = len(df_cli)
    total_contratos = len(df_con)
    total_ativos = df_con[df_con["status"] == "ATIVO"].shape[0]
    total_mensal = df_kpi["total_mensal"].sum()

    data = [
        ["Indicador", "Valor"],
        ["Clientes Cadastrados", f"{total_clientes:,}".replace(",", ".")],
        ["Contratos Totais", f"{total_contratos:,}".replace(",", ".")],
        ["Contratos Ativos", f"{total_ativos:,}".replace(",", ".")],
        ["Total Contribuído (R$)", f"{total_mensal:,.2f}".replace(",", ".")],
    ]

    table = Table(data, hAlign="LEFT", colWidths=[220, 150])
    table.setStyle(
        TableStyle([
            ("BACKGROUND", (0, 0), (-1, 0), colors.lightblue),
            ("TEXTCOLOR", (0, 0), (-1, 0), colors.whitesmoke),
            ("ALIGN", (1, 1), (-1, -1), "RIGHT"),
            ("FONTNAME", (0, 0), (-1, 0), "Helvetica-Bold"),
            ("INNERGRID", (0, 0), (-1, -1), 0.25, colors.gray),
            ("BOX", (0, 0), (-1, -1), 0.25, colors.gray),
        ])
    )

    story.append(table)
    story.append(Spacer(1, 18))

    for nome, titulo in [
        ("grafico_contribuicoes.png", "Evolução das Contribuições Mensais"),
        ("grafico_faixa_etaria.png", "Distribuição de Contratos por Faixa Etária"),
        ("grafico_tipo_titulo.png", "Valor Médio Mensal por Tipo de Título"),
    ]:
        story.append(Paragraph(titulo, style_h2))
        story.append(Image(str(IMG_DIR / nome), width=450, height=250))
        story.append(Spacer(1, 18))

    doc.build(story)
    print(">>> Relatório gerado com sucesso.")


if __name__ == "__main__":
    gerar_pdf()
