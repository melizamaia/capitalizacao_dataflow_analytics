Títulos Ativos =
CALCULATE (
    DISTINCTCOUNT ( 'analytics_fact_contrato'[id] ),
    'analytics_fact_contrato'[status] = "ATIVO"
)