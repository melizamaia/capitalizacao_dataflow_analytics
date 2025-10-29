-- Base
Contratos =
DISTINCTCOUNT ( 'Contrato'[contrato_id] )

-- Status
Títulos Ativos =
CALCULATE ( [Contratos], 'Contrato'[status] = "ATIVO" )

Títulos Encerrados =
CALCULATE ( [Contratos], 'Contrato'[status] IN { "CANCELADO", "RESGATADO" } )

-- Valores
Valor Total (R$) =
SUM ( 'Contrato'[valor] )

Valor Total Ativo (R$) =
CALCULATE ( [Valor Total (R$)], 'Contrato'[status] = "ATIVO" )

Valor Médio Contrato =
DIVIDE ( [Valor Total (R$)], [Contratos] )

Prêmios Pagos (R$) =
SUM ( 'Premio'[valor_premio] )

-- Resgates
Taxa de Resgate (%) =
DIVIDE ( DISTINCTCOUNT ( 'Resgate'[contrato_id] ), [Contratos], 0 )
