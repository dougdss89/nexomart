from api.schemas.sales.sales_eventlog import SalesEventType

SALES_EVENT_LOG_TEMPLATES = [
    {
        "event_type": SalesEventType.PAYMENT_APPROVED,
        "reason": "Pagamento confirmado via Gateway",
        "description": "Transação financeira concluída com sucesso."
    },
    {
        "event_type": SalesEventType.IN_SEPARATING,
        "reason": "Pedido enviado para o WMS",
        "description": "O armazém iniciou a coleta dos itens."
    },
    {
        "event_type": SalesEventType.ITEM_PICKED,
        "reason": "Triagem concluída",
        "description": "Todos os itens foram bipados e embalados."
    },
    {
        "event_type": SalesEventType.IN_TRANSIT,
        "reason": "Postado na Transportadora",
        "description": "O pacote saiu do centro de distribuição."
    },
    {
        "event_type": SalesEventType.DELIVERED,
        "reason": "Entrega realizada",
        "description": "O cliente recebeu o pedido no endereço cadastrado."
    }
]

# Note: In documentation we use IN_SEPARATION, checking schema for exact enum name...
# SalesEventType has IN_SEPARATION (line 12 of schema).
# Correcting template mapping to match schema exactly.

SALES_EVENT_LOG_TEMPLATES = [
    {
        "event_type": "payment_approved",
        "reason": "Pagamento confirmado via Gateway"
    },
    {
        "event_type": "in_separation",
        "reason": "Pedido enviado para o WMS"
    },
    {
        "event_type": "item_picked",
        "reason": "Triagem concluída"
    },
    {
        "event_type": "in_transit",
        "reason": "Postado na Transportadora"
    },
    {
        "event_type": "delivered",
        "reason": "Entrega realizada"
    }
]
