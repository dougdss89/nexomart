# Sample data for Sales Channels
# Matching SalesChannelSchema components

SALES_CHANNEL_SAMPLES = [
    {
        "saleschannel_id": 1,
        "saleschannel_code": "WEB-DIRECT",
        "saleschannel_name": "E-commerce Website",
        "saleschannel_info": {
            "channel_type": "web",
            "allows_return": True,
            "requires_pos_auth": False,
            "default_currency": "BRL",
            "stock_source_priority": ["warehouse_sp", "warehouse_mg"]
        }
    },
    {
        "saleschannel_id": 2,
        "saleschannel_code": "STORE-SP-01",
        "saleschannel_name": "Flagship Store São Paulo",
        "saleschannel_info": {
            "channel_type": "store",
            "allows_return": True,
            "requires_pos_auth": True,
            "default_currency": "BRL",
            "stock_source_priority": ["store_sp_01", "warehouse_sp"]
        }
    },
    {
        "saleschannel_id": 3,
        "saleschannel_code": "MKT-PLACE-ML",
        "saleschannel_name": "Mercado Livre Marketplace",
        "saleschannel_info": {
            "channel_type": "marketplace",
            "allows_return": False,
            "requires_pos_auth": False,
            "default_currency": "BRL",
            "stock_source_priority": ["warehouse_sp"]
        }
    },
    {
        "saleschannel_id": 4,
        "saleschannel_code": "APP-MOBILE",
        "saleschannel_name": "Native Mobile App",
        "saleschannel_info": {
            "channel_type": "app",
            "allows_return": True,
            "requires_pos_auth": False,
            "default_currency": "BRL",
            "stock_source_priority": ["warehouse_sp", "warehouse_rj"]
        }
    },
    {
        "saleschannel_id": 5,
        "saleschannel_code": "B2B-DIRECT",
        "saleschannel_name": "B2B Corporate Sales",
        "saleschannel_info": {
            "channel_type": "b2b",
            "allows_return": False,
            "requires_pos_auth": False,
            "default_currency": "BRL",
            "stock_source_priority": ["warehouse_sp", "factory_direct"]
        }
    }
]
