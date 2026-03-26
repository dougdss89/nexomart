import asyncio
import argparse
import sys
import os
import random
from datetime import datetime, timedelta

# Ajustar path para importar módulos da API
sys.path.append(os.path.abspath(os.path.join(os.path.dirname(__file__), "../../../")))

from sqlalchemy import select
from api.database import AsyncSessionLocal
from api.services.sales.sales_event_log_service import SalesEventLogService
from api.schemas.sales.sales_eventlog import SalesOrderEventLogSchema, SalesEventContext, GPSLocation
from api.samples.sales_eventlog import SALES_EVENT_LOG_TEMPLATES
from api.models.sales.salesorder import SalesOrderModel

async def seed_sales_eventlogs(count_orders: int):
    """
    Seeder para Logs de Eventos de Pedidos com Integridade Dimensional.
    Gera trilhas de eventos realistas vinculadas a pedidos REAIS do banco.
    """
    async with AsyncSessionLocal() as session:
        # 1. Buscar ordens reais para vincular os eventos
        order_ids = (await session.execute(select(SalesOrderModel.salesorder_id))).scalars().all()

        if not order_ids:
            print("⚠️ Nenhuma ordem (SalesOrder) encontrada no banco! Rode o seeder de SalesOrder primeiro.")
            return

        service = SalesEventLogService(session)
        
        all_events = []
        
        # Seleciona um subset ou todas as ordens dependendo do count_orders solicitado
        selected_orders = random.sample(order_ids, min(len(order_ids), count_orders))
        
        for order_id in selected_orders:
            # Decide quantos eventos esse pedido teve (1 a 5 estágios)
            stages = random.randint(1, len(SALES_EVENT_LOG_TEMPLATES))
            
            # Data base retroativa de 1 a 15 dias
            base_date = datetime.now() - timedelta(days=random.randint(1, 15))
            
            for i in range(stages):
                template = SALES_EVENT_LOG_TEMPLATES[i]
                
                # Cada evento ocorre algumas horas depois do anterior para simular fluxo logístico
                event_date = base_date + timedelta(hours=i * random.randint(2, 24))
                
                event = SalesOrderEventLogSchema(
                    event_id=None, # Auto-increment
                    order_id=order_id,
                    event_type=template["event_type"],
                    event_date=event_date,
                    event_data=SalesEventContext(
                        user_id=random.randint(1, 50),
                        reason=template["reason"],
                        affected_line_ids=[random.randint(1, 100) for _ in range(random.randint(1,3))],
                        gps_location=GPSLocation(
                            lat=random.uniform(-23.6, -23.4), # Simulação região metropolitana SP
                            long=random.uniform(-46.8, -46.5)
                        )
                    )
                )
                all_events.append(event)
        
        print(f"📈 Gerando trilhas de {len(all_events)} eventos para {len(selected_orders)} pedidos reais...")
        
        try:
            await service.process_bulk_events(all_events)
            await session.commit()
            print(f"✅ Seeder finalizado com sucesso!")
        except Exception as e:
            await session.rollback()
            print(f"❌ Erro na carga: {e}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Seeder para Sales Event Logs")
    parser.add_argument("--orders", type=int, default=10, help="Quantidade de pedidos para gerar eventos")
    args = parser.parse_args()
    
    asyncio.run(seed_sales_eventlogs(args.orders))
