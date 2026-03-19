import uuid
import random
from datetime import datetime, timedelta
from decimal import Decimal

def get_purchase_orderdetail_seeder(headers: list[dict], product_ids: list[int]) -> list[dict]:
    """
    Gera Purchase Order Details baseados nos headers já gerados.
    Garante matematicamente que a soma de (order_qty * unit_price) é igual ao sub_total do header.
    Garante consistência lógica nas quantidades de estoque, rejeições, e recebimentos.
    """
    batch = []

    for header in headers:
        header_id = header["purchaseorderheader_id"]
        sub_total = Decimal(str(header["sub_total"]))
        
        # Decide quantos itens este pedido terá
        num_items = random.randint(1, 5)
        
        # Variáveis para controle do sub_total e distribuição exata
        current_sum = Decimal('0.0000')
        
        for i in range(num_items):
            product_id = random.choice(product_ids)
            
            # Se for o último item, ele absorve todo o resto do sub_total
            if i == num_items - 1:
                remainder = sub_total - current_sum
                
                # Se acontecer do remainder for <= 0 devido às escolhas randômicas,
                # apenas ignoramos este e os próximos itens (o pedido terá menos itens do que num_items planejado)
                if remainder <= 0:
                    break
                    
                order_qty = 1
                unit_price = remainder
            else:
                # Pegamos um pedaço seguro do sub_total restante
                safe_max = float(sub_total - current_sum) / (num_items - i)
                if safe_max < 1.0:
                    continue # não gera item
                
                order_qty = random.randint(1, 10)
                # Garante que o item vai custar no máximo o 'safe_max'
                max_price_per_item = safe_max / order_qty
                
                price = round(random.uniform(0.1, max_price_per_item), 4)
                unit_price = Decimal(str(price))
                
                item_total = Decimal(str(order_qty)) * unit_price
                current_sum += item_total
                
            # Calcular datas e status com base no Header
            due_date = header["ship_date"] + timedelta(days=random.randint(1, 10))
            last_receipt_date = None
            
            status = header["status"]
            
            if status == "completed":
                # Pedido concluído, tudo foi recebido
                rejected_qty = random.randint(0, order_qty // 5) if order_qty > 5 else 0
                stocked_qty = order_qty # A quantidade alvo que precisávamos em estoque
                received_qty = stocked_qty + rejected_qty
                
                last_receipt_date = header["orderfinished_date"] if header.get("orderfinished_date") else header["ship_date"] + timedelta(days=5)
                is_fully_received = True

            elif status == "approved":
                # Pedido aprovado pode estar parcialmente recebido
                is_partially = random.choice([True, False])
                if is_partially:
                    received_qty = random.randint(1, order_qty - 1) if order_qty > 1 else 0
                    if received_qty > 0:
                        rejected_qty = random.randint(0, received_qty)
                        stocked_qty = received_qty - rejected_qty
                        last_receipt_date = header["ship_date"] + timedelta(days=random.randint(1, 5))
                    else:
                        rejected_qty = 0
                        stocked_qty = 0
                else:
                    received_qty = 0
                    rejected_qty = 0
                    stocked_qty = 0
                is_fully_received = False

            else:
                # pending, waiting_approval, canceled, rejected etc... não tem recebimento físico
                received_qty = 0
                rejected_qty = 0
                stocked_qty = 0
                is_fully_received = False
            
            # Gera a PK em formato PO-XXXXXX-XX (ex: PO-100001-01)
            detail_id = f"{header_id}-{(i+1):02d}"
            
            detail = {
                "purchaseorderdetail_id": detail_id,
                "purchaseorderheader_id": header_id,
                "product_id": product_id,
                
                "due_date": due_date,
                "last_receipt_date": last_receipt_date,
                
                "order_qty": order_qty,
                "unit_price": unit_price,
                
                "received_qty": received_qty,
                "rejected_qty": rejected_qty,
                "stocked_qty": stocked_qty,
                
                "is_fully_received": is_fully_received,
                
                "created_at": header["created_at"],
                "updated_at": header["updated_at"],
                "is_active": True,
                "row_guid": uuid.uuid4()
            }
            batch.append(detail)
            
    return batch

if __name__ == "__main__":
    from api.seeder.purchasing.purchase_purchaseorderheader import get_purchase_order_header
    import json
    
    dummy_vendors = list(range(1, 51))
    dummy_employees = list(range(1, 11))
    dummy_products = list(range(1, 101))
    
    print("Gerando headers...")
    headers = get_purchase_order_header(vendor_ids=dummy_vendors, employee_ids=dummy_employees, num_records=5)
    
    print("Gerando details...")
    details = get_purchase_orderdetail_seeder(headers, product_ids=dummy_products)
    
    print(f"Total de cabeçalhos gerados: {len(headers)}")
    print(f"Total de details gerados: {len(details)}")
    print("-" * 50)
    
    # Verifica a consistência de 1 pedido
    h1 = headers[0]
    d1 = [d for d in details if d["purchaseorderheader_id"] == h1["purchaseorderheader_id"]]
    
    sub_total = h1["sub_total"]
    calc_total = sum(d["order_qty"] * d["unit_price"] for d in d1)
    
    print(f"Header SubTotal: {sub_total}")
    print(f"Details Calc Total: {calc_total}")
    print(f"Match: {sub_total == calc_total}")
    
    print(json.dumps(d1, indent=2, default=str))