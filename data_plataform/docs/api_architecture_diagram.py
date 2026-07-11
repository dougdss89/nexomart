from diagrams import Diagram, Cluster, Edge
from diagrams.programming.framework import Fastapi
from diagrams.onprem.database import PostgreSQL
from diagrams.onprem.workflow import Airflow
from diagrams.onprem.client import User
from diagrams.programming.language import Python

with Diagram("Data Platform API - 3-Layer Architecture", show=False, filename="api_architecture", direction="LR"):
    
    user = User("Data Scientist / ERP")
    airflow = Airflow("ETL / Seeders")
    
    with Cluster("FastAPI Application"):
        
        with Cluster("Routers (API Layer)"):
            routers = [
                Fastapi("Sales Router"),
                Fastapi("Person Router"),
                Fastapi("Purchasing Router"),
                Fastapi("Warehouse Router")
            ]
            
        with Cluster("Services (Business Layer)"):
            services = [
                Python("Order Service"),
                Python("Seller Service"),
                Python("Inventory Service"),
                Python("Promo Service")
            ]
            
        with Cluster("Repositories (Data Access)"):
            repos = [
                Python("Sales Repo"),
                Python("Person Repo"),
                Python("Purchasing Repo"),
                Python("Warehouse Repo")
            ]

    db = PostgreSQL("Operational DB (PostgreSQL)")
    
    # Relationships
    user >> Edge(label="REST API") >> routers
    airflow >> Edge(label="Bulk Ingest") >> routers
    airflow >> Edge(label="Internal CLI") >> Python("Seeders") >> db
    
    for i in range(len(routers)):
        routers[i] >> services[i] >> repos[i] >> db
