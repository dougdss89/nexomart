from typing import List, Dict, Any
from api.schemas.product.production_product_category import ProductCategory
from api.schemas.product.production_product_model import ProductModel
import random
from faker import Faker

fake = Faker('pt_BR')

class BaseProductSample:
    """Base class for pseudo-realistic product generation patterns."""
    category: ProductCategory
    model: ProductModel

    # Variation attributes to explode the combinations
    base_names: List[str] = []
    brands: List[str] = []
    colors: List[str] = []
    materials: List[str] = []
    sizes: List[str] = [None] # Default, overridden where applicable
    
    list_price_range: tuple = (10.0, 100.0)
    markup_range: tuple = (1.5, 3.0)
    weight_range: tuple = (0.1, 1.0)
    
    base_dimensions: Dict[str, float] = {"height": 10.0, "width": 10.0, "depth": 10.0}

    @classmethod
    def generate_random_product(cls) -> Dict[str, Any]:
        """Generates a random product based on the class's characteristics."""

        base_name = random.choice(cls.base_names)
        brand = random.choice(cls.brands)
        color = random.choice(cls.colors)
        material = random.choice(cls.materials)
        size = random.choice(cls.sizes)

        # Unique name combining commercial characteristics
        product_name = f"{base_name} {brand} - {color}"
        if size:
            product_name += f" Tam {size}"

        # Financials
        list_price = round(random.uniform(*cls.list_price_range), 2)
        price = round(list_price * random.uniform(*cls.markup_range), 2)
        tax = round(price * random.choice([0.12, 0.18, 0.25]), 2)

        # Logistics
        weight = round(random.uniform(*cls.weight_range), 3)
        dimensions = {k: round(v * random.uniform(0.9, 1.1), 1) for k, v in cls.base_dimensions.items()}

        # Pseudo-real SKU (Brand-Model-Color-Random)
        brand_prefix = brand[:3].upper().replace(" ", "")
        model_prefix = cls.model.value[:4].upper().replace("_", "")
        color_prefix = color[:3].upper()
        sku = f"{brand_prefix}-{model_prefix}-{color_prefix}-{fake.unique.random_int(min=10000, max=99999)}"

        details = {
            "color": color,
            "list_price": list_price,
            "price": price,
            "tax": tax,
            "weight": weight,
            "material": material,
            "brand": brand,
            "dimensions": dimensions,
            "sku": sku
        }
        
        if size:
            details["size"] = size
            
        # --- DATA ENGINEERING CHALLENGES (INTENTIONAL ANOMALIES) ---
        # 1. Negative or zeroed-out weight (2% chance)
        if random.random() < 0.02:
            details["weight"] = -abs(details["weight"])

        # 2. Missing or incorrectly zeroed-out tax (3% chance)
        if random.random() < 0.03:
            details["tax"] = 0.0

        # 3. String noise / incorrect UPPERCASE standardization (5% chance)
        if random.random() < 0.05:
            details["brand"] = str(details["brand"]).upper()
            details["color"] = str(details["color"]).lower()
            product_name = product_name.upper()

        # 4. Missing Dimension (hole in the JSON payload)
        if random.random() < 0.04:
            details["dimensions"].pop("depth", None)

        # 5. Silent Schema Evolution: inject extra keys into the JSONB
        if random.random() < 0.15:
            details["is_refurbished"] = random.choice([True, False])
        if random.random() < 0.10:
            details["warranty_months"] = random.choice([3, 12, 24, "Sem Garantia"])
        if random.random() < 0.05:
            details["voltage"] = random.choice(["110V", "220V", "Bivolt"])

        return {
            "category": cls.category,
            "model": cls.model,
            "product_name": product_name,
            "details": details
        }

class SmartphoneSamples(BaseProductSample):
    category = ProductCategory.ELECTRONICS
    model = ProductModel.SMARTPHONE
    base_names = [
        "Smartphone", "Celular Top de Linha", "Celular Intermediário", 
        "Aparelho 5G", "Smartphone Pro Max", "Smartphone Ultra",
        "Celular Gamer", "Smartphone Flip"
    ]
    brands = ["Apple", "Samsung", "Motorola", "Xiaomi", "Asus", "Realme", "Poco"]
    colors = ["Preto Metálico", "Branco Perolado", "Azul Oceano", "Titânio Natural", "Rosa Claro", "Verde Esmeralda"]
    materials = ["Vidro/Alumínio", "Titânio/Vidro", "Plástico/Vidro", "Cerâmica"]
    
    list_price_range = (800.0, 4500.0)
    markup_range = (1.2, 1.8)
    weight_range = (0.150, 0.250)
    base_dimensions = {"height": 16.0, "width": 7.5, "depth": 0.8}

class LaptopSamples(BaseProductSample):
    category = ProductCategory.ELECTRONICS
    model = ProductModel.LAPTOP_COMPUTING
    base_names = [
        "Notebook Gamer", "Ultrabook Corporativo", "Notebook Premium",
        "Laptop para Estudantes", "MacBook Equivalente", "Workstation Mobile",
        "Notebook 2 em 1", "Laptop Ultra Leve"
    ]
    brands = ["Dell", "HP", "Lenovo", "Apple", "Asus", "Acer", "Avell"]
    colors = ["Prata Espacial", "Preto Fosco", "Grafite", "Branco Neve", "Azul Metálico"]
    materials = ["Alumínio", "Fibra de Carbono", "Plástico ABS", "Magnésio"]
    
    list_price_range = (2000.0, 15000.0)
    markup_range = (1.3, 2.0)
    weight_range = (1.2, 3.5)
    base_dimensions = {"height": 2.0, "width": 35.0, "depth": 25.0}

class WearableSamples(BaseProductSample):
    category = ProductCategory.ELECTRONICS
    model = ProductModel.WEARABLE_TECH
    base_names = [
        "Smartwatch GPS", "Relógio Inteligente Pro", "Pulseira Fit",
        "Monitor Cardíaco", "Smart Band Esportiva", "Relógio Clássico Inteligente"
    ]
    brands = ["Apple", "Samsung", "Garmin", "Amazfit", "Xiaomi", "Polar"]
    colors = ["Preto Meia-noite", "Branco Estelar", "Prateado", "Ouro Rosa", "Verde Oliva"]
    materials = ["Alumínio", "Aço Inoxidável", "Titânio", "Polímero"]
    sizes = ["40mm", "41mm", "44mm", "45mm", "49mm", "Único"]
    
    list_price_range = (300.0, 3500.0)
    markup_range = (1.4, 2.2)
    weight_range = (0.030, 0.080)
    base_dimensions = {"height": 4.5, "width": 4.0, "depth": 1.2}

class LargeApplianceSamples(BaseProductSample):
    category = ProductCategory.HOME_APPLIANCE
    model = ProductModel.LARGE_APPLIANCE
    base_names = [
        "Geladeira Frost Free", "Geladeira French Door", "Máquina de Lavar Frontal",
        "Lavadora e Secadora", "Fogão 5 Bocas de Embutir", "Forno Elétrico de Embutir",
        "Freezer Vertical", "Geladeira Duplex"
    ]
    brands = ["Brastemp", "Electrolux", "Samsung", "LG", "Consul", "Panasonic"]
    colors = ["Inox", "Branco Escovado", "Preto Onix", "Titânio", "Espelhado"]
    materials = ["Aço Inox", "Aço Carbono/Vidro", "Metais Diversos"]
    
    list_price_range = (1500.0, 8000.0)
    markup_range = (1.3, 1.8)
    weight_range = (40.0, 120.0)
    base_dimensions = {"height": 180.0, "width": 70.0, "depth": 70.0}

class MensApparelSamples(BaseProductSample):
    category = ProductCategory.CLOTHING
    model = ProductModel.MENS_APPAREL
    base_names = [
        "Camiseta Básica Premium", "Polo Piquet Clássica", "Calça Jeans Slim Fit",
        "Bermuda de Sarja", "Jaqueta Corta-Vento Esportiva", "Moletom Canguru",
        "Camisa Social Manga Longa", "Calça Jogger Performance"
    ]
    brands = ["Levi's", "Hering", "Reserva", "Calvin Klein", "Adidas", "Nike", "Vans"]
    colors = ["Preto Puro", "Azzul Marinho Clássico", "Branco Gelo", "Cinza Mescla", "Vinho", "Verde Militar"]
    materials = ["100% Algodão", "Algodão Egípcio", "Jeans Premium", "Poliéster/Elastano", "Moletom Flanelado"]
    sizes = ["PP", "P", "M", "G", "GG", "XG", "38", "40", "42", "44", "46"]
    
    list_price_range = (20.0, 250.0)
    markup_range = (2.0, 4.0)
    weight_range = (0.150, 0.800)
    base_dimensions = {"height": 3.0, "width": 25.0, "depth": 30.0} # Folded in packaging

class WomensApparelSamples(BaseProductSample):
    category = ProductCategory.CLOTHING
    model = ProductModel.WOMENS_APPAREL
    base_names = [
        "Vestido Midi Estampado", "Blusa de Seda", "Calça Pantalona Alfaiataria",
        "Saia Plissada", "Jaqueta Jeans Oversized", "Cropped Canelado",
        "Cardigan de Tricô", "Blazer Acinturado"
    ]
    brands = ["Farm", "Zara", "Renner", "Amaro", "Animale", "Le Lis Blanc", "Hering"]
    colors = ["Preto Básico", "Off-White", "Rosé", "Azul Serenity", "Vermelho Carmim", "Estampado Tropical"]
    materials = ["Viscose", "Seda Pura", "Linho Misto", "Jeans Leve", "Tricô", "Algodão Sustentável"]
    sizes = ["PP", "P", "M", "G", "GG", "34", "36", "38", "40", "42", "44"]
    
    list_price_range = (30.0, 350.0)
    markup_range = (2.0, 4.5)
    weight_range = (0.100, 0.600)
    base_dimensions = {"height": 3.0, "width": 25.0, "depth": 30.0}

class AthleticFootwearSamples(BaseProductSample):
    category = ProductCategory.FOOTWEAR
    model = ProductModel.ATHLETIC_FOOTWEAR
    base_names = [
        "Tênis de Corrida Performance", "Tênis Crossfit Elite", "Chuteira Campo Pro",
        "Tênis Basquete High", "Tênis Caminhada Conforto", "Tênis Skate PRO"
    ]
    brands = ["Nike", "Adidas", "Asics", "Puma", "Mizuno", "Under Armour", "Olympikus"]
    colors = ["Preto/Branco", "Azul/Laranja", "Neon", "Branco Total", "Cinza/Vermelho", "Preto Total"]
    materials = ["Mesh/Borracha", "Sintético/EVA", "Flyknit", "Borracha Premium"]
    sizes = [str(i) for i in range(35, 46)]
    
    list_price_range = (100.0, 600.0)
    markup_range = (1.5, 3.0)
    weight_range = (0.400, 0.900)
    base_dimensions = {"height": 12.0, "width": 20.0, "depth": 35.0} # Shoe box

class DietarySupplementsSamples(BaseProductSample):
    category = ProductCategory.HEALTH_WELLNESS
    model = ProductModel.DIETARY_SUPPLEMENTS
    base_names = [
        "Whey Protein Concentrado 1KG", "Whey Protein Isolado 900g", "Creatina Monohidratada 250g",
        "Pré-Treino Explosivo", "BCAA 2400", "Multivitamínico Completo",
        "Glutamina 300g", "Colágeno Hidrolisado"
    ]
    brands = ["Growth Supplements", "Max Titanium", "Integral Médica", "Optimum Nutrition", "Essential Nutrition", "Probiótica"]
    colors = ["Embalagem Preta", "Embalagem Vermelha", "Embalagem Azul", "Pote Neutro", "Refil Metálico"]
    materials = ["Pó", "Cápsulas", "Líquido", "Gomas"]
    
    list_price_range = (30.0, 300.0)
    markup_range = (1.5, 2.5)
    weight_range = (0.300, 2.500)
    base_dimensions = {"height": 25.0, "width": 15.0, "depth": 15.0}

class MusicalInstrumentsSamples(BaseProductSample):
    category = ProductCategory.MUSICAL_INSTRUMENTS
    model = ProductModel.ELECTRIC_GUITAR
    base_names = ["Guitarra Stratocaster", "Guitarra Les Paul", "Baixo Ativo 5 Cordas", "Teclado Arranjador 61 Teclas", "Bateria Acústica", "Violão Eletroacústico"]
    brands = ["Fender", "Gibson", "Yamaha", "Ibanez", "Roland", "Tagima"]
    colors = ["Sunburst", "Preto", "Branco Vintage", "Vermelho Cereja", "Madeira Natural"]
    materials = ["Mogno", "Maple", "Alder", "Rosewood"]
    
    list_price_range = (500.0, 8000.0)
    markup_range = (1.4, 2.5)
    weight_range = (3.0, 25.0)
    base_dimensions = {"height": 110.0, "width": 40.0, "depth": 15.0}

class MensWatchesSamples(BaseProductSample):
    category = ProductCategory.JEWELRY_WATCHES
    model = ProductModel.MENS_WATCHES
    base_names = ["Relógio Automático Clássico", "Relógio Cronógrafo Esportivo", "Relógio Mergulho 200m", "Relógio Minimalista Couro"]
    brands = ["Seiko", "Casio", "Citizen", "Orient", "Rolex", "Tag Heuer", "G-Shock"]
    colors = ["Prata/Preto", "Ouro", "Preto Fosco", "Couro Marrom", "Prata/Azul"]
    materials = ["Aço Inoxidável", "Titânio", "Couro Genuíno", "Resina"]
    
    list_price_range = (150.0, 50000.0)
    markup_range = (1.5, 3.5)
    weight_range = (0.050, 0.200)
    base_dimensions = {"height": 5.0, "width": 5.0, "depth": 1.5}

class PowerToolsSamples(BaseProductSample):
    category = ProductCategory.HOME_IMPROVEMENT
    model = ProductModel.POWER_TOOLS
    base_names = ["Furadeira de Impacto", "Parafusadeira a Bateria", "Serra Circular", "Esmerilhadeira Angular", "Martelete Perfurador"]
    brands = ["Bosch", "Makita", "DeWalt", "Black+Decker", "Skil", "Vonder"]
    colors = ["Azul Escuro", "Amarelo/Preto", "Verde Água", "Laranja", "Vermelho Ouro"]
    materials = ["Plástico Reforçado ABS", "Aço", "Alumínio Fundido"]
    
    list_price_range = (80.0, 1500.0)
    markup_range = (1.3, 2.0)
    weight_range = (1.5, 6.0)
    base_dimensions = {"height": 25.0, "width": 30.0, "depth": 10.0}

class GamingConsoleSamples(BaseProductSample):
    category = ProductCategory.VIDEO_GAMES
    model = ProductModel.GAMING_CONSOLES
    base_names = ["Console Next-Gen 1TB", "Console Digital Edition", "Console Portátil Híbrido", "Console Retrô Classic"]
    brands = ["Sony", "Microsoft", "Nintendo", "Sega", "Steam"]
    colors = ["Branco Gelo", "Preto Metálico", "Neon Blue/Red", "Cinza Clássico"]
    materials = ["Plástico ABS", "Silicone", "Alumínio"]
    
    list_price_range = (1500.0, 4000.0)
    markup_range = (1.1, 1.3) # Typical low margin for consoles
    weight_range = (0.400, 4.500)
    base_dimensions = {"height": 40.0, "width": 10.0, "depth": 26.0}

def generate_randomized_catalog(quantity: int) -> List[Dict[str, Any]]:
    """Generates a unified catalog by randomly drawing from all BaseProductSample subclasses."""
    all_products = []

    # List of all factories
    factories = BaseProductSample.__subclasses__()

    for _ in range(quantity):
        # Pick a random category
        factory = random.choice(factories)

        # Generate 1 random product from that category
        all_products.append(factory.generate_random_product())

    return all_products
