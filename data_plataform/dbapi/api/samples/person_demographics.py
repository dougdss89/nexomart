"""
Amostra estática de perfis demográficos para popular a tabela de clientes (Customer).
Visa criar massa de dados heterogênea para Data Engineering, simulando perfis muito
variados que fogem do estereótipo padrão (T.I.).
"""

INTERESTS_BANK = [
    # Esportes e Lazer
    "Futebol", "Ciclismo", "Natação", "Academia", "Corrida", "Artes Marciais",
    "Tênis", "Pilates", "Yoga", "Trilhas", "Acampamento", "Surf",
    
    # Entretenimento e Cultura
    "Cinema", "Música Clássica", "Rock", "Sertanejo", "Literatura de Ficção",
    "Biografias", "Teatro", "Museus", "Séries Turcas", "K-Pop", "Animes",
    "Video Games", "Jogos de Tabuleiro", "E-sports",

    # Cotidiano e Família
    "Culinária", "Churrasco", "Vinhos", "Cervejas Artesanais", "Cafés Especiais",
    "Jardinagem", "Animais de Estimação (Cães)", "Gatos", "Decoração", "Faça Você Mesmo (DIY)",
    "Educação Infantil",
    
    # Mercado e Mundo
    "Mercado Financeiro", "Criptomoedas", "Empreendedorismo", "Tecnologia", 
    "Sustentabilidade", "Carros Antigos", "Motos", "Fotografia", "Moda", "Beleza"
]

OCCUPATIONS = [
    # Saúde
    "Médico(a) Clínico Geral", "Enfermeiro(a)", "Fisioterapeuta", "Odontologista",
    "Nutricionista", "Psicólogo(a)", "Farmacêutico(a)",
    
    # Educação
    "Professor(a) de Ensino Médio", "Professor(a) Universitário", "Pedagogo(a)",
    "Diretor(a) Escolar",
    
    # Comercial e Autônomos
    "Vendedor(a) Varejo", "Representante Comercial", "Corretor(a) de Imóveis",
    "Motorista de Aplicativo", "Entregador", "Comerciante", "Cabeleireiro(a)",
    "Mecânico(a)",
    
    # Corporativo
    "Analista de Recursos Humanos", "Assistente Administrativo", "Gerente Financeiro",
    "Contador(a)", "Advogado(a)", "Consultor(a) de Vendas", "Analista de Marketing",
    
    # Tecnologia (Isolado e realístico)
    "Desenvolvedor(a) de Software", "Engenheiro(a) de Dados", "Suporte Técnico",
    
    # Ofícios Base
    "Pedreiro", "Eletricista", "Encanador", "Marceneiro", "Soldador"
]

ROLES = [
    # C-Level e Presidência (Menos frequente no mundo real)
    "CEO", "CFO", "CTO", "Diretor(a) Executivo", "Fundador(a)",
    
    # Gerência e Compras (Gatilho forte B2B)
    "Gerente de Compras", "Comprador(a) Pleno", "Coordenador(a) de Suprimentos",
    "Gerente de Operações", "Líder de Logística",
    
    # Administrativo / B2B Padrão
    "Analista de Contratos", "Assistente Financeiro", "Auxiliar Administrativo",
    "Supervisor(a) Comercial", "Representante de Sucesso do Cliente"
]
