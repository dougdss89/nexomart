"""
Exemplo básico de testes para FastAPI
"""

import pytest
from fastapi.testclient import TestClient


def test_example():
    """Teste de exemplo básico"""
    assert 1 + 1 == 2


# Descomente quando tiver o app configurado
# @pytest.fixture
# def client():
#     from app.app import app
#     return TestClient(app)
#
# def test_read_root(client):
#     response = client.get("/")
#     assert response.status_code == 200
