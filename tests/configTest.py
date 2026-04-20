import pytest
from dotenv import load_dotenv

@pytest.fixture(scope="session", autouse=True)
def carregar_variaveis_ambiente():
    """Garante que o .env seja lido antes de iniciar a suite de testes."""
    load_dotenv(override=True)