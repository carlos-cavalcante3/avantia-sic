import requests
import os
import json
from dotenv import load_dotenv

load_dotenv()

BASE_URL = "https://api.rd.services/crm/v2"
TOKEN = os.getenv("RD_ACCESS_TOKEN")


def make_request(endpoint, params=None):
    url = f"{BASE_URL}/{endpoint}"

    headers = {
        "Authorization": f"Bearer {TOKEN}",
        "Accept": "application/json",
    }

    print(f"\nREQUEST: {url}")
    if params:
        print(f"PARAMS: {params}")

    response = requests.get(url, headers=headers, params=params)

    print(f"STATUS: {response.status_code}")

    try:
        data = response.json()
    except:
        print("Impossível converter resposta para JSON")
        print(response.text)
        return None
    
    print("=" * 50)
    print("\nHEADERS:")
    print(dict(response.headers))
    print("=" * 50)

    print("\BODY:")
    print(json.dumps(data, indent=2)[:2000]) 
    print("=" * 50) 

    return data


def test_deals_structure():
    """
    Teste principal: entender estrutura da resposta
    """
    print("\n===== ESTRUTURA =====")
    data = make_request("deals")

    if not data:
        return

    print("\nJSON KEYS:")
    if isinstance(data, dict):
        for key in data.keys():
            print(f" - {key}")
    elif isinstance(data, list):
        print("lista direta como resposta")


def test_pagination_candidates():
    """
    Testa possíveis formas de paginação
    """
    print("\n===== PAGINAÇÃO =====")

    tests = [
        {"limit": 100},
        {"page": 2},
        {"page": 1, "limit": 100},
        {"page[number]": 2, "page[size]": 100},
    ]

    for params in tests:
        print("\n-----------------------------")
        make_request("deals", params=params)


def test_multiple_pages_manual():
    """
    Teste forçando múltiplas páginas manualmente
    """
    print("\n===== TESTE 3: LOOP PAGE =====")

    all_items = []

    for page in range(1, 6):  # testa 5 páginas
        print(f"\nPágina {page}")

        data = make_request("deals", {"page": page})

        if not data:
            break

        if isinstance(data, dict):
            for key in ["deals", "data", "items"]:
                if key in data and isinstance(data[key], list):
                    items = data[key]
                    break
            else:
                print("Não encontrou na lista de itens")
                break
        elif isinstance(data, list):
            items = data
        else:
            break

        print(f"Itens encontrados: {len(items)}")

        if not items:
            break

        all_items.extend(items)

    print(f"\nitens encontrados: {len(all_items)}")


def test_cursor_detection():
    """
    Procura por cursor/next/meta automaticamente
    """
    print("\n===== DETECÇÃO DE PAGINAÇÃO =====")

    data = make_request("deals")

    if not isinstance(data, dict):
        print("Resposta não é dict, pode ser lista direta")
        return

    for key, value in data.items():
        if isinstance(value, dict):
            print(f"\nPOSSÍVEL META: {key}")
            print(json.dumps(value, indent=2))

    print("\nBusca por palavras-chave:")
    keywords = ["next", "cursor", "page", "total", "links", "meta"]

    def search(obj, path=""):
        if isinstance(obj, dict):
            for k, v in obj.items():
                if any(word in k.lower() for word in keywords):
                    print(f"{path}.{k} -> {v}")
                search(v, f"{path}.{k}")
        elif isinstance(obj, list):
            for i, item in enumerate(obj[:2]):  # limita
                search(item, f"{path}[{i}]")

    search(data)


if __name__ == "__main__":
    test_deals_structure()
    test_pagination_candidates()
    test_multiple_pages_manual()
    test_cursor_detection()