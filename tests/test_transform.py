import unittest

class TransformadorDeDados:
    """Classe responsável apenas por limpar e validar dados."""
    
    def padronizar_nome_cliente(self, nome: str) -> str:
        """Remove espaços em branco sobrando e deixa a primeira letra maiúscula."""
        if not nome:
            return ""
        return nome.strip().title()

    def validar_negocio(self, deal: dict) -> bool:
        """Um negócio do RD Station só é válido se tiver 'id' e valor >= 0."""
        if "id" not in deal or "valor" not in deal:
            return False
        if deal["valor"] < 0:
            return False
        return True

