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


class TestTransformadorDeDados(unittest.TestCase):
    
    def setUp(self):
        self.transformador = TransformadorDeDados()

    def test_limpar_e_capitalizar_nome_do_cliente(self):
        # 1. Arrange
        nome_sujo = "   carlos cavalcante  "
        
        # 2. Act 
        resultado = self.transformador.padronizar_nome_cliente(nome_sujo)
        
        # 3. Assert 
        # Verificamos se ele tirou os espaços e colocou maiúsculas
        self.assertEqual(resultado, "Carlos Cavalcante")

    def test_rejeitar_negocio_sem_id(self):
        # 1. Arrange - dicionário simulando um JSON do RD Station incompleto
        deal_invalido = {"valor": 1500} 
        
        # 2. Act 
        valido = self.transformador.validar_negocio(deal_invalido)
        
        # 3. Assert 
        # expected: Falso, pois não tem ID
        self.assertFalse(valido)

    def test_aprovar_negocio_correto(self):
        # 1. Arrange 
        deal_perfeito = {"id": 12345, "valor": 5000}
        
        # 2. Act 
        valido = self.transformador.validar_negocio(deal_perfeito)
        
        # 3. Assert
        self.assertTrue(valido)


if __name__ == '__main__':
    unittest.main()
