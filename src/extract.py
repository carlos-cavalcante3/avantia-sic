import os
import logging

class TokenError(Exception):
    """Erro irrecuperável na cadeia de tokens OAuth2. Deve interromper o ETL."""
    pass


class ExtractionError(Exception):
    """Erro recuperável em um recurso específico. O pipeline pode continuar."""
    pass


# ---------------------------------------------------------------------------
# Constantes
# ---------------------------------------------------------------------------

RD_API_BASE_URL = "https://crm.rdstation.com/api/v1"


class Extract:
    """
    Responsável por extrair dados.
    """

    def __init__(self, logger: logging.Logger = None):
        self.logger = logger or self._configurar_logger()
        self.logger.info("Extract inicializado.")

    # -----------------------------------------------------------------------
    # Configuração
    # -----------------------------------------------------------------------

    @staticmethod
    def _configurar_logger() -> logging.Logger:
        logger = logging.getLogger("extract")
        if not logger.handlers:
            handler = logging.StreamHandler()
            handler.setFormatter(logging.Formatter(
                "%(asctime)s [%(levelname)s] %(name)s — %(message)s"
            ))
            logger.addHandler(handler)
        logger.setLevel(logging.INFO)
        return logger