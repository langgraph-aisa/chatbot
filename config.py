"""
config.py - Configuración central con mapeo explícito de variables de entorno.
VERSIÓN 2.0.20 – Añadidas variables para conexión directa a BD de Odoo.
"""
import os
from dotenv import load_dotenv
from pydantic_settings import BaseSettings
from pydantic import Field

load_dotenv()

class Settings(BaseSettings):
    # API Keys
    chatbot_master_api_key: str = Field(default="sk_dev_fallback", env="CHATBOT_MASTER_API_KEY")
    
    # OpenAI
    openai_api_key_1: str = Field(default="", env="OPENAI_API_KEY_1")
    openai_api_key_2: str = Field(default="", env="OPENAI_API_KEY_2")
    openai_api_key_3: str = Field(default="", env="OPENAI_API_KEY_3")
    
    @property
    def openai_api_key(self) -> str:
        for key in [self.openai_api_key_1, self.openai_api_key_2, self.openai_api_key_3]:
            if key:
                return key
        return os.getenv("OPENAI_API_KEY", "")

    # Langfuse
    langfuse_public_key: str = Field(default="", env="LANGFUSE_PUBLIC_KEY")
    langfuse_secret_key: str = Field(default="", env="LANGFUSE_SECRET_KEY")
    langfuse_host: str = Field(default="https://cloud.langfuse.com", env="LANGFUSE_HOST")
    langfuse_tracing_environment: str = Field(default="production", env="LANGFUSE_TRACING_ENVIRONMENT")
    langfuse_project_id: str = Field(default="", env="LANGFUSE_PROJECT_ID")
    release_version: str = Field(default="jarvi-2.0.20", env="RELEASE_VERSION")

    # Bases de datos
    ctfom_database_url: str = Field(default="", env="CTFOM_DATABASE_URL")
    bi_database_url: str = Field(default="", env="BI_DATABASE_URL")
    database_url: str = Field(default="", env="DATABASE_URL")

    # Redis
    redis_url: str = Field(default="", env="REDIS_URL")
    redis_ttl: int = Field(default=604800, env="REDIS_TTL")

    # NUEVAS: Variables para conexión directa a BD de Odoo (PostgreSQL)
    odoo_db_host: str = Field(default="", env="DATABASE_HOST")          # servidor
    odoo_db_port: int = Field(default=5432, env="DATABASE_PORT")        # puerto
    odoo_db_name: str = Field(default="", env="DATABASE_PROD")          # nombre BD
    odoo_db_user: str = Field(default="", env="DATABASE_USER")          # usuario
    odoo_db_password: str = Field(default="", env="DATABASE_PASSWORD")  # contraseña

    # Webhook
    n8n_webhook_url: str = Field(default="", env="N8N_WEBHOOK_URL")

    # URLs públicas
    backend_url: str = Field(default="https://jarvi-backend-production.up.railway.app", env="BACKEND_URL")

    class Config:
        env_file = ".env"
        env_file_encoding = "utf-8"
        extra = "ignore"

settings = Settings()

# =============================================================================
# CONSTANTES PARA MICDP
# =============================================================================
MICDP_DIMENSIONS = ["IDENTIDAD", "PROBLEMA", "TECNICA", "COMPORTAMIENTO", "VIABILIDAD", "SINTESIS"]
MICDP_WEIGHTS = {"IDENTIDAD": 0.05, "PROBLEMA": 0.10, "TECNICA": 0.25, "COMPORTAMIENTO": 0.25, "VIABILIDAD": 0.15, "SINTESIS": 0.20}
MICDP_THRESHOLDS = {"IDENTIDAD": 0.85, "PROBLEMA": 0.80, "TECNICA": 0.90, "COMPORTAMIENTO": 0.85, "VIABILIDAD": 0.80, "SINTESIS": 0.95}
