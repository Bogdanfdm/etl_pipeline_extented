import os 
from pydantic_settings import BaseSettings, SettingsConfigDict

class CONN_DB_SETTINGS(BaseSettings):
    POSTGRES_USER: str
    POSTGRES_PASSWORD: str
    POSTGRES_HOST: str
    POSTGRES_PORT: int
    POSTGRES_DB: str
    POSTGRES_OUTSIDE_PORT: int
    BASE_DIR: str = os.path.abspath(os.path.join(os.path.dirname(__file__), ".."))

    @property
    def DATABASE_CONN(self)-> str:
        return(
            f"postgresql://{self.POSTGRES_USER}:{self.POSTGRES_PASSWORD}@{self.POSTGRES_HOST}:{self.POSTGRES_OUTSIDE_PORT}/{self.POSTGRES_DB}"
        )
    # new connections need to be added here 
    model_config = SettingsConfigDict(env_file= f"{BASE_DIR}/.env")

settings = CONN_DB_SETTINGS()