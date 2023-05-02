from pydantic import BaseSettings
from dotenv import find_dotenv


class MySettings(BaseSettings):
    class Config:
        env_file = find_dotenv()
        case_sensitive = True

    DATABASE_HOST: str
    DATABASE_PORT: str
    DATABASE_NAME: str
    DATABASE_USER: str
    DATABASE_PASSWORD: str


conf = MySettings()
