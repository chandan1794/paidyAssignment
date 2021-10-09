from dataclasses import dataclass


@dataclass
class DatabaseConfig:
    USERNAME: str
    PASSWORD: str
    HOST: str
    PORT: int
    DATABASE_NAME: str


@dataclass
class S3Config:
    BUCKET: str
    AWS_ACCESS_KEY: str
    AWS_SECRET_KEY: str


@dataclass
class ETLConfig:
    JOB_SIZE_IN_BYTES: int
