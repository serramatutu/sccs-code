from dataclasses import dataclass
from enum import Enum


class TransactionType(str, Enum):
    GET = "GET"
    OVERWRITE = "OVERWRITE"
    INCREASE = "INCREASE"


@dataclass(frozen=True)
class Transaction:
    id: int
    type: TransactionType
    submit_time: float
    execution_time: float
    key: int


