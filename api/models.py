"""Pydantic request/response models for the API."""

from pydantic import BaseModel
from typing import Optional


class LoginRequest(BaseModel):
    username: str
    password: str
    mode: str = "paper"          # "paper" or "live"


class ConnectRequest(BaseModel):
    host: str = "127.0.0.1"
    port: int = 4002


class EnterRequest(BaseModel):
    max_new: Optional[int] = None
    tickers: Optional[list[str]] = None


class SizingRequest(BaseModel):
    account_value: Optional[float] = None


class AutoManageRequest(BaseModel):
    max_new: Optional[int] = 20
    account_value: Optional[float] = 1_023_443


class AddPositionRequest(BaseModel):
    ticker: str
    combo: str
    strike: float
    put_strike: Optional[float] = None
    front_exp: str
    back_exp: str
    contracts: int = 1
    cost_per_share: float
    ff: float
    spread_type: str = "double"
    n_legs: int = 4
    send_to_ibkr: Optional[bool] = None  # None=auto, True=force IBKR, False=track only


class ClosePositionRequest(BaseModel):
    position_id: str
    exit_price: Optional[float] = None
    use_ibkr: Optional[bool] = None  # None=auto-detect, True=force IBKR, False=paper


class DaemonConfig(BaseModel):
    auto_start: Optional[bool] = True  # start daemon on app launch
