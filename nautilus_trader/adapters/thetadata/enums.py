from enum import Enum


class ThetaDataFrameType(Enum):
    STATUS = "STATUS"
    QUOTE = "QUOTE"
    TRADE = "TRADE"
    OHLC = "OHLC"
    STATE = "STATE"
    REQ_RESPONSE = "REQ_RESPONSE"


class ThetaDataConditionCode(Enum):
    BUYER = 145
    SELLER = 146
