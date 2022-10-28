from typing import Counter
from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime
import json


class BookingConfirm(Base):
    """Check In"""

    __tablename__ = "booking_confirm"

    id = Column(Integer, primary_key=True)
    confirmationCode = Column(String(250), nullable=False)
    name = Column(String(250), nullable=False)
    roomNum = Column(Integer, nullable=False)
    nights = Column(Integer, nullable=False)
    arriveDate = Column(String(250))
    date_created = Column(DateTime, nullable=False)
    trace_id = Column(String(250))

    def __init__(self, confirmationCode, name, roomNum, nights, arriveDate, trace_id):
        self.confirmationCode = confirmationCode
        self.name = name
        self.roomNum = roomNum
        self.nights = nights
        self.arriveDate = arriveDate
        self.date_created = datetime.datetime.now()
        self.trace_id = trace_id

    def to_dict(self):
        dict = {}
        dict["id"] = self.id
        dict["confirmationCode"] = self.confirmationCode
        dict["name"] = self.name
        dict["roomNum"] = self.roomNum
        dict["nights"] = self.nights
        dict["arriveDate"] = self.arriveDate
        dict["date_created"] = self.date_created
        dict["trace_id"] = self.trace_id
        return dict
