from sqlalchemy import Column, Integer, String, DateTime
from base import Base
import datetime


class CheckIn(Base):
    """Check In"""

    __tablename__ = "check_in"

    id = Column(Integer, primary_key=True)
    reservationCode = Column(String(250), nullable=False)
    name = Column(String(250), nullable=False)
    initialDeposit = Column(Integer, nullable=False)
    numPeople = Column(Integer, nullable=False)
    date_created = Column(DateTime, nullable=False)
    trace_id = Column(String(250))

    def __init__(self, reservationCode, name, initialDeposit, numPeople, trace_id):
        self.reservationCode = reservationCode
        self.name = name
        self.initialDeposit = initialDeposit
        self.numPeople = numPeople
        self.date_created = datetime.datetime.now()
        self.trace_id = trace_id

    def to_dict(self):
        dict = {}
        dict["id"] = self.id
        dict["reservationCode"] = self.reservationCode
        dict["name"] = self.name
        dict["initialDeposit"] = self.initialDeposit
        dict["numPeople"] = self.numPeople
        dict["date_created"] = self.date_created
        dict["trace_id"] = self.trace_id
        return dict
