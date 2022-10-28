from sqlalchemy import Column, Integer, String, DateTime
from base import Base


class Stats(Base):
    """ Processing Statistics """
    __tablename__ = "stats"
    id = Column(Integer, primary_key=True)
    num_ci_readings = Column(Integer, nullable=False)
    num_bc_readings = Column(Integer, nullable=False)
    max_numPeople = Column(Integer, nullable=True)
    max_numNights = Column(Integer, nullable=True)
    last_updated = Column(DateTime, nullable=False)

    def __init__(self, num_ci_readings, num_bc_readings,
                 max_numPeople, max_numNights,
                 last_updated):
        """ Initializes a processing statistics objet """
        self.num_ci_readings = num_ci_readings
        self.num_bc_readings = num_bc_readings
        self.max_numPeople = max_numPeople
        self.max_numNights = max_numNights
        self.last_updated = last_updated

    def to_dict(self):
        """ Dictionary Representation of a statistics """
        dict = {}
        dict['num_ci_readings'] = self.num_ci_readings
        dict['num_bc_readings'] = self.num_bc_readings
        dict['max_numPeople'] = self.max_numPeople
        dict['max_numNights'] = self.max_numNights
        dict['last_updated'] = self.last_updated.strftime("%Y-%m-%dT%H:%M:%S")

        return dict
