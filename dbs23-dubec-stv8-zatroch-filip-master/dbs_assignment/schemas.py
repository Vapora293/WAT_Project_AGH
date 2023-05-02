from pydantic import BaseModel

class Flight(BaseModel):
    flight_id: str | None = None
    flight_no: str | None = None
    scheduled_departure: str
    scheduled_arrival: str
    departure_airport: str
    arrival_airport: str
    status: str
    aircraft_code: str
    actual_departure: str | None = None
    actual_arrival: str | None = None
