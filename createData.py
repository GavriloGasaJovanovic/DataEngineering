from sqlalchemy import create_engine, Column, Integer, String, Float, ForeignKey, JSON, text
from sqlalchemy.orm import declarative_base, relationship, sessionmaker
import json

def read_file(file_path):
    result = []
    with open(file_path, 'r') as file:
        for line in file:
            # Parse JSON from each line
            json_obj = json.loads(line)
            result.append(json_obj)
    return result

DATABASE_URL = "postgresql://postgres:password@localhost/nordeus_db"
engine = create_engine(DATABASE_URL)

Base = declarative_base()

class User(Base):
    __tablename__ = 'users'
    user_id = Column(String(255), primary_key=True)
    country = Column(String(255))  # Add the country column here
    name = Column(String(255))  # Add the name column here
    events = relationship('Event', back_populates='user')

class Event(Base):
    __tablename__ = 'events'
    event_id = Column(Integer, primary_key=True)
    event_timestamp = Column(Integer)
    event_type = Column(String(255))
    user_id = Column(String(255), ForeignKey('users.user_id'), nullable=False)
    user = relationship('User', back_populates='events')
    registration_event = relationship('RegistrationEvent', uselist=False, back_populates='event')
    transaction_event = relationship('TransactionEvent', uselist=False, back_populates='event')
    login_event = relationship('LoginEvent', uselist=False, back_populates='event')
    logout_event = relationship('LogoutEvent', uselist=False, back_populates='event')

class RegistrationEvent(Base):
    __tablename__ = 'registration_events'
    event_id = Column(Integer, ForeignKey('events.event_id'), primary_key=True)
    country = Column(String(255))
    name = Column(String(255))
    device_os = Column(String(255))
    marketing_campaign = Column(String(255))
    event = relationship('Event', back_populates='registration_event')

class TransactionEvent(Base):
    __tablename__ = 'transaction_events'
    event_id = Column(Integer, ForeignKey('events.event_id'), primary_key=True)
    transaction_amount = Column(Float)
    transaction_currency = Column(String(3))
    event = relationship('Event', back_populates='transaction_event')

class LoginEvent(Base):
    __tablename__ = 'login_events'
    event_id = Column(Integer, ForeignKey('events.event_id'), primary_key=True)
    event = relationship('Event', back_populates='login_event')

class LogoutEvent(Base):
    __tablename__ = 'logout_events'
    event_id = Column(Integer, ForeignKey('events.event_id'), primary_key=True)
    event = relationship('Event', back_populates='logout_event')

# Re-create tables and other structures as needed
Base.metadata.create_all(bind=engine)

Session = sessionmaker(bind=engine)
session = Session()

file_path = './data/valid_events.jsonl'
data = read_file(file_path)

for entry in data:
    event_data = entry["event_data"]
    user_id = event_data["user_id"]

    # Check if the user ID already exists in the database
    existing_user = session.query(User).filter_by(user_id=user_id).first()

    if not existing_user:
        # Create the User instance
        user = User(user_id=user_id, country=event_data.get("country"), name=event_data.get("name"))
        session.add(user)

    # Create the Event instance
    event = Event(
        event_id=entry["event_id"],
        event_timestamp=entry["event_timestamp"],
        event_type=entry["event_type"],
        user_id=user_id
    )

    if entry["event_type"] == "registration":
        registration_event = RegistrationEvent(
            event_id=entry["event_id"],
            country=event_data["country"],
            name=event_data["name"],
            device_os=event_data["device_os"],
            marketing_campaign=event_data["marketing_campaign"],
        )
        event.registration_event = registration_event

    elif entry["event_type"] == "transaction":
        transaction_event = TransactionEvent(
            event_id=entry["event_id"],
            transaction_amount=event_data["transaction_amount"],
            transaction_currency=event_data["transaction_currency"],
        )
        event.transaction_event = transaction_event

    elif entry["event_type"] == "login":
        login_event = LoginEvent(event_id=entry["event_id"])
        event.login_event = login_event

    elif entry["event_type"] == "logout":
        logout_event = LogoutEvent(event_id=entry["event_id"])
        event.logout_event = logout_event

    # Add the Event instance to the session
    session.merge(event)

# Commit the changes
session.commit()

