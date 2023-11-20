from fastapi import FastAPI, Depends
from sqlalchemy import cast
from sqlalchemy import func
from sqlalchemy.orm import Session
from datetime import datetime, timedelta

from createData import User, Event, TransactionEvent, Integer
from database import SessionLocal

app = FastAPI()

# Dependency to get the database session
def get_db():
    db = SessionLocal()
    try:
        yield db
    finally:
        db.close()


# Get user level stats
@app.get("/user-stats/{user_id}")
def get_user_stats(user_id: str, date: str = None, db: Session = Depends(get_db)):
    # Filter events for the specified user
    user_events = (
        db.query(Event)
            .join(User)
            .filter(User.user_id == user_id)
    )

    # If date is specified, filter events for that date
    if date:
        date = datetime.strptime(date, "%Y-%m-%d")
        user_events = user_events.filter(func.date(Event.event_timestamp) == date)

    # Get the latest login event for the user
    latest_login_event = (
        user_events.filter(Event.event_type == "login")
            .order_by(Event.event_timestamp.desc())
            .first()
    )

    # Calculate days since last login
    days_since_last_login = None
    if latest_login_event:
        days_since_last_login = (datetime.utcnow() - datetime.utcfromtimestamp(latest_login_event.event_timestamp)).days

    # Calculate the number of logins
    num_logins = user_events.filter(Event.event_type == "login").count()

    # Calculate the number of sessions
    num_sessions = user_events.filter(Event.event_type == "login").count()

    # Calculate the total time spent in game
    total_time_spent = (
        user_events.filter(Event.event_type == "logout")
            .filter(Event.event_timestamp > latest_login_event.event_timestamp)
            .with_entities(func.sum(Event.event_timestamp - latest_login_event.event_timestamp))
            .scalar()
    )

    total_time_spent = timedelta(seconds=total_time_spent) if total_time_spent else timedelta()

    # Fetch user details
    user_details = db.query(User).filter(User.user_id == user_id).first()

    return {
        "user_id": user_id,
        "country": user_details.country,
        "name": user_details.name,
        "num_logins": num_logins,
        "days_since_last_login": days_since_last_login,
        "num_sessions": num_sessions,
        "total_time_spent": total_time_spent.total_seconds() if total_time_spent else 0,
    }


@app.get("/game-stats")
def get_game_stats(date: str = None, country: str = None, db: Session = Depends(get_db)):
    # Filter events for the specified date
    if date:
        date = datetime.strptime(date, "%Y-%m-%d")

    game_events = db.query(Event)

    if date:
        game_events = game_events.filter(func.date(Event.event_timestamp) == date)

    # Filter events for the specified country
    if country:
        game_events = game_events.join(User).filter(User.country == country)

    # Calculate number of daily active users
    num_daily_active_users = (
        game_events.filter(Event.event_type == "login")
            .distinct(Event.user_id)
            .count()
    )

    # Calculate number of logins
    num_logins = game_events.filter(Event.event_type == "login").count()

    # Calculate total revenue
    total_revenue = (
        db.query(func.sum(TransactionEvent.transaction_amount))
            .filter(Event.event_id == TransactionEvent.event_id)
            .filter(User.country == country)
            .scalar()
    )

    # Calculate number of paid users
    num_paid_users = (
        game_events.join(TransactionEvent)
            .filter(TransactionEvent.transaction_amount > 0)
            .distinct(Event.user_id)
            .count()
    )

    # Calculate average number of sessions per user
    # Calculate first all the sessions
    avg_sessions = (
        game_events.filter(Event.event_type == "login")
            .group_by(Event.user_id)
            .with_entities(func.count().label("num_sessions"))
            .subquery()
    )

    # Avg all the sessions
    avg_sessions_query = (
        db.query(func.avg(avg_sessions.c.num_sessions))
        .scalar()
    )

    # Calculate average total time spent in game
    # Save all the time differences in game
    cte = (
        db.query(
            Event.user_id,
            User.country,
            func.coalesce(
                Event.event_timestamp - func.lag(Event.event_timestamp)
                .over(partition_by=Event.user_id, order_by=Event.event_timestamp),
                0
            ).label("time_diff")
        )
            .join(User, User.user_id == Event.user_id)
            .filter(Event.event_type == "login")
            .filter(User.country == country)
            .cte("login_time_diff_cte")
    )

    # Average all the time differences in the game
    avg_time_spent = (
        db.query(func.avg(cast(cte.c.time_diff, Integer)))
            .scalar()
    )
    return {
        "num_daily_active_users": num_daily_active_users,
        "num_logins": num_logins,
        "total_revenue": total_revenue or 0,
        "num_paid_users": num_paid_users,
        "avg_sessions": avg_sessions_query or 0,
        "avg_time_spent": avg_time_spent or 0,
    }
