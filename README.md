# App requirements

Install following:

* `uvicorn`
* `sqlalchemy`
* `fastapi`

Database that was used: `Postgres` with `PgAdmin`

Application is run with following command:
`uvicorn server:app --host 0.0.0.0 --port 8000 --reload`

In `data/invalid_events` you will have the reason for discarding each one of them!