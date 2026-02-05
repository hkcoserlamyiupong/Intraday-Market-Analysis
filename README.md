docker stop timescaledbdocker rm timescaledbdocker run -d --name timescaledb -p 5433:5432 -e POSTGRES_PASSWORD=mysecretpassword -e POSTGRES_USER=postgres -e POSTGRES_DB=market_data timescale/timescaledb:latest-pg16

uvicorn api:app --reload

#open another terminal

python benchmark.py
