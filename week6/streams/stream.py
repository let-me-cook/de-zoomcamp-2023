import faust
from _schema import TaxiRide


app = faust.App(
    "datatalksclub.stream.v2", broker="kafka://localhost:9092", web_port=6066
)
topic = app.topic("datatalkclub.yellow_taxi_ride.json", value_type=TaxiRide)


@app.agent(topic)
async def start_reading(records):
    async for record in records:
        print(record)


if __name__ == "__main__":
    app.main()
