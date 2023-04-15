from datetime import timedelta
import faust
from faust.types import StreamT
from _schema import TaxiRide

app = faust.App(
    "datatalksclub.stream.v2", broker="kafka://localhost:9092", web_port=6069
)
topic = app.topic("datatalkclub.yellow_taxi_ride.json", value_type=TaxiRide)

vendor_rides = app.Table("vendor_rides_windowed", default=int).tumbling(
    timedelta(minutes=1),
    expires=timedelta(hours=1),
)


@app.agent(topic)
async def process(stream: StreamT[TaxiRide]):
    async for event in stream.group_by(TaxiRide.vendor_id):
        assert isinstance(event, TaxiRide)

        vendor_rides[event.vendor_id] += 1


if __name__ == "__main__":
    app.main()
