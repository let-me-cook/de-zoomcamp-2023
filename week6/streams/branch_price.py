import faust
from _schema import TaxiRide
from faust import current_event
from faust.types import StreamT, EventT

app = faust.App(
    "datatalksclub.stream.v3",
    broker="kafka://localhost:9092",
    consumer_auto_offset_reset="earliest",
    web_port=6068
)
topic = app.topic("datatalkclub.yellow_taxi_ride.json", value_type=TaxiRide)

high_amount_rides = app.topic("datatalksclub.yellow_taxi_rides.high_amount")
low_amount_rides = app.topic("datatalksclub.yellow_taxi_rides.low_amount")


@app.agent(topic)
async def process(stream: StreamT[TaxiRide]):
    async for event in stream:
        try:
            cur_event = current_event()
            assert cur_event is not None

            if event.total_amount >= 40.0:
                await cur_event.forward(high_amount_rides) 
            else:
                await cur_event.forward(low_amount_rides)
        except AssertionError:
            pass


if __name__ == "__main__":
    app.main()
