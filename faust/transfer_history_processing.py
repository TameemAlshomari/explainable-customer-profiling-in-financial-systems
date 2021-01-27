import faust
import json
from datetime import datetime
import pandas as pd

app = faust.App(
    'customer_profiling_app',
    broker='kafka://localhost:9092',
    consumer_auto_offset_reset = 'latest'
    # value_serializer='raw',
)

class Transfer(faust.Record, serializer='json'):
    id: int
    TRANSFER_ID: str
    PARTY_ID: str
    CURRENCY: str
    AMOUNT: str
    VALUE_DATE: str


kafka_topic = app.topic('transfers', value_type=Transfer)
transfers_count = app.Table('customer-profiling', default=int)
transfers_amount_sum = app.Table('sum-table', default=float)
amount_rate = app.Table('rate-table', default=float)
life = app.Table('life-table', default=float)
rate_class = app.Table('rateclass-table', default=float)


static_data = pd.read_csv('data/static_data.csv')

def get_creation_date(df: pd.DataFrame):
    df = df.dropna()
    date = "2021-01-01 00:00:00.000"
    if len(df["CREATED_ON"].values) > 0:
        date = df["CREATED_ON"].values[0]
        # print (date)

    return date

def lifeClassifier(create):
    if (datetime.strptime("1970-01-01 00:00:00.000", "%Y-%m-%d %H:%M:%S.%f") <= create < datetime.strptime("1986-06-01 00:00:00.000", "%Y-%m-%d %H:%M:%S.%f")):
        return 1
    if (datetime.strptime("1986-06-01 00:00:00.000", "%Y-%m-%d %H:%M:%S.%f") <= create < datetime.strptime("2002-01-01 00:00:00.000", "%Y-%m-%d %H:%M:%S.%f")):
        return 2
    if create >= datetime.strptime("2002-01-01 00:00:00.000", "%Y-%m-%d %H:%M:%S.%f"):
       return 3

def rateClassifier(rate):
    if 0 <= rate < 529366:
        return 10
    if 529366 <= rate < 1029500:   
        return 20
    if rate >= 1029500:
        return 30

def lifeCalculator(create):
    filtered_date = create
    print(filtered_date)
    creation =  datetime.strptime(filtered_date, '%Y-%m-%d %H:%M:%S.%f')
    current =  datetime.strptime("2021-01-01 00:00:00.000", '%Y-%m-%d %H:%M:%S.%f')
    life = current -creation
    # print(life.total_seconds())
    return life #.total_seconds

def groupClassifier(group):
    active = [32, 23, 33]
    average = [13, 22,31]
    idle = [11, 12, 21]
    if group in active:
        return "Active"
    if group in average:
        return "Average"
    if group in idle:
        return "IDLE"

def get_transfer_party_id(transfer):
    print (json.loads(transfer)['PARTY_ID'])


#Sum transfers per Customer in the stream
@app.agent(kafka_topic)
async def rate_transfers(transfers: faust.Stream[Transfer]) -> None:
    async for transfer in transfers.group_by(Transfer.PARTY_ID):
        transfers_count[transfer.PARTY_ID] += 1
        transfers_amount_sum[transfer.PARTY_ID] += float(transfer.AMOUNT)
        amount_rate[transfer.PARTY_ID] = (transfers_amount_sum[transfer.PARTY_ID]/ transfers_count[transfer.PARTY_ID])
        # print("USER "+str(transfer.PARTY_ID)+"    has "+str(amount_rate[transfer.PARTY_ID]))
        # print("########################")
        x = static_data.loc[static_data['PARTY_ID'] == int(transfer.PARTY_ID)]
        # print(x)
        create = get_creation_date(x)
        create = datetime.strptime(create, '%Y-%m-%d %H:%M:%S.%f')
        life[transfer.PARTY_ID] = lifeClassifier(create)
        rate_class[transfer.PARTY_ID]= rateClassifier(amount_rate[transfer.PARTY_ID])
        group = life[transfer.PARTY_ID] + rate_class[transfer.PARTY_ID]
        # print(group)
        # print(groupClassifier(group))
        if transfers_count[transfer.PARTY_ID] != 0:
            print('Transfer Amount Rate per Customer:', amount_rate[transfer.PARTY_ID])
            profile = groupClassifier(group)
            output_message = {
                "party_id": transfer.PARTY_ID,
                "profile": profile
            }
            await app.send('transfer-profile', value=json.dumps(output_message))

# @app.agent(kafka_topic)
# async def process(transfers):
#     async for value in transfers:
#         # result = requests.post('http://127.0.0.1:5000/invocations', json=json.loads(value))
#         print('Input data: ' + str(value))
#         # print('Fraud detection result: ' + str(result.json()))  