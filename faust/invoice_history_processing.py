import os
from datetime import datetime

import faust
import pandas as pd
from faust.utils import json
from forex_python.converter import CurrencyRates


class Invoices(faust.Record, serializer='json'):
    invoice_id: str
    party_id: str
    currency: str
    taxexclusiveamount: str
    taxinclusiveamount: str
    payableamount: str
    issuedate: str
    taxpointdate: str
    duedate: str


app = faust.App('invoice_processing', broker='kafka://localhost:9092')
invoices_topic = app.topic('invoices', value_type=Invoices)

# invoice_count_by_currency = app.Table('invoice_count_currency', default=int)
# party_id_not_in_static_data = app.Table('party_id_not_in_static_data', default=int)

invoice_amnt_sum = app.Table('invoices_amnt_sum', default=float)
invoice_count = app.Table('invoice_count', default=int)
size_of_business = app.Table('business_size', default=int)
cancelled_invoices = app.Table('cancelled_invoices', default=int)
invoice_cycle_time = app.Table('invoice_cycle_time', default=int)
international_invoices = app.Table('international_invoices', default=int)

static_data = pd.read_csv(os.curdir + '/original_data/static_data.csv')


# def convert_huf_to_eur(amount_in_huf):
#     rate_eur_to_huf = 360  # 1 EUR = 360 HUF
#     return amount_in_huf / rate_eur_to_huf


def convert_to_huf(currency, amount, issue_date):
    c = CurrencyRates()
    converted_amount = c.convert(currency, 'HUF', amount, issue_date)
    # rate_eur_to_huf = 360  # 1 EUR = 360 HUF
    return converted_amount


def get_business_size(df: pd.DataFrame):
    # assert df.size == 1, 'size of df: %d' % df.size
    df = df.dropna()
    if df.size:
        size = df.iloc[0]
        if size == 'Ismeretlen':
            return 0
        if '-' in size:
            size = size.split()[0].split('-')
            return int(size[1].strip())
        else:
            return int(size.split()[0].strip())

    return 0


def classify_business(business_size):
    if business_size < 50:
        return 'S'
    if business_size < 250:
        return 'M'

    return 'L'


def get_global_coverage(international_invoices):
    if international_invoices < 3:
        return 'S'
    if international_invoices < 6:
        return 'M'

    return 'L'


def get_invoice_amount_type(avg_inv_amnt_per_enterprise):
    if avg_inv_amnt_per_enterprise < 10000:
        return 'S'

    if avg_inv_amnt_per_enterprise < 100000:
        return 'M'

    return 'L'


def profile_customers(avg_invoice_amnt, business_size, international_invoices):
    avg_inv_amnt_per_enterprise = avg_invoice_amnt / business_size

    business_type = classify_business(business_size)
    global_coverage = get_global_coverage(international_invoices)
    invoice_amount_type = get_invoice_amount_type(avg_inv_amnt_per_enterprise)
    mappings = {'S': 1, 'M': 2, 'L': 3}

    if business_type == invoice_amount_type == global_coverage:
        return 'Steady Customers'
    if mappings[business_type] < mappings[global_coverage] and mappings[invoice_amount_type] < mappings[global_coverage]:
        return 'Under-performing Customers'

    return 'Developed Customers'
    # profile = mappings[business_type] + ' enterprise with ' + mappings[global_coverage] + ' global coverage having ' + invoice_type
    # print(profile)


# @app.agent(invoices_topic)
# async def process(invoices: faust.Stream[Invoices]) -> None:
#     async for invoice in invoices.group_by(Invoices.currency):
#         invoice_count_by_currency[invoice.currency] += 1
#         print(invoice)
#         print(invoice_count_by_currency.values())
#         print(invoice_count_by_currency.keys())


@app.agent(invoices_topic)
async def process(invoices: faust.Stream[Invoices]) -> None:
    async for invoice in invoices.group_by(Invoices.party_id):
        # print(invoice)
        party_id = invoice.party_id
        if int(party_id) in static_data['PARTY_ID'].values:
            amount = float(invoice.payableamount)
            due_date = datetime.strptime(invoice.duedate, '%Y-%m-%d')
            issue_date = datetime.strptime(invoice.issuedate, '%Y-%m-%d')
            size_of_business[party_id] = get_business_size(
                static_data.loc[static_data['PARTY_ID'] == int(party_id)]['SIZE'])

            if amount < 0:
                cancelled_invoices[party_id] += 1
            else:
                if invoice.currency != 'HUF':
                    international_invoices[party_id] += 1
                    amount = convert_to_huf(invoice.currency, amount, issue_date)

                invoice_amnt_sum[party_id] += amount
                invoice_count[party_id] += 1

                invoice_cycle = due_date - issue_date
                invoice_cycle_time[party_id] += invoice_cycle.days

                avg_invoice_amnt = invoice_amnt_sum[party_id] / invoice_count[party_id]
                # avg_invoice_cycle = invoice_cycle_time[party_id] / invoice_count[party_id]

                if size_of_business[party_id] != 0:
                    print('Average invoice amount per enterprise:', avg_invoice_amnt / size_of_business[party_id])
                    profile = profile_customers(avg_invoice_amnt, size_of_business[party_id], international_invoices[party_id])
                    await app.send('invoice_profile', value=json.dumps(json.loads('{profile:"%s"}' % profile)))

                # print('Average invoice cycle:', avg_invoice_cycle)
                # if avg_invoice_cycle != 0:
                #     print('Average invoice amount per avg invoice cycle:', avg_invoice_amnt / avg_invoice_cycle)

        # print('Invoices: ', invoice_amnt_sum.items())
        # print('Business size:', size_of_business.items())
        # print('Cancelled invoices:', cancelled_invoices.items())
