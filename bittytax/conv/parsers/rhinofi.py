# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2021

import sys
from decimal import Decimal

from colorama import Fore, Back

from ..out_record import TransactionOutRecord
from ..dataparser import DataParser
from ..exceptions import UnexpectedTypeError

WALLET = "rhino.fi"


def parse_rhino_trades(data_row, parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['date'])

    symbol = row_dict['symbol']
    seperator_ind = symbol.index(":")
    s1 = symbol[0:seperator_ind]
    s2 = symbol[seperator_ind+1:]
    price = Decimal(row_dict['price'])
    s1_amount = abs(Decimal(row_dict['fillAmount']))
    s2_amount = abs(Decimal(row_dict['fillAmount']))*abs(price)

    if price > 0:
        buy_asset = s1
        buy_amt = s1_amount
        sell_asset = s2
        sell_amt = s2_amount
    else:
        buy_asset = s2
        buy_amt = s2_amount
        sell_asset = s1
        sell_amt = s1_amount

    fee_amt = Decimal(row_dict['feeAmount']) / 1000_000
    fee_asset = row_dict['feeCurrency']

    data_row.t_record = TransactionOutRecord(
        TransactionOutRecord.TYPE_TRADE,
        data_row.timestamp,
        buy_quantity= buy_amt,
        buy_asset = buy_asset,
        sell_quantity= sell_amt,
        sell_asset= sell_asset,
        fee_quantity= fee_amt,
        fee_asset= fee_asset,
        wallet= WALLET,
    )


def parse_rhino_deposits(data_row, parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(row_dict['date'])
    if row_dict['status'] == 'DONE':
        data_row.t_record = TransactionOutRecord(
            TransactionOutRecord.TYPE_DEPOSIT,
            data_row.timestamp,
            buy_quantity= row_dict['amount'],
            buy_asset= row_dict['token'],
            wallet=WALLET
        )


DataParser(DataParser.TYPE_EXCHANGE,
           "rhino.fi",
           ['fillId', 'orderId', 'symbol', 'fillAmount', 'orderAmount', 'price', 'date',
               'orderCreationDate', 'orderType', 'orderActive', 'orderCanceled', 'feeAmount', 'feeCurrency'],
           worksheet_name="rhinofi",
           row_handler=parse_rhino_trades)

DataParser(DataParser.TYPE_EXCHANGE,
           "rhino.fi",
           ['id',	'type',	'token', 'amount', 'chain', 'transactionHash', 'date', 'source', 'status'],
           worksheet_name="rhinofi",
           row_handler=parse_rhino_deposits)
