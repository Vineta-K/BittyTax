# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2021

import sys
import copy

from decimal import Decimal

from colorama import Fore, Back

from ...config import config
from ..out_record import TransactionOutRecord
from ..datamerge import DataMerge, MergeDataRow
from ..exceptions import UnexpectedContentError
from ..parsers.etherscan_like import  etherscan_like_tokens, etherscan_like_nfts,  \
    etherscan_txns, etherscan_int,\
    arbi_txns, arbi_int, \
    bsc_txns, bsc_int, \
    get_note

PRECISION = Decimal('0.' + '0' * 18)

TXNS = 'txn'
TOKENS = 'token'
NFTS = 'nft'
INTERNAL_TXNS = 'int'


def merge_etherscan_like(data_files):
    return do_merge_etherscan(data_files, [])


def do_merge_etherscan(data_files, staking_addresses):
    merge = False
    tx_ids = {}

    # Group all tx data with same txhash in preparation to merge?
    for file_id in data_files:
        for dr in data_files[file_id].data_rows:
            if not dr.t_record:
                continue

            if dr.row_dict['Txhash'] not in tx_ids:
                tx_ids[dr.row_dict['Txhash']] = []

            tx_ids[dr.row_dict['Txhash']]. \
                append(MergeDataRow(dr, data_files[file_id], file_id))

    # Iterate through grouped tx data and merge into one row if possible - need to handle multi buy/sell MEV arbitrage cases at some point
    for txns in tx_ids.values():
        if len(txns) == 1:
            if config.debug:
                sys.stderr.write("%smerge: %s:%s\n" % (
                    Fore.BLUE,
                    txns[0].data_file_id.ljust(5),
                    txns[0].data_row))
            continue

        for t in txns:
            if config.debug:
                sys.stderr.write("%smerge: %s:%s\n" % (
                    Fore.GREEN,
                    t.data_file_id.ljust(5),
                    t.data_row))

        t_ins, t_outs, t_fee = get_ins_outs(txns)

        if config.debug:
            output_records(t_ins, t_outs, t_fee)
            sys.stderr.write("%smerge:     consolidate:\n" % (Fore.YELLOW))

        consolidate(txns, [TXNS, INTERNAL_TXNS])

        t_ins, t_outs, t_fee = get_ins_outs(txns)

        if config.debug:
            output_records(t_ins, t_outs, t_fee)
            sys.stderr.write("%smerge:     merge:\n" % (Fore.YELLOW))

        if t_fee:
            fee_quantity = t_fee.t_record.fee_quantity
            fee_asset = t_fee.t_record.fee_asset

        t_ins_orig = copy.copy(t_ins)
        if t_fee:
            method_handling(t_ins, t_fee, staking_addresses)

        # Make trades
        if len(t_ins) == 1 and t_outs:
            do_etherscan_multi_sell(t_ins, t_outs, t_fee)
        elif len(t_outs) == 1 and t_ins:
            do_etherscan_multi_buy(t_ins, t_outs, t_fee)
        elif len(t_ins) > 1 and len(t_outs) > 1:
            # multi-sell to multi-buy trade not supported
            sys.stderr.write("%sWARNING%s Merge failure for Txhash: %s\n" % (
                Back.YELLOW+Fore.BLACK, Back.RESET+Fore.YELLOW, txns))

            for mdr in txns:
                mdr.data_row.failure = UnexpectedContentError(
                    mdr.data_file.parser.in_header.index('Txhash'),
                    'Txhash', mdr.data_row.row_dict['Txhash'])
                sys.stderr.write("%srow[%s] %s\n" % (
                    Fore.YELLOW,
                    mdr.data_file.parser.in_header_row_num + mdr.data_row.line_num,
                    mdr.data_row))
            continue

        if t_fee:
            # Split fees
            t_all = [t for t in t_ins_orig + t_outs if t.t_record]
            do_fee_split(t_all, t_fee, fee_quantity, fee_asset)

        merge = True

        if config.debug:
            output_records(t_ins_orig, t_outs, t_fee)

    return merge


def get_ins_outs(txns):
    t_ins = [t.data_row for t in txns if t.data_row.t_record and
             t.data_row.t_record.t_type == TransactionOutRecord.TYPE_DEPOSIT]
    t_outs = [t.data_row for t in txns if t.data_row.t_record and
              t.data_row.t_record.t_type == TransactionOutRecord.TYPE_WITHDRAWAL]
    t_fees = [t.data_row for t in txns if t.data_row.t_record and
              t.data_row.t_record.fee_quantity]

    if len(t_fees) == 0:
        t_fee = None
    elif len(t_fees) == 1:
        t_fee = t_fees[0]
    else:
        raise Exception

    return t_ins, t_outs, t_fee


def consolidate(txns, file_ids):
    tx_assets = {}

    for t in list(txns):
        if t.data_file_id not in file_ids:
            return

        asset = t.data_row.t_record.get_asset()
        if asset not in tx_assets:
            tx_assets[asset] = t
            tx_assets[asset].quantity += t.data_row.t_record.get_quantity()
        else:
            tx_assets[asset].quantity += t.data_row.t_record.get_quantity()
            t.data_row.t_record = None
            txns.remove(t)

    for asset in tx_assets:
        t = tx_assets[asset]
        if t.quantity > 0:
            t.data_row.t_record.t_type = TransactionOutRecord.TYPE_DEPOSIT
            t.data_row.t_record.buy_asset = asset
            t.data_row.t_record.buy_quantity = t.quantity
            t.data_row.t_record.sell_asset = ''
            t.data_row.t_record.sell_quantity = None
        elif tx_assets[asset].quantity < 0:
            t.data_row.t_record.t_type = TransactionOutRecord.TYPE_WITHDRAWAL
            t.data_row.t_record.buy_asset = ''
            t.data_row.t_record.buy_quantity = None
            t.data_row.t_record.sell_asset = asset
            t.data_row.t_record.sell_quantity = abs(t.quantity)
        else:
            if t.data_row.t_record.fee_quantity:
                t.data_row.t_record.t_type = TransactionOutRecord.TYPE_SPEND
                t.data_row.t_record.buy_asset = ''
                t.data_row.t_record.buy_quantity = None
                t.data_row.t_record.sell_asset = asset
                t.data_row.t_record.sell_quantity = Decimal(0)
            else:
                txns.remove(t)


def output_records(t_ins, t_outs, t_fee):
    dup = bool(t_fee and t_fee in t_ins + t_outs)

    if t_fee:
        sys.stderr.write("%smerge:   TR-F%s: %s\n" % (
            Fore.YELLOW, '*' if dup else '', t_fee.t_record))

    for t_in in t_ins:
        sys.stderr.write("%smerge:   TR-I%s: %s\n" % (
            Fore.YELLOW, '*' if t_fee is t_in else '', t_in.t_record))
    for t_out in t_outs:
        sys.stderr.write("%smerge:   TR-O%s: %s\n" % (
            Fore.YELLOW, '*' if t_fee is t_out else '', t_out.t_record))


def method_handling(t_ins, t_fee, staking_addresses):
    if t_fee.row_dict.get('Method') in ("Enter Staking", "Leave Staking", "Deposit", "Withdraw"):
        if t_ins:
            staking = [t for t in t_ins if t.row_dict['ContractAddress'] in staking_addresses
                       and t.row_dict['From'] != t_fee.row_dict['To']]
            if staking:
                if len(staking) == 1:
                    staking[0].t_record.t_type = TransactionOutRecord.TYPE_STAKING
                    t_ins.remove(staking[0])

                    if config.debug:
                        sys.stderr.write(
                            "%smerge:     staking:\n" % (Fore.YELLOW))
                else:
                    raise Exception


def do_etherscan_multi_sell(t_ins, t_outs, t_fee):
    if config.debug:
        sys.stderr.write("%smerge:     trade sell(s):\n" % (Fore.YELLOW))

    tot_buy_quantity = 0

    buy_quantity = t_ins[0].t_record.buy_quantity
    buy_asset = t_ins[0].t_record.buy_asset

    if config.debug:
        sys.stderr.write("%smerge:       buy_quantity=%s buy_asset=%s\n" % (
            Fore.YELLOW,
            TransactionOutRecord.format_quantity(buy_quantity), buy_asset))

    for cnt, t_out in enumerate(t_outs):
        if cnt < len(t_outs) - 1:
            split_buy_quantity = (
                buy_quantity / len(t_outs)).quantize(PRECISION)
            tot_buy_quantity += split_buy_quantity
        else:
            # Last t_out, use up remainder
            split_buy_quantity = buy_quantity - tot_buy_quantity

        if config.debug:
            sys.stderr.write("%smerge:       split_buy_quantity=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(split_buy_quantity)))

        t_out.t_record.t_type = TransactionOutRecord.TYPE_TRADE
        t_out.t_record.buy_quantity = split_buy_quantity
        t_out.t_record.buy_asset = buy_asset
        if t_fee:
            t_out.t_record.note = get_note(t_fee.row_dict)

    # Remove TR for buy now it's been added to each sell
    t_ins[0].t_record = None


def do_etherscan_multi_buy(t_ins, t_outs, t_fee):
    if config.debug:
        sys.stderr.write("%smerge:     trade buy(s):\n" % (Fore.YELLOW))

    tot_sell_quantity = 0

    sell_quantity = t_outs[0].t_record.sell_quantity
    sell_asset = t_outs[0].t_record.sell_asset

    if config.debug:
        sys.stderr.write("%smerge:       sell_quantity=%s sell_asset=%s\n" % (
            Fore.YELLOW,
            TransactionOutRecord.format_quantity(sell_quantity), sell_asset))

    for cnt, t_in in enumerate(t_ins):
        if cnt < len(t_ins) - 1:
            split_sell_quantity = (
                sell_quantity / len(t_ins)).quantize(PRECISION)
            tot_sell_quantity += split_sell_quantity
        else:
            # Last t_in, use up remainder
            split_sell_quantity = sell_quantity - tot_sell_quantity

        if config.debug:
            sys.stderr.write("%smerge:       split_sell_quantity=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(split_sell_quantity)))

        t_in.t_record.t_type = TransactionOutRecord.TYPE_TRADE
        t_in.t_record.sell_quantity = split_sell_quantity
        t_in.t_record.sell_asset = sell_asset
        if t_fee:
            t_in.t_record.note = get_note(t_fee.row_dict)

    # Remove TR for sell now it's been added to each buy
    t_outs[0].t_record = None


def do_fee_split(t_all, t_fee, fee_quantity, fee_asset):
    if config.debug:
        sys.stderr.write("%smerge:     split fees:\n" % (Fore.YELLOW))
        sys.stderr.write("%smerge:       fee_quantity=%s fee_asset=%s\n" % (
            Fore.YELLOW,
            TransactionOutRecord.format_quantity(fee_quantity), fee_asset))

    tot_fee_quantity = 0

    for cnt, t in enumerate(t_all):
        if cnt < len(t_all) - 1:
            split_fee_quantity = (
                fee_quantity / len(t_all)).quantize(PRECISION)
            tot_fee_quantity += split_fee_quantity
        else:
            # Last t, use up remainder
            split_fee_quantity = fee_quantity - tot_fee_quantity if fee_quantity else None

        if config.debug:
            sys.stderr.write("%smerge:       split_fee_quantity=%s\n" % (
                Fore.YELLOW,
                TransactionOutRecord.format_quantity(split_fee_quantity)))

        t.t_record.fee_quantity = split_fee_quantity
        t.t_record.fee_asset = fee_asset
        t.t_record.note = get_note(t_fee.row_dict)

    # Remove TR for fee now it's been added to each withdrawal
    if t_fee.t_record and t_fee not in t_all:
        if t_fee.t_record.t_type == TransactionOutRecord.TYPE_SPEND:
            t_fee.t_record = None
        else:
            t_fee.t_record.fee_quantity = None
            t_fee.t_record.fee_asset = ''


DataMerge("Etherscan-like fees & multi-token transactions",
          {TXNS: {'req': DataMerge.MAN, 'obj': etherscan_txns},
           TOKENS: {'req': DataMerge.OPT, 'obj': etherscan_like_tokens},
           NFTS: {'req': DataMerge.OPT, 'obj': etherscan_like_nfts},
           INTERNAL_TXNS: {'req': DataMerge.OPT, 'obj': etherscan_int}},
          merge_etherscan_like)