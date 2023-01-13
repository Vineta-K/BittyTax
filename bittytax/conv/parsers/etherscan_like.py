# -*- coding: utf-8 -*-
# (c) Nano Nano Ltd 2019

from decimal import Decimal

from ..out_record import TransactionOutRecord
from ..dataparser import DataParser
from ..exceptions import DataFilenameError
from .banned_tokens import banned


def parse_etherscan_like(data_row, parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(
        int(row_dict['UnixTimestamp']))
    chain_asset = parser.chain_asset

    if row_dict.get('Status'):
        if row_dict['Status'] != '':
            # Failed txns should not have a Value_OUT
            row_dict[f'Value_OUT({chain_asset})'] = 0

    if Decimal(row_dict[f'Value_IN({chain_asset})']) > 0:
        if row_dict.get('Status') == '':
            data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                     data_row.timestamp,
                                                     buy_quantity=row_dict[f'Value_IN({chain_asset})'],
                                                     buy_asset=chain_asset,
                                                     wallet=get_wallet(
                                                         row_dict['To']),
                                                     note=get_note(row_dict))
    elif Decimal(row_dict[f'Value_OUT({chain_asset})']) > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict[f'Value_OUT({chain_asset})'],
                                                 sell_asset=chain_asset,
                                                 fee_quantity=row_dict[f'TxnFee({chain_asset})'],
                                                 fee_asset=chain_asset,
                                                 wallet=get_wallet(
                                                     row_dict['From']),
                                                 note=get_note(row_dict))
    else:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_SPEND,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict[f'Value_OUT({chain_asset})'],
                                                 sell_asset=chain_asset,
                                                 fee_quantity=row_dict[f'TxnFee({chain_asset})'],
                                                 fee_asset=chain_asset,
                                                 wallet=get_wallet(
                                                     row_dict['From']),
                                                 note=get_note(row_dict))


def get_wallet(address):
    return str(address.lower()[0:TransactionOutRecord.WALLET_ADDR_LEN])


def get_note(row_dict):
    if row_dict.get('Status'):
        if row_dict['Status'] != '':
            if row_dict.get('Method'):
                return "Failure (%s)" % row_dict['Method']
            return "Failure"

    if row_dict.get('Method'):
        return row_dict['Method']

    return row_dict.get('PrivateNote', '')


def parse_etherscan_like_internal(data_row, parser, **kwargs):
    row_dict = data_row.row_dict
    data_row.timestamp = DataParser.parse_timestamp(
        int(row_dict['UnixTimestamp']))
    chain_asset = parser.chain_asset

    # Failed internal txn
    if row_dict.get('Status'):  # For stupid blockchains with no status
        if row_dict['Status'] != '0':
            return

    if Decimal(row_dict[f'Value_IN({chain_asset})']) > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict[f'Value_IN({chain_asset})'],
                                                 buy_asset=chain_asset,
                                                 wallet=get_wallet(row_dict['TxTo']))
    elif Decimal(row_dict[f'Value_OUT({chain_asset})']) > 0:
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict[f'Value_OUT({chain_asset})'],
                                                 sell_asset=chain_asset,
                                                 wallet=get_wallet(row_dict['From']))


def parse_etherscan_like_tokens(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict

    if row_dict['TokenSymbol'] in banned:  # do not process shitty airdrops
        return

    data_row.timestamp = DataParser.parse_timestamp(
        int(row_dict['UnixTimestamp']))

    if row_dict['TokenSymbol'].endswith('-LP'):
        asset = row_dict['TokenSymbol'] + '-' + \
            row_dict['ContractAddress'][0:10]
    else:
        asset = row_dict['TokenSymbol']

    if row_dict['To'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=row_dict['TokenValue'].replace(
                                                     ',', ''),
                                                 buy_asset=asset,
                                                 wallet=get_wallet(row_dict['To']))
    elif row_dict['From'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=row_dict['TokenValue'].replace(
                                                     ',', ''),
                                                 sell_asset=asset,
                                                 wallet=get_wallet(row_dict['From']))
    else:
        raise DataFilenameError(kwargs['filename'], "Ethereum address")


def parse_etherscan_like_nfts(data_row, _parser, **kwargs):
    row_dict = data_row.row_dict
    if '{} #{}'.format(row_dict['TokenName'], row_dict['TokenId']) in banned:
        return

    data_row.timestamp = DataParser.parse_timestamp(
        int(row_dict['UnixTimestamp']))

    if row_dict['To'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_DEPOSIT,
                                                 data_row.timestamp,
                                                 buy_quantity=1,
                                                 buy_asset='{} #{}'.format(row_dict['TokenName'],
                                                                           row_dict['TokenId']),
                                                 wallet=get_wallet(row_dict['To']))
    elif row_dict['From'].lower() in kwargs['filename'].lower():
        data_row.t_record = TransactionOutRecord(TransactionOutRecord.TYPE_WITHDRAWAL,
                                                 data_row.timestamp,
                                                 sell_quantity=1,
                                                 sell_asset='{} #{}'.format(row_dict['TokenName'],
                                                                            row_dict['TokenId']),
                                                 wallet=get_wallet(row_dict['From']))
    else:
        raise DataFilenameError(kwargs['filename'], "Ethereum address")


etherscan_txns = DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan (ETH Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
     'Value_IN(ETH)', 'Value_OUT(ETH)', None, 'TxnFee(ETH)', 'TxnFee(USD)',
     'Historical $Price/Eth', 'Status', 'ErrCode', 'Method'],
    worksheet_name="Etherscan",
    row_handler=parse_etherscan_like,
    chain_asset='ETH')

bsc_txns = DataParser(DataParser.TYPE_EXPLORER,
                      "BscScan (BSC Transactions)",
                      ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
                       'Value_IN(BNB)', 'Value_OUT(BNB)', None, 'TxnFee(BNB)', 'TxnFee(USD)',
                       'Historical $Price/BNB', 'Status', 'ErrCode', 'Method'],
                      worksheet_name="BscScan",
                      row_handler=parse_etherscan_like,
                      chain_asset='BNB')

arbi_txns = DataParser(DataParser.TYPE_EXPLORER,
                       "ArbiScan (Arbitrum Transactions)",
                       ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
                        'Value_IN(ETH)', 'Value_OUT(ETH)', None, 'TxnFee(ETH)', 'TxnFee(USD)',
                        'Historical $Price/ETH', 'Status', 'ErrCode', 'Method'],
                       worksheet_name="ArbiScan",
                       row_handler=parse_etherscan_like,
                       chain_asset='ETH')

avax_txns = DataParser(DataParser.TYPE_EXPLORER,
                       "SnowTrace (Avax Transactions)",
                       ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
                        'Value_IN(AVAX)', 'Value_OUT(AVAX)', None, 'TxnFee(AVAX)', 'TxnFee(USD)',
                        'Historical $Price/AVAX', 'Status', 'ErrCode', 'Method'],
                       worksheet_name="SnowTrace",
                       row_handler=parse_etherscan_like,
                       chain_asset='AVAX')

cronos_txns = DataParser(DataParser.TYPE_EXPLORER,
                         "CronoScan (Cronos Transactions)",
                         ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
                          'Value_IN(CRO)', 'Value_OUT(CRO)', None, 'TxnFee(CRO)', 'TxnFee(USD)',
                          'Historical $Price/CRO', 'Status', 'ErrCode', 'Method'],
                         worksheet_name="CronoScan",
                         row_handler=parse_etherscan_like,
                         chain_asset='CRO')

ftm_txns = DataParser(DataParser.TYPE_EXPLORER,
                      "FTMScan (FTM Transactions)",
                      ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
                       'Value_IN(FTM)', 'Value_OUT(FTM)', None, 'TxnFee(FTM)', 'TxnFee(USD)',
                       'Historical $Price/FTM', 'Status', 'ErrCode', 'Method'],
                      worksheet_name="FTMScan",
                      row_handler=parse_etherscan_like,
                      chain_asset='FTM')

gnosis_txns = DataParser(DataParser.TYPE_EXPLORER,
                         "GnosisScan (Gnosis Transactions)",
                         ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
                          'Value_IN(xDAI)', 'Value_OUT(xDAI)', None, 'TxnFee(xDAI)', 'TxnFee(USD)',
                          'Historical $Price/xDAI', 'Status', 'ErrCode', 'Method'],
                         worksheet_name="GnosisScan",
                         row_handler=parse_etherscan_like,
                         chain_asset='xDAI')

harmony_txns = DataParser(DataParser.TYPE_EXPLORER,
                          "Harmony (Harmony Transactions)",
                          ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To',
                           'Value_IN(ONE)', 'Value_OUT(ONE)', None, 'TxnFee(ONE)', 'TxnFee(USD)',
                           'Method'],
                          worksheet_name="Harmony",
                          row_handler=parse_etherscan_like,
                          chain_asset='ONE')

moonriver_txns = DataParser(DataParser.TYPE_EXPLORER,
                            "MoonScan (Moonriver Transactions)",
                            ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
                             'Value_IN(MOVR)', 'Value_OUT(MOVR)', None, 'TxnFee(MOVR)', 'TxnFee(USD)',
                             'Historical $Price/MOVR', 'Status', 'ErrCode', 'Method'],
                            worksheet_name="MoonScan",
                            row_handler=parse_etherscan_like,
                            chain_asset='MOVR')

polygon_txns = DataParser(DataParser.TYPE_EXPLORER,
                          "PolygonScan (Polygon Transactions)",
                          ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress',
                           'Value_IN(MATIC)', 'Value_OUT(MATIC)', None, 'TxnFee(MATIC)', 'TxnFee(USD)',
                           'Historical $Price/MATIC', 'Status', 'ErrCode', 'Method'],
                          worksheet_name="PolygonScan",
                          row_handler=parse_etherscan_like,
                          chain_asset='MATIC')

etherscan_int = DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan (ETH Internal Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
     'ParentTxETH_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(ETH)',
     'Value_OUT(ETH)', None, 'Historical $Price/Eth', 'Status', 'ErrCode', 'Type'],
    worksheet_name="Etherscan",
    row_handler=parse_etherscan_like_internal,
    chain_asset='ETH')

bsc_int = DataParser(
    DataParser.TYPE_EXPLORER,
    "BscScan (BSC Internal Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
        'ParentTxBNB_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(BNB)',
        'Value_OUT(BNB)', None, 'Historical $Price/BNB', 'Status', 'ErrCode', 'Type'],
    worksheet_name="BscScan",
    row_handler=parse_etherscan_like_internal,
    chain_asset='BNB')

arbi_int = DataParser(
    DataParser.TYPE_EXPLORER,
    "ArbiScan (Arbitrum Internal Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
        'ParentTxETH_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(ETH)',
        'Value_OUT(ETH)', None, 'Historical $Price/ETH', 'Status', 'ErrCode', 'Type'],
    worksheet_name="ArbiScan",
    row_handler=parse_etherscan_like_internal,
    chain_asset='ETH')

avax_int = DataParser(
    DataParser.TYPE_EXPLORER,
    "SnowTrace (Avax Internal Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
        'ParentTxAVAX_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(AVAX)',
        'Value_OUT(AVAX)', None, 'Historical $Price/AVAX', 'Status', 'ErrCode', 'Type'],
    worksheet_name="SnowTrace",
    row_handler=parse_etherscan_like_internal,
    chain_asset='AVAX')

cronos_int = DataParser(
    DataParser.TYPE_EXPLORER,
    "CronoScan (Cronos Internal Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
        'ParentTxCRO_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(CRO)',
        'Value_OUT(CRO)', None, 'Historical $Price/CRO', 'Status', 'ErrCode', 'Type'],
    worksheet_name="CronoScan",
    row_handler=parse_etherscan_like_internal,
    chain_asset='CRO')

ftm_int = DataParser(
    DataParser.TYPE_EXPLORER,
    "FTMScan (FTM Internal Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
        'ParentTxFTM_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(FTM)',
        'Value_OUT(FTM)', None, 'Historical $Price/FTM', 'Status', 'ErrCode', 'Type'],
    worksheet_name="FTMScan",
    row_handler=parse_etherscan_like_internal,
    chain_asset='FTM')

gnosis_int = DataParser(
    DataParser.TYPE_EXPLORER,
    "GnosisScan (Gnosis Internal Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
        'ParentTxxDAI_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(xDAI)',
        'Value_OUT(xDAI)', None, 'Historical $Price/xDAI', 'Status', 'ErrCode', 'Type'],
    worksheet_name="GnosisScan",
    row_handler=parse_etherscan_like_internal,
    chain_asset='xDAI')

moonriver_int = DataParser(
    DataParser.TYPE_EXPLORER,
    "MoonScan (Moonriver Internal Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
        'ParentTxMOVR_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(MOVR)',
        'Value_OUT(MOVR)', None, 'Historical $Price/MOVR', 'Status', 'ErrCode', 'Type'],
    worksheet_name="MoonScan",
    row_handler=parse_etherscan_like_internal,
    chain_asset='MOVR')

polygon_int = DataParser(
    DataParser.TYPE_EXPLORER,
    "PolygonScan (Polygon Internal Transactions)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'ParentTxFrom', 'ParentTxTo',
        'ParentTxMATIC_Value', 'From', 'TxTo', 'ContractAddress', 'Value_IN(MATIC)',
        'Value_OUT(MATIC)', None, 'Historical $Price/MATIC', 'Status', 'ErrCode', 'Type'],
    worksheet_name="PolygonScan",
    row_handler=parse_etherscan_like_internal,
    chain_asset='MATIC')

etherscan_like_tokens = DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan Like (ERC-20 Tokens)",
    ['Txhash', 'UnixTimestamp', 'DateTime', 'From', 'To', 'TokenValue', None, 'ContractAddress',
     'TokenName', 'TokenSymbol'],
    worksheet_name="Etherscan Like",
    row_handler=parse_etherscan_like_tokens,
    chain_asset=None)

etherscan_like_tokens_2 = DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan Like (ERC-20 Tokens)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'TokenValue', None, 'ContractAddress',
     'TokenName', 'TokenSymbol'],
    worksheet_name="Etherscan Like",
    row_handler=parse_etherscan_like_tokens,
    chain_asset=None)

etherscan_like_nfts = DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan Like (ERC-721 NFTs)",
    ['Txhash', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress', 'TokenId',
     'TokenName', 'TokenSymbol'],
    worksheet_name="Etherscan",
    row_handler=parse_etherscan_like_nfts,
    chain_asset=None)

etherscan_like_nfts_2 = DataParser(
    DataParser.TYPE_EXPLORER,
    "Etherscan Like (ERC-721 NFTs)",
    ['Txhash', 'Blockno', 'UnixTimestamp', 'DateTime', 'From', 'To', 'ContractAddress', 'TokenId',
     'TokenName', 'TokenSymbol'],
    worksheet_name="Etherscan",
    row_handler=parse_etherscan_like_nfts,
    chain_asset=None)
