import requests
import json
import time
from datetime import datetime, timedelta
import csv
import pandas as pd
import os
import base64
import re
import src.s3helper as s3helper

def fetch_all_nfts(token_id, nextUrl=None):
    url = 'https://mainnet-public.mirrornode.hedera.com'
    path = nextUrl or f'/api/v1/tokens/{token_id}/nfts/'

    response = requests.get(f'{url}{path}')
    nfts = response.json()

    nft_data = []
    if len(nfts['nfts']) > 0:
        for item in nfts['nfts']:
            serial_number = item['serial_number']
            nft_data.append(serial_number)

    if 'links' in nfts and 'next' in nfts['links']:
        if nfts['links']['next'] != None:
            nft_data.extend(fetch_all_nfts(token_id, nfts['links']['next']))

    return nft_data
def fetch_transaction_from_mirror_node(transactionId):
    url = 'https://mainnet-public.mirrornode.hedera.com'
    path = f'/api/v1/transactions/{transactionId}'

    response = requests.get(f'{url}{path}')
    transaction = response.json()

    # In case there are no transactions in the response
    return transaction

def fetch_nft_mints(token_id, serial_number, nextUrl = None):
    url = 'https://mainnet-public.mirrornode.hedera.com'

    # If last_timestamp is provided, append it to the path
    path = nextUrl or f'/api/v1/tokens/{token_id}/nfts/{serial_number}/transactions'

    response = requests.get(f'{url}{path}')
    nfts = response.json()

    nft_mint_data = []
    for nft in nfts['transactions']:
        if nft['type'] in ['TOKENMINT']:
            nft_mint = fetch_transaction_from_mirror_node(nft['transaction_id'])
            nft_mint_data.append(nft_mint)

    if 'links' in nfts and 'next' in nfts['links']:
        if nfts['links']['next'] != None:
            nft_mint_data.extend(fetch_nft_mints(token_id, serial_number, nfts['links']['next']))

    return nft_mint_data

def get_receiver_account_for_cryptotransfer(transaction):
    """Return the receiver_account_id for a CRYPTOTRANSFER transaction."""
    if transaction['name'] == "CRYPTOTRANSFER" and transaction['nft_transfers']:
        return transaction['nft_transfers'][0]['receiver_account_id']
    return None

def get_mint_price_for_contractcall(transaction, receiver_account_id, nft_mint_cnt):
    """Return the mint price amount for a CONTRACTCALL transaction."""
    if transaction['name'] == "CONTRACTCALL":
        for transfer in transaction['transfers']:
            if transfer['account'] == receiver_account_id:
                # Returning the positive value of the amount as mint price
                return round((abs(transfer['amount']) / 100000000) / nft_mint_cnt, 2)
    return 0

def execute(token_id):
    # Pull nft_data
    nfts = fetch_all_nfts(token_id)
    #nfts = ['101', '1', '2', '3', '300']
    csv_data = []

    for serial_number in nfts:
        nft_mints = fetch_nft_mints(token_id, serial_number)

        # Initialize variables for each serial number
        receiver_account_id = None
        txn_time_as_datetime = None
        nft_mint_cnt = 1
        mint_price = 0

        # Iterating over each transaction
        for transaction in nft_mints[0]['transactions']:
            if transaction['name'] == "CRYPTOTRANSFER":
                receiver_account_id = get_receiver_account_for_cryptotransfer(transaction)
                nft_mint_cnt = len(transaction['nft_transfers'])

        for transaction in nft_mints[0]['transactions']:
            if transaction['name'] == "CONTRACTCALL":
                mint_price = get_mint_price_for_contractcall(transaction, receiver_account_id, nft_mint_cnt)

            txn_time_as_datetime = s3helper.hedera_timestamp_to_datetime(transaction['consensus_timestamp'])
            txn_id = transaction['transaction_id']

        if receiver_account_id is None:
            receiver_account_id = 'Company'

        csv_data.append([txn_time_as_datetime, txn_id, 'Mint', receiver_account_id, token_id, serial_number, mint_price])

    # Convert csv_data to a DataFrame once all serial numbers are processed
    df = pd.DataFrame(csv_data, columns=["txn_time",
                                         "txn_id",
                                         "txn_type",
                                         "account_id_buyer",
                                         "token_id",
                                         "serial_number",
                                         "amount"])

    return df

def main():
    CFP = "0.0.2235264"
    AD = "0.0.2371643"
    TLO = "0.0.3721853"

    token_id = AD
    results = execute(token_id)
    s3helper.upload_df_s3(token_id, 'nft_mints.csv', results)


if __name__ == '__main__':
    main()

