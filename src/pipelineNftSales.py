import requests
import json
import time
from datetime import datetime, timedelta
import csv
import pandas as pd
import os
import base64
import re
import s3helper

API_CALL_LIMIT = 20

# Used for full loads
def save_next_url(market_id, next_url):
    """Save the next_url to a JSON file."""
    file_name = f'full_{market_id}.json'
    data = {
        'next_url': next_url
    }

    with open(file_name, 'w') as file:
        json.dump(data, file)

def fetch_transactions(token_ids, market_id, config, load_type, next_url=None, counter=0):
    url = 'https://mainnet-public.mirrornode.hedera.com'

    if market_id == '0.0.690356':
        last_timestamp = config["last_nft_transaction_zuse_ts"]
    elif market_id == '0.0.1064038':
        last_timestamp = config["last_nft_transaction_sentx_ts"]

    if load_type == "incr":
        last_timestamp_float = float(last_timestamp)
    else:
        last_timestamp_float = float("1680344833.048029498")  # Beginning of CFPS

        if next_url == None:
            # Load next_url from saved file if it exists
            file_name = f'full_{market_id}.json'
            try:
                with open(file_name, 'r') as file:
                    saved_data = json.load(file)
                    next_url = saved_data.get("next_url")
            except FileNotFoundError:
                pass

    if counter >= API_CALL_LIMIT and load_type != "incr":
        save_next_url(market_id, next_url)
        return []

    if next_url:
        path = next_url
    else:
        if load_type == "incr":
            path = f'/api/v1/accounts/{market_id}?transactionType=cryptotransfer'
        else:
            path = f'/api/v1/accounts/{market_id}?transactionType=cryptotransfer&timestamp=lt:{last_timestamp}'

    response = requests.get(f'{url}{path}')
    data = response.json()

    transactions = data.get('transactions', [])
    filtered_transactions = [txn for txn in transactions if
                             any(data.get('token_id') in token_ids for data in txn.get('nft_transfers', []))]

    if 'links' in data and 'next' in data['links']:
        next_url = data['links']['next']

        match = re.search(r'timestamp=lt:(\d+\.\d+)', next_url)
        if match:
            next_timestamp_float = float(match.group(1))
            if next_timestamp_float > last_timestamp_float:
                filtered_transactions.extend(
                    fetch_transactions(token_ids, market_id, config, load_type, next_url, counter=counter + 1))
    else:
        if load_type != "incr":
            save_next_url(market_id, next_url)

    return filtered_transactions

def nft_sales(transactions):

    # Convert flattened_data to DataFrame and drop duplicates based on 'transaction_id'
    df_flattened = pd.DataFrame(transactions)
    df_flattened.drop_duplicates(subset='transaction_id', inplace=True)

    # Convert the DataFrame back to a list of dictionaries
    flattened_data = df_flattened.to_dict('records')

    csv_data = []

    for item in flattened_data:
        txn_time_as_datetime = s3helper.hedera_timestamp_to_datetime(item['consensus_timestamp'])
        txn_id = item['transaction_id']

        # Get all the account IDs from the transfers list
        transfer_accounts = [transfer['account'] for transfer in item.get('transfers', [])]

        # Calculate the total amount for the transaction
        total_amount = 0
        for transfer in item.get('transfers', []):
            if any(transfer['account'] == nft['receiver_account_id'] for nft in item['nft_transfers']):
                total_amount += abs(float(transfer['amount']) / 100000000)

        # Check if total_amount is 0; if it is, continue to the next iteration
        if total_amount == 0:
            continue

        # Calculate individual NFT price by dividing by the number of nft_transfers
        individual_nft_price = round(total_amount / len(item['nft_transfers']), 2) if item['nft_transfers'] else None

        for nft_transfer in item['nft_transfers']:
            receiver = nft_transfer['receiver_account_id']
            sender = nft_transfer['sender_account_id'] if nft_transfer['sender_account_id'] else "N/A"
            serial_number = nft_transfer['serial_number']
            token_id = nft_transfer['token_id']

            # Use the get_market_account_name function to set market_id
            market_name = s3helper.get_market_account_name(transfer_accounts)
            csv_data.append([txn_time_as_datetime, txn_id, 'Sale', market_name, sender, receiver, token_id, serial_number, individual_nft_price ])

    # Convert csv_data to a DataFrame
    df = pd.DataFrame(csv_data, columns=["txn_time",
                                         "txn_id",
                                         "txn_type",
                                         "market_name",
                                         "account_id_seller",
                                         "account_id_buyer",
                                         "token_id",
                                         "serial_number",
                                         "amount"])

    # Remove duplicates
    df = df.drop_duplicates()

    return df

def execute(token_ids, market_ids):
    # Pull config file
    config = s3helper.read_json_s3('0.0.2235264', 'nft_config.json') # Hardcoded to CFP as we grab all token listings at the same time.

    # Initialize an empty list to store individual DataFrames
    dfs = []

    for market_id in market_ids:
        transactions = fetch_transactions(token_ids, market_id, config, load_type = "incr")
        df = nft_sales(transactions)
        dfs.append(df)

        # Determine the earliest consensus_timestamp from the transactions
        max_txn_time = min(transactions, key=lambda x: x['consensus_timestamp'])['consensus_timestamp'] if transactions else None

        # Update the respective field in the config dictionary
        if market_id == '0.0.690356' and max_txn_time:
            config['last_nft_transaction_zuse_ts'] = max_txn_time
        elif market_id == '0.0.1064038' and max_txn_time:
            config['last_nft_transaction_sentx_ts'] = max_txn_time
        # You can add more conditions here if there are more markets

    # Concatenate all DataFrames into a single DataFrame
    main_df = pd.concat(dfs, ignore_index=True)

    # Separate the main DataFrame based on token_id and save separate files
    for token_id in token_ids:
        sales_token_df = main_df[main_df['token_id'] == token_id]
        if not sales_token_df.empty:
            existing_sales_df = s3helper.read_df_s3(token_id, 'nft_transactions.csv')

            # If there's data in existing_sales_df, then merge
            if not existing_sales_df.empty:
                # Filter out entries from existing_listings_df that already exist in new_listings_df
                existing_sales_df = existing_sales_df[
                    ~existing_sales_df['txn_id'].isin(sales_token_df['txn_id'])]

                # Concatenate the two DataFrames
                combined_df = pd.concat([existing_sales_df, sales_token_df], ignore_index=True)
            else:
                combined_df = sales_token_df

            # If you still want to sort them (e.g., by 'txn_time' in descending order)
            combined_df.sort_values(by='txn_time', ascending=False, inplace=True)

            s3helper.upload_df_s3(token_id, 'nft_transactions.csv', combined_df)

    s3helper.upload_json_s3('0.0.2235264', 'nft_config.json', config)


