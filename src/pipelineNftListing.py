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
            account_id = item['account_id']
            token_id = item['token_id']
            serial_number = item['serial_number']
            modified_timestamp = item['modified_timestamp']
            spender = item['spender']
            nft_data.append({'account_id': account_id, 'token_id': token_id, 'serial_number': serial_number,
                             'modified_timestamp': modified_timestamp, 'spender': spender})

    if 'links' in nfts and 'next' in nfts['links']:
        if nfts['links']['next'] != None:
            nft_data.extend(fetch_all_nfts(token_id, nfts['links']['next']))

    return nft_data

def compare_nfts_with_existing_data(token_id, config, nft_data):
    # Convert the spender column
    for item in nft_data:
        item['spender'] = s3helper.get_market_account_name([item['spender']])

    # Convert the list of dictionaries into DataFrame
    current_nft_df = pd.DataFrame(nft_data)

    # Convert columns to correct data type for comparison
    current_nft_df['modified_timestamp'] = current_nft_df['modified_timestamp'].astype(float)

    last_nft_listing_ts = config["last_nft_listing_ts"]

    if pd.notna(last_nft_listing_ts):
        updated_nft_df = current_nft_df[current_nft_df['modified_timestamp'] > float(last_nft_listing_ts)]
    else:
        updated_nft_df = current_nft_df

    # Extract the updated NFTs' details
    updated_nft_records = updated_nft_df[
        ['account_id', 'token_id', 'serial_number', 'modified_timestamp', 'spender']].to_dict(orient='records')

    # Save the new NFT data
    s3helper.upload_df_s3(token_id, 'nft_collection.csv', current_nft_df)

    return updated_nft_records

def fetch_transaction_from_mirror_node(transactionId):
    url = 'https://mainnet-public.mirrornode.hedera.com'
    path = f'/api/v1/transactions/{transactionId}'

    response = requests.get(f'{url}{path}')
    transaction = response.json()

    # In case there are no transactions in the response
    return transaction

def fetch_nfts_from_mirror_node(token_id, config, nft_record, nextUrl = None):
    url = 'https://mainnet-public.mirrornode.hedera.com'

    # If last_timestamp is provided, append it to the path
    path = nextUrl or f'/api/v1/tokens/{token_id}/nfts/{nft_record["serial_number"]}/transactions'

    response = requests.get(f'{url}{path}')
    nfts = response.json()

    nft_listing_data = []
    for nft in nfts['transactions']:
        if nft['type'] in ['CRYPTOAPPROVEALLOWANCE'] and float(nft['consensus_timestamp']) > float(config["last_nft_listing_ts"]):
            listing_data = fetch_transaction_from_mirror_node(nft['transaction_id'])
            # Append the nft_record data to the listing_data
            listing_data['account_id'] = nft_record['account_id']
            listing_data['token_id'] = nft_record['token_id']
            listing_data['serial_number'] = nft_record['serial_number']
            listing_data['modified_timestamp'] = nft_record['modified_timestamp']
            nft_listing_data.append(listing_data)

    if 'links' in nfts and 'next' in nfts['links']:
        if nfts['links']['next'] != None:
            nft_listing_data.extend(fetch_nfts_from_mirror_node(token_id, config, nft_record, nfts['links']['next']))

    return nft_listing_data

def extract_hbar_amount(memo_decoded):
    # Check for patterns and extract the HBAR amount
    patterns = [
        r"for (\d+) HBAR",     # Matches "for 665 HBAR" or "for 400 HBAR"
        r"#\d+ for (\d+) HBAR" # Matches "#930 for 400 HBAR" or "#27 for 2222 HBAR"
    ]

    for pattern in patterns:
        match = re.search(pattern, memo_decoded)
        if match:
            return match.group(1)  # Return the matched HBAR amount
    return None  # If no matches found

def extract_market_name(memo_decoded):
    # Check for unique patterns in memo to determine the market name
    # Zuse Example:
    #   (0.0.1317633) Confirm listing of NFT: 0.0.2235264 with serial number 29 for 500 HBAR
    # SentX Examples:
    #   Approve NFT Token 0.0.2235264 Serial 27 marketplace listing for 2500 HBAR
    #   SentX Market Listing: NFT 0.0.2235264 #27 for 2222 HBAR
    #   Approve Bulk Listing of 5 NFTs on Sentient Marketplace
    #   SentX Market Bulk Listing: 4 NFTs

    if "Confirm listing of NFT:" in memo_decoded:
        return "Zuse"
    elif "SentX" in memo_decoded or "Approve NFT Token" in memo_decoded:
        return "SentX"
    return None  # If no market name can be determined

def nft_listings(token_id, transactions):
    listings = []

    for transaction_block in transactions:
        txn = transaction_block['transactions'][0]
        memo_decoded = s3helper.decode_memo_base64(txn['memo_base64'])
        market_name = extract_market_name(memo_decoded)

        # If the decoded memo includes the word "Bulk", since we can't pull amount put bulk listing in amount
        if "Bulk" in memo_decoded:
            amount = 'Bulk Listing'
        else:
            amount = extract_hbar_amount(memo_decoded)


        if amount:  # Only proceed if we've successfully extracted an amount
            txn_time_as_datetime = s3helper.hedera_timestamp_to_datetime(txn['consensus_timestamp'])
            txn_id = txn['transaction_id']

            listings.append({
                'txn_time': txn_time_as_datetime,
                'txn_id': txn_id,
                'txn_type': "List",
                'account_id_seller': transaction_block['account_id'],
                'token_id': transaction_block['token_id'],
                'serial_number': transaction_block['serial_number'],
                'market_name': market_name,  # using spender as market_name
                'amount': amount
            })

    if listings:
        # listings not empty
        new_listings_df = pd.DataFrame(listings)

        # Convert the columns to string type for consistency
        # columns_to_convert = ['account_id_seller', 'token_id', 'serial_number']
        # for column in columns_to_convert:
        #     new_listings_df[column] = new_listings_df[column].astype(str)

        # Pull existing list data, merge, and re-upload
        existing_listings_df = s3helper.read_df_s3(token_id, 'nft_listings.csv')

        # If there's data in existing_listings_df, then merge
        if not existing_listings_df.empty:
            # Filter out entries from existing_listings_df that already exist in new_listings_df
            existing_listings_df = existing_listings_df[~existing_listings_df['txn_id'].isin(new_listings_df['txn_id'])]

            # Concatenate the two DataFrames
            combined_df = pd.concat([new_listings_df, existing_listings_df], ignore_index=True)
        else:
            combined_df = new_listings_df

        # If you still want to sort them (e.g., by 'txn_time' in descending order)
        combined_df.sort_values(by='txn_time', ascending=False, inplace=True)

        s3helper.upload_df_s3(token_id, 'nft_listings.csv', combined_df)



def execute(token_id):
    # Pull config file
    config = s3helper.read_json_s3(token_id, 'nft_config.json')
    # Pull nft_data
    nft_data = fetch_all_nfts(token_id)
    # Find updated records
    updated_nft_records = compare_nfts_with_existing_data(token_id, config, nft_data)

    # If no updated records just skip this.
    if not updated_nft_records:
        return

    all_nft_data = []  # To store data fetched for each updated serial number

    # Loop over updated NFTs and fetch details from mirror node
    for nft_record in updated_nft_records:
        nft_transactions = fetch_nfts_from_mirror_node(token_id, config, nft_record)
        all_nft_data.extend(nft_transactions)

    nft_listings(token_id, all_nft_data)

    # Assuming nft_data has been populated using the fetch_all_nfts function
    nft_data_sorted = sorted(nft_data, key=lambda x: x['modified_timestamp'], reverse=True)
    most_recent_timestamp = nft_data_sorted[0]['modified_timestamp'] if nft_data_sorted else None
    config['last_nft_listing_ts'] = most_recent_timestamp
    s3helper.upload_json_s3(token_id, 'nft_config.json', config)
