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

api_key = 'c8ee2170-ebc3-4c2a-b6f0-87560b282626'

def fetch_nft_floor(token_id):
    url = f'https://api.sentx.io/v1/public/market/floor?apikey={api_key}&token={token_id}'

    response = requests.get(url)
    nft_floor = response.json()

    # In case there are no transactions in the response
    return nft_floor

def fetch_nft_listings(token_id):
    url = f'https://api.sentx.io/v1/public/market/listings?apikey={api_key}&token={token_id}'


    response = requests.get(url)
    nft_listings = response.json()

    # In case there are no transactions in the response
    return nft_listings

def transform_to_dataframe(nft_listings):
    """Transform the nft_listings JSON data to a DataFrame."""
    csv_data = []
    for listing in nft_listings['marketListings']:
        row = [
            datetime.strptime(listing['listingDate'], "%Y-%m-%dT%H:%M:%S.%fZ").strftime('%Y-%m-%d %H:%M:%S'),
            listing['marketplaceListingId'],  # Assuming this as transaction id
            "List",  # Assuming all entries from this source are listings
            listing['sellerAccount'],
            listing['nftTokenAddress'],
            listing['nftSerialId'],
            'SentX',
            listing['salePrice'],
            0
        ]
        csv_data.append(row)

    df = pd.DataFrame(csv_data, columns=["txn_time", "txn_id", "txn_type", "account_id_seller", "token_id", "serial_number", "market_name", "amount", "old_amount"])
    return df


def compare_and_update_records(existing_df, new_df):
    """Compare and update records, return updated DataFrame and new records DataFrame."""
    new_records = []

    for index, row in new_df.iterrows():
        existing_row = existing_df[existing_df['serial_number'] == row['serial_number']]
        if existing_row.empty:
            # New record
            new_records.append(row)
        elif not (existing_row['amount'].iloc[0] == row['amount']):
            # Updated Price

            row['txn_type'] = 'Updated Price'
            row['txn_time'] = datetime.utcnow().strftime('%Y-%m-%d %H:%M:%S')
            row['old_amount'] = int(round(float(existing_row['amount'].iloc[0]), 0))
            new_records.append(row)

    new_records_df = pd.DataFrame(new_records)

    return new_records_df


def execute(token_id):
    # Fetch new data
    results = fetch_nft_listings(token_id)
    new_df = transform_to_dataframe(results)

    # Load existing data
    existing_df = s3helper.read_df_s3(token_id, 'nft_sentx_listings.csv')

    # Compare and update records
    if not existing_df.empty:
        new_listings_df = compare_and_update_records(existing_df, new_df)

        # If there are new records, save them to a new CSV
        if not new_listings_df.empty:
            existing_listings_df = s3helper.read_df_s3(token_id, 'nft_listings.csv')

            # Concatenate the two DataFrames
            combined_df = pd.concat([new_listings_df, existing_listings_df], ignore_index=True)

            combined_df.sort_values(by='txn_time', ascending=False, inplace=True)

            s3helper.upload_df_s3(token_id, 'nft_listings.csv', combined_df)

    # Save new data to CSV
    s3helper.upload_df_s3(token_id, 'nft_sentx_listings.csv', new_df)
    #return results



def main():
    CFP = "0.0.2235264"
    AD = "0.0.2371643"
    TLO = "0.0.3721853"

    token_id = CFP
    results = execute(token_id)


if __name__ == '__main__':
    main()

