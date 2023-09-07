import requests
import json
import time
import datetime
import csv
from dotenv import load_dotenv
import os

load_dotenv()

def get_latest_timestamp_from_csv(filename):
    with open(filename, 'r') as csvfile:
        reader = csv.reader(csvfile, delimiter='|')
        next(reader)  # Skip the header row

        # Extract the Transaction Time column
        timestamps = [float(row[0]) for row in reader]

    # Return the maximum timestamp
    return max(timestamps)

CFP_TOKEN_ID = os.environ["CFP_TOKEN_ID"]
def fetch_transaction_from_mirror_node(transactionId):
    url = 'https://mainnet-public.mirrornode.hedera.com'
    path = f'/api/v1/transactions/{transactionId}'

    response = requests.get(f'{url}{path}')
    transaction = response.json()

    # In case there are no transactions in the response
    return transaction


def fetch_nfts_from_mirror_node(serialNumber, nextUrl = None, last_timestamp=None):
    url = 'https://mainnet-public.mirrornode.hedera.com'

    # If last_timestamp is provided, append it to the path
    if last_timestamp:
        path = nextUrl or f'/api/v1/tokens/{CFP_TOKEN_ID}/nfts/{serialNumber}/transactions?timestamp=lte:{last_timestamp}'
    else:
        path = nextUrl or f'/api/v1/tokens/{CFP_TOKEN_ID}/nfts/{serialNumber}/transactions'

    print (path)
    response = requests.get(f'{url}{path}')
    nfts = response.json()
    print (nfts)
    mint_transaction_ids = set(nft['transaction_id'] for nft in nfts['transactions'] if nft['type'] == 'TOKENMINT')

    nft_data = []
    for nft in nfts['transactions']:
        transaction_id = nft['transaction_id']
        if nft['type'] == 'CRYPTOTRANSFER' and transaction_id not in mint_transaction_ids:
            transaction_data = fetch_transaction_from_mirror_node(transaction_id)
            nft_data.append(transaction_data)

    if 'links' in nfts and 'next' in nfts['links']:
        if nfts['links']['next'] != None:
            nft_data.extend(fetch_nfts_from_mirror_node(serialNumber, nfts['links']['next']))

    return nft_data

def main():
    # Using the function
    filename = 'nft_transaction.csv'  # Replace with the actual filename or path
    last_timestamp = get_latest_timestamp_from_csv(filename)
    print(f"Latest transaction time: {last_timestamp}")

    all_nft_data = []
    for serialNumber in range(246,248):  # Loop over serial numbers 1-1000
        nft_data = fetch_nfts_from_mirror_node(serialNumber, last_timestamp=last_timestamp)

        all_nft_data.append(nft_data)

    # Save all_nft_data to a JSON file
    with open('nft_transaction.json', 'w') as f:
        json.dump(all_nft_data, f, indent=4)

if __name__ == "__main__":
    main()


    # 0.0.690356@1685285926.978764271 This transaction doesn't exist for serial 247 in mirrornode
