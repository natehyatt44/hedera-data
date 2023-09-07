import pandas as pd
import json

# Load data from the local nft_data.json file
with open("nft_transactionFULL.json", "r") as file:
    data = json.load(file)

# Extract data and store in CSV format with the added Transaction Type
csv_data = []

# Flatten the nested list structure based on your provided JSON format
flattened_data = [transaction for sublist in data for block in sublist for transaction in block["transactions"]]

# Convert flattened_data to DataFrame and drop duplicates based on 'transaction_id'
df_flattened = pd.DataFrame(flattened_data)
df_flattened.drop_duplicates(subset='transaction_id', inplace=True)

# Convert the DataFrame back to a list of dictionaries
flattened_data = df_flattened.to_dict('records')


def get_market_account_name(transfer_accounts):
    mapping = {
        "0.0.1064038": "sentx",
        "0.0.690356": "zuse"
    }

    for account_id in transfer_accounts:
        if account_id in mapping:
            return mapping[account_id]
    return "hashpack"


for item in flattened_data:
    timestamp = item['consensus_timestamp']
    transaction_type = item['name']

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
    individual_nft_price = total_amount / len(item['nft_transfers']) if item['nft_transfers'] else None

    for nft_transfer in item['nft_transfers']:
        receiver = nft_transfer['receiver_account_id']
        sender = nft_transfer['sender_account_id'] if nft_transfer['sender_account_id'] else "N/A"
        serial_number = nft_transfer['serial_number']

        # Use the get_market_account_name function to set market_id
        market_id = get_market_account_name(transfer_accounts)
        csv_data.append([timestamp, transaction_type, serial_number, receiver, sender, individual_nft_price, market_id])

# Convert csv_data to a DataFrame
df = pd.DataFrame(csv_data, columns=["Transaction Time", "Transaction Type", "Serial #", "Buyer", "Seller", "Amount",
                                     "Market ID"])

# Sort DataFrame by Serial # and then by Transaction Time (both ascending)
df = df.sort_values(by=["Serial #", "Transaction Time"])

# Remove duplicates
df = df.drop_duplicates()

# Write DataFrame to CSV using | as a delimiter
csv_filename = "nft_transaction.csv"
df.to_csv(csv_filename, sep='|', index=False)

# Calculate and print the total amount
total_amount = df["Amount"].sum()
print(f"Total Amount: {total_amount}")

csv_filename
