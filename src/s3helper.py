import boto3
import io
import json
import pandas as pd
import base64
from datetime import datetime, timedelta
from botocore.exceptions import NoCredentialsError, PartialCredentialsError, ClientError

# Bucket used for processing data
bucket = 'lost-ones-upload32737-staging'

def read_json_s3(token_id, filename):
    """Read the config JSON from an S3 bucket."""
    s3 = boto3.client('s3')
    key = f'public/data-analytics/{token_id}/{filename}'
    default_config = {
        "last_nft_listing_ts": 0,
        "last_nft_transaction_ts":  0,
        "last_discord_listings_ts": "",
        "last_discord_sales_ts": ""
    }
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj['Body'].read().decode('utf-8')
        return json.loads(data)
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            print(f"No json {filename} found for token_id {token_id}.")
            return default_config
        else:
            print(f"Unexpected error: {e}")
            return default_config
    except (NoCredentialsError, PartialCredentialsError):
        print("Credentials not available")
        return default_config


def read_df_s3(token_id, filename):
    """Read a DataFrame from an S3 bucket."""
    s3 = boto3.client('s3')
    key = f'public/data-analytics/{token_id}/{filename}'
    try:
        obj = s3.get_object(Bucket=bucket, Key=key)
        data = obj['Body'].read().decode('utf-8')
        return pd.read_csv(io.StringIO(data), delimiter='|')  # Specify delimiter here
    except ClientError as e:
        if e.response['Error']['Code'] == "NoSuchKey":
            print(f"No data found for token_id {token_id} & {filename}.")
            return pd.DataFrame()
        else:
            print(f"Unexpected error: {e}")
            return pd.DataFrame()
    except (NoCredentialsError, PartialCredentialsError):
        print("Credentials not available")
        return pd.DataFrame()

def upload_json_s3(token_id, filename, json_data):
    """Update and save the config JSON to S3."""
    s3 = boto3.client('s3')
    key = f'public/data-analytics/{token_id}/{filename}'

    # Convert config to JSON format
    str_data = json.dumps(json_data)

    try:
        s3.put_object(Bucket=bucket, Key=key, Body=str_data)
        print(f"Updated json for token_id {token_id} {filename} saved to S3.")
    except ClientError as e:
        print(f"Error saving updated config to S3: {e}")
    except (NoCredentialsError, PartialCredentialsError):
        print("Credentials not available")


def upload_df_s3(token_id, filename, df):
    """Save the updated NFT data back to S3, overwriting the original file."""
    s3 = boto3.client('s3')
    key = f'public/data-analytics/{token_id}/{filename}'

    # Convert list of dictionaries to DataFrame
    df = pd.DataFrame(df)

    # Convert DataFrame to CSV format
    csv_buffer = io.StringIO()
    df.to_csv(csv_buffer, sep='|', index=False)

    try:
        s3.put_object(Bucket=bucket, Key=key, Body=csv_buffer.getvalue())
        print(f"Updated data token_id {token_id} {filename} saved to S3.")
    except ClientError as e:
        print(f"Error saving updated NFT data to S3: {e}")
    except (NoCredentialsError, PartialCredentialsError):
        print("Credentials not available")

def hedera_timestamp_to_datetime(timestamp):
    unix_epoch = datetime.strptime('1970-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
    seconds_since_epoch = int(float(timestamp))
    dt_object = unix_epoch + timedelta(seconds=seconds_since_epoch)

    # Convert datetime object to string in the desired format
    return dt_object.strftime('%Y-%m-%d %H:%M:%S')

def datetime_to_hedera_timestamp(dt_string):
    # Convert the input string to a datetime object
    dt_object = datetime.strptime(dt_string, '%Y-%m-%d %H:%M:%S')

    # Calculate the difference between the given date and the Unix epoch
    unix_epoch = datetime.strptime('1970-01-01T00:00:00Z', '%Y-%m-%dT%H:%M:%SZ')
    time_difference = dt_object - unix_epoch

    # Get the total seconds and add the microseconds fraction
    hedera_timestamp = time_difference.total_seconds()  # This gives the total seconds
    hedera_timestamp += time_difference.microseconds / 1e6  # Add fractional part

    return str(hedera_timestamp)

def hedera_timestamp_plus_days(hedera_timestamp, days=1):
    # Convert the Hedera timestamp to a Python datetime object
    seconds, nanoseconds = map(int, hedera_timestamp.split('.'))
    base_datetime = datetime.fromtimestamp(seconds)

    # Add the specified number of days
    new_datetime = base_datetime + timedelta(days=days)

    # Convert back to Hedera timestamp format
    new_seconds = int(new_datetime.timestamp())
    return f"{new_seconds}.{nanoseconds}"

def decode_memo_base64(encoded_str):
    """Decode a base64 encoded string."""
    decoded_bytes = base64.b64decode(encoded_str)
    return decoded_bytes.decode('utf-8')

def get_market_account_name(transfer_accounts):
    mapping = {
        "0.0.1064038": "SentX",
        "0.0.690356": "Zuse"
    }

    for account_id in transfer_accounts:
        if account_id in mapping:
            return mapping[account_id]
    return None