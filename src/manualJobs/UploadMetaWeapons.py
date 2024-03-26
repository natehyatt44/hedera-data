import requests
import json
import time
import os
import base64
import boto3


token_id = '0.0.4605609'
s3_bucket = "lost-ones-upload32737-staging"
s3_object_name = "public/nft-collections/ARGdatamap.json"
local_json_file = 'ARGdatamap.json'

def download_file_from_s3(bucket, object_name, file_path):
    """
    Downloads a file from an S3 bucket.
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.download_file(bucket, object_name, file_path)
        print(f"File {file_path} downloaded from {bucket}/{object_name}")
    except Exception as e:
        print(f"Error downloading file from S3: {e}")

def upload_file_to_s3(file_path, bucket, object_name):
    """
    Uploads a file to an S3 bucket.
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket, object_name)
        print(f"File {file_path} uploaded to {bucket}/{object_name}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")

def fetch_with_retries(url, max_retries = 10):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url)
            if response.status_code != 200:
                raise ValueError('Fetch failed')
            return response
        except Exception as e:
            retries += 1
            print(f'Retrying fetch ({retries}/{max_retries}): {str(e)}')
            if retries == max_retries:
                raise ValueError('Max retries reached')
            time.sleep(3)

def fetch_nfts_from_mirror_node(nextUrl = None):
    url = 'https://mainnet-public.mirrornode.hedera.com'
    path = nextUrl or f'/api/v1/tokens/{token_id}/nfts?limit=100'

    response = requests.get(f'{url}{path}')
    nfts = response.json()

    nft_data = []
    if len(nfts['nfts']) > 0:
        for item in nfts['nfts']:
            ipfs_hash = item['metadata']
            serial_number = item['serial_number']
            metadata = base64.b64decode(ipfs_hash).decode('utf-8')
            cid = metadata.replace('ipfs://', '')
            #if (serial_number in (24,23,22,21,20, 19,18,17,16,15,14,1,2,3,5,6,7,8)):
            nft_data.append({'serial_number': serial_number, 'ipfsCid': cid})

    if 'links' in nfts and 'next' in nfts['links']:
        if nfts['links']['next'] != None:
            nft_data.extend(fetch_nfts_from_mirror_node(nfts['links']['next']))

    print (nft_data)
    return nft_data

def fetch_ipfs_metadata(nft_data):
    ipfs_gateway = 'https://ipfs.io/ipfs/'

    for nft in nft_data:
        if 'ipfsCid' in nft:
            print(f'{ipfs_gateway}{nft["ipfsCid"]}')
            ipfs_metadata_response = fetch_with_retries(f'{ipfs_gateway}{nft["ipfsCid"]}')
            ipfs_metadata = ipfs_metadata_response.json()
            nft['edition'] = ipfs_metadata['edition']
            nft['attributes'] = ipfs_metadata['attributes']
    return nft_data

def is_playable(item):
    # Extract the traits for easier checking
    traits = {trait['trait_type']: trait['value'] for trait in item['attributes']}

    if traits.get('Weapon') == 'Spirit Armor':
        return traits.get('Background') in ['Black']
    if traits.get('Weapon') == 'Stasis Cube':
        return traits.get('Background') in ['Light Blend']
    if traits.get('Weapon') == 'Soft-Body Curse':
        return traits.get('Background') in ['Dark Grey']
    if traits.get('Weapon') == 'Angelic Strike':
        return traits.get('Background') in ['Grey']
    if traits.get('Weapon') == "Lightning Claws":
        return traits.get('Background') in ['Light Purple']
    if traits.get('Weapon') == 'Micro Burst':
        return traits.get('Background') in ['Blue']
    if traits.get('Weapon') == 'Aetherized Apex Rifle':
        return traits.get('Background') in ['Red']
    if traits.get('Weapon') == 'Apex Laser Sword':
        return traits.get('Background') in ['Moss Green']
    if traits.get('Weapon') == 'Sword of Gabriel':
        return traits.get('Background') in ['Green']
    if traits.get('Weapon') == 'Storm Staff':
        return traits.get('Background') in ['Yellow']
    if traits.get('Weapon') == "Apex Rifle":
        return traits.get('Background') in ['Dark Blue']
    if traits.get('Weapon') == 'Storm Sigil':
        return traits.get('Background') in ['Blue']

    # If none of the conditions are met, return False
    return False

def path(item):
    # Extract the traits for easier checking
    traits = {trait['trait_type']: trait['value'] for trait in item['attributes']}

    if traits.get('Weapon') in ('Stasis Cube', 'Aetherized Apex Rifle', 'Lightning Claws', 'Angelic Strike', 'Apex Rifle', 'Storm Sigil'):
        return 'A'
    if traits.get('Weapon') in ('Sword of Gabriel', 'Soft-Body Curse', 'Micro Burst', 'Spirit Armor', 'Apex Laser Sword', 'Storm Staff'):
        return 'B'

    return 'Nothing'

def forRace(item):
    # Extract the traits for easier checking
    traits = {trait['trait_type']: trait['value'] for trait in item['attributes']}

    if traits.get('Weapon') in ('Stasis Cube',  "Sword of Gabriel"):
        return 'ArchAngel'
    if traits.get('Weapon') in ('Aetherized Apex Rifle',  "Soft-Body Curse"):
        return 'Soulweaver'
    if traits.get('Weapon') in ('Lightning Claws',  "Micro Burst"):
        return 'Zephyr'
    if traits.get('Weapon') in ('Angelic Strike',  "Spirit Armor"):
        return 'Runekin'
    if traits.get('Weapon') in ('Apex Rifle',  "Apex Laser Sword"):
        return 'Mortal'
    if traits.get('Weapon') in ('Storm Sigil',  "Storm Staff"):
        return 'Gaian'

    return 'Nothing'

def upload_file_to_s3(file_path, bucket, object_name):
    """
    Uploads a file to an S3 bucket.
    """
    s3_client = boto3.client('s3')
    try:
        s3_client.upload_file(file_path, bucket, object_name)
        print(f"File {file_path} uploaded to {bucket}/{object_name}")
    except Exception as e:
        print(f"Error uploading file to S3: {e}")

def main():
    # First, download the existing JSON file from S3
    download_file_from_s3(s3_bucket, s3_object_name, local_json_file)

    # Load the existing data
    with open(local_json_file, 'r') as f:
        existing_data = json.load(f)

    result_data = []
    nft_data = fetch_nfts_from_mirror_node()
    nft_data_with_ipfs = fetch_ipfs_metadata(nft_data)

    for item in nft_data_with_ipfs:
        item_data = {
            'serial_number': item['serial_number'],
            'playable': 1 if is_playable(item) else 0,
            'tokenId': token_id,
            'type': 'Weapon',
            'forRace': forRace(item),
            'path': path(item)
        }
        result_data.append(item_data)

    # Replace all records with the same token ID
    updated_data = [item for item in existing_data if item['tokenId'] != token_id]
    # Add new tokenID data to the TOP of the JSON file
    updated_data = result_data + updated_data

    # Write the updated data to ARGdatamap.json
    with open('ARGdatamap.json', 'w') as f:
        json.dump(updated_data, f, indent=2)

    # Optionally upload the updated file back to S3
    upload_file_to_s3('ARGdatamap.json', s3_bucket, s3_object_name)


if __name__ == "__main__":
    while True:
        main()
        print("Script execution completed. Waiting for 30 minutes before next run.")
        time.sleep(10)  # Wait for 1800 seconds (30 minutes) before next execution
