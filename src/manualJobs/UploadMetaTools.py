import requests
import json
import time
import os
import base64


token_id = '0.0.3954030'

def fetch_with_retries(url, max_retries = 3):
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
            nft_data.append({'serial_number': serial_number, 'ipfsCid': cid})

    if 'links' in nfts and 'next' in nfts['links']:
        if nfts['links']['next'] != None:
            nft_data.extend(fetch_nfts_from_mirror_node(nfts['links']['next']))

    return nft_data

def fetch_ipfs_metadata(nft_data):
    ipfs_gateway = 'https://ipfs.io/ipfs/'

    for nft in nft_data:
        if 'ipfsCid' in nft:
            ipfs_metadata_response = fetch_with_retries(f'{ipfs_gateway}{nft["ipfsCid"]}')
            ipfs_metadata = ipfs_metadata_response.json()
            nft['edition'] = ipfs_metadata['edition']
            #nft['attributes'] = ipfs_metadata['attributes']
    return nft_data

def is_playable(item):
    # Extract the traits for easier checking
    return 1
    # traits = {trait['trait_type']: trait['value'] for trait in item['attributes']}
    #
    # # Condition for Gaian
    # if traits.get('Race') == 'Gaian':
    #     return traits.get('Eyes') in ['Blind Fighter Red', 'Blind Fighter Blue'] or traits.get(
    #         'Mouth') == 'Skeleton'
    #
    # # Condition for Runekin
    # if traits.get('Race') == 'Runekin':
    #     return traits.get('Eyes') == 'Angry' or traits.get('Clothes') == 'Villager Tunic With Pendant'
    #
    # # Condition for Soulweaver
    # if traits.get('Race') == 'Soulweaver':
    #     return traits.get('Body') == 'Fire' or traits.get('Smoke') in ['Vape Skull Smoke', 'Vape Smoke'] or traits.get("Eyes Mask") in ['Smoke', 'Kitsune Mask'] or traits.get('Background') == 'Alixon Special'
    #
    # # Condition for Zephyr
    # if traits.get('Race') == 'Zephyr':
    #     return traits.get('Eye Wear') != 'Blank' or traits.get('Body') == 'Rainbow' or traits.get('Clothes') == 'Rainbow Suit' or traits.get('Background') == 'Alixon Special'
    #
    # # Condition for ArchAngel
    # if traits.get('Race') == 'ArchAngel':
    #     return
    #
    # # If none of the conditions are met, return False
    # return False

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
    result_data = []
    nft_data = fetch_nfts_from_mirror_node()
    nft_data_with_ipfs = fetch_ipfs_metadata(nft_data)

    for item in nft_data_with_ipfs:
        item_data = {
            'serial_number': item['serial_number'],
            'playable': 1 if is_playable(item) else 0,
            'tokenId': token_id,
            'tool': 'test',
            'forRace': 'test'
        }

    result_data.append(item_data)

    # Write the data to a JSON file
    with open('test.json', 'w') as f:
        json.dump(result_data, f, indent=2)

    # # S3 bucket and object name
    # s3_bucket = "lost-ones-upload32737-staging"
    # s3_object_name = "public/nft-collections/1.json"
    #
    # # Upload the JSON file to S3
    # upload_file_to_s3(json_file_path, s3_bucket, s3_object_name)


if __name__ == "__main__":
    main()
