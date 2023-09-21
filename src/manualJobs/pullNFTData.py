import requests
import json
import time
import os
import base64


token_id = '0.0.3721853'

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
            nft['attributes'] = ipfs_metadata['attributes']
    return nft_data

def is_playable(item):
    # Extract the traits for easier checking
    traits = {trait['trait_type']: trait['value'] for trait in item['attributes']}

    # Condition for Gaian
    if traits.get('Race') == 'Gaian':
        return traits.get('Eyes') in ['Blind Fighter Red', 'Blind Fighter Blue'] or traits.get(
            'Mouth') == 'Skeleton'

    # Condition for Runekin
    if traits.get('Race') == 'Runekin':
        print (traits)
        return traits.get('Eyes') == 'Angry' or traits.get('Clothes') == 'Villager Tunic With Pendant'

    # If none of the conditions are met, return False
    return False

def main():
    nft_data = fetch_nfts_from_mirror_node()
    with open('nftMirroNodeLO.json', 'w') as f:
        json.dump(nft_data, f, indent=2)

    nft_data_with_ipfs = fetch_ipfs_metadata(nft_data)
    with open('nftIpfsLO.json', 'w') as f:
        json.dump(nft_data_with_ipfs, f, indent=2)

    with open('nftIpfsLO.json', 'r') as f:
        data_from_file = json.load(f)

    for item in data_from_file:
        if 'ipfsCid' in item:
            del item['ipfsCid']
        item['playable'] = 1 if is_playable(item) else 0

    with open('nftMetadataLO.json', 'w') as f:
        json.dump(data_from_file, f, indent=2)

if __name__ == "__main__":
    main()
