import boto3
import requests
import base64
import time
import json
from PIL import Image
from io import BytesIO

token_id = '0.0.3954030'

def fetch_with_retries(url, max_retries=3):
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

def fetch_nfts_from_mirror_node(nextUrl=None):
    url = 'https://mainnet-public.mirrornode.hedera.com'
    path = nextUrl or f'/api/v1/tokens/{token_id}/nfts?limit=100'

    response = requests.get(f'{url}{path}')
    nfts = response.json()

    nft_data = []
    if 'nfts' in nfts and len(nfts['nfts']) > 0:
        for item in nfts['nfts']:
            ipfs_hash = item['metadata']
            serial_number = item['serial_number']
            metadata = base64.b64decode(ipfs_hash).decode('utf-8')
            cid = metadata.replace('ipfs://', '')
            nft_data.append({'serial_number': serial_number, 'ipfsCid': cid})

    if 'links' in nfts and 'next' in nfts['links'] and nfts['links']['next'] is not None:
        nft_data.extend(fetch_nfts_from_mirror_node(nfts['links']['next']))

    return nft_data

def fetch_ipfs_metadata(nft_data):
    ipfs_gateway = 'https://ipfs.io/ipfs/'

    for nft in nft_data:
        if 'ipfsCid' in nft:
            ipfs_metadata_response = fetch_with_retries(f'{ipfs_gateway}{nft["ipfsCid"]}')
            ipfs_metadata = ipfs_metadata_response.json()
            nft['image'] = ipfs_metadata['image']
            #nft['attributes'] = ipfs_metadata['attributes']
    return nft_data

def download_image(url, max_retries=3):
    retries = 0
    while retries < max_retries:
        try:
            response = requests.get(url, stream=True)
            if response.status_code == 200:
                response.raw.decode_content = True
                image = Image.open(response.raw)
                image.load()  # Ensure the entire image is loaded
                return image
            else:
                raise ValueError('Image download failed')
        except (OSError, ValueError) as e:
            print(f"Error downloading image: {e}. Retrying ({retries + 1}/{max_retries})...")
            retries += 1
            time.sleep(1)  # Wait a bit before retrying

    raise ValueError('Max retries reached for image download')

def resize_image(image, size=(512, 512)):
    return image.resize(size)

def upload_to_s3(image, bucket, object_name):
    img_byte_arr = BytesIO()
    image.save(img_byte_arr, format='WEBP')
    img_byte_arr = img_byte_arr.getvalue()

    s3_client = boto3.client('s3')

    # Upload to both specified directories
    for directory in ["public/testing/", "public/nft-collections/Tool/images/"]:
        if directory in ["public/testing/"]:
            full_object_name = f"{directory}{token_id}/images/{object_name}"
        else:
            full_object_name = f"{directory}{object_name}"
        s3_client.put_object(Body=img_byte_arr, Bucket=bucket, Key=full_object_name, ACL='public-read', ContentType='image/webp')
        print(f"Image uploaded to {bucket}/{full_object_name}")

def save_progress(data, filename):
    with open(filename, 'w') as file:
        json.dump(data, file, indent=4)  # Indents the JSON data with 4 spaces per level

def load_progress(filename):
    try:
        with open(filename, 'r') as file:
            return json.load(file)
    except FileNotFoundError:
        return {}

def main():
    progress_file = f'upload_progress_{token_id}.json'
    progress_data = load_progress(progress_file)
    s3_bucket = "lost-ones-upload32737-staging"

    # Fetch new data
    nft_data = fetch_nfts_from_mirror_node()
    nft_data = fetch_ipfs_metadata(nft_data)

    # Update or add new items to progress data
    for item in nft_data:
        serial_number = str(item['serial_number'])  # Ensure serial_number is a string
        if serial_number in progress_data:
            print(f"Serial number {serial_number} already exists in progress file.")
        else:
            print(f"Adding new serial number {serial_number} to progress file.")
            progress_data[serial_number] = {
                'uploaded': False,
                'url': item['image']
            }

    save_progress(progress_data, progress_file)


    # Upload images and update progress
    for serial_number, item in progress_data.items():
        if item['uploaded']:
            print(f"Skipping already uploaded image for serial number {serial_number}")
            continue

        try:
            resized_image = resize_image(download_image(item['url'].replace('ipfs://', 'https://ipfs.io/ipfs/')))
            s3_object_name = f"{serial_number}.webp"
            upload_to_s3(resized_image, s3_bucket, s3_object_name)
            progress_data[serial_number]['uploaded'] = True
            print(f"Uploaded {s3_object_name}")
        except Exception as e:
            print(f"Error uploading image for serial number {serial_number}: {e}")
            progress_data[serial_number]['uploaded'] = False

        save_progress(progress_data, progress_file)

if __name__ == "__main__":
    while True:
        main()
        print("Script execution completed. Waiting for 30 minutes before next run.")
        time.sleep(300)  # Wait for 1800 seconds (30 minutes) before next execution

