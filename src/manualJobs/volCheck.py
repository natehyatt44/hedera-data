import pandas as pd
import src.s3helper

TOKEN_CFP = '0.0.2235264'
TOKEN_AIRDROP = '0.0.2371643'
TOKEN_LOSTONES = '0.0.3721853'

MARKET_ZUSE = '0.0.690356'
MARKET_SENTX = '0.0.1064038'

def main():
    df = src.s3helper.read_df_s3(TOKEN_AIRDROP, 'nft_transactions.csv')

    # Calculate total volume by market_name
    volume_by_market = df.groupby('market_name')['amount'].sum()

    # Print the total volume by market
    for market, volume in volume_by_market.items():
        print(f"Total volume for {market}: {volume}")

    # Calculate the overall total volume
    overall_volume = df['amount'].sum()
    print(f"Overall total volume: {overall_volume}")

if __name__ == "__main__":
    main()
