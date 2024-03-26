import src.pipelineNftListing as pipelineNftListing
import src.pipelineNftListingSentX as pipelineNftListingSentX
import src.pipelineNftSales as pipelineNftSales
import time
import traceback

TOKEN_CFP = '0.0.2235264'
TOKEN_AIRDROP = '0.0.2371643'
TOKEN_LOSTONES = '0.0.3721853'
TOKEN_ARTHOUSE_CR = '0.0.5102420'
TOKEN_TOOLS = '0.0.4849512'

MARKET_ZUSE = '0.0.690356'
MARKET_SENTX = '0.0.1064038'


def main():
    token_ids = [TOKEN_CFP, TOKEN_AIRDROP, TOKEN_LOSTONES, TOKEN_ARTHOUSE_TRIZTAZZ, TOKEN_TOOLS]
    market_ids = [MARKET_ZUSE, MARKET_SENTX]
    for token_id in token_ids:
        pipelineNftListing.execute(token_id)
        pipelineNftListingSentX.execute(token_id)

    pipelineNftSales.execute(token_ids, market_ids)


def run_main_with_retry():
    while True:
        try:
            main()
            time.sleep(600)  # Sleeps for 600 seconds, which is 10 minutes
        except Exception as e:
            print(f"An error occurred: {e}")
            print("Retrying in 5 minutes...")
            traceback.print_exc()  # prints detailed traceback
            time.sleep(300)  # Sleeps for 300 seconds, which is 5 minutes


if __name__ == "__main__":
    run_main_with_retry()
