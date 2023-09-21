import src.pipelineNftListing as pipelineNftListing
import src.pipelineNftListingSentX as pipelineNftListingSentX
import src.pipelineNftSales as pipelineNftSales
import time

TOKEN_CFP = '0.0.2235264'
TOKEN_AIRDROP = '0.0.2371643'
TOKEN_LOSTONES = '0.0.3721853'

MARKET_ZUSE = '0.0.690356'
MARKET_SENTX = '0.0.1064038'


def main():
    token_ids = [TOKEN_CFP, TOKEN_AIRDROP, TOKEN_LOSTONES]
    market_ids = [MARKET_ZUSE, MARKET_SENTX]
    for token_id in token_ids:
        pipelineNftListing.execute(token_id)
        pipelineNftListingSentX.execute(token_id)

    pipelineNftSales.execute(token_ids, market_ids)

while True:
    main()
    time.sleep(600)  # Sleeps for 600 seconds, which is 10 minutes

if __name__ == "__main__":
    main()




