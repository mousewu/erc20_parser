"""A client to interact with node and to save data to mongo."""

from pymongo import MongoClient
import crawler_util
import requests
import json
import sys
import os
import logging
import time
import tqdm
from datetime import datetime
import math
sys.path.append(os.path.realpath(os.path.dirname(__file__)))

#DIR = os.environ['BLOCKCHAIN_MONGO_DATA_DIR']
LOGFIL = "/data1/workdir/wuxianyue/erc20parser/erc20parser.log"

logging.basicConfig(filename=LOGFIL, level=logging.DEBUG)
logging.getLogger("urllib3").setLevel(logging.WARNING)


class erc20parser(object):
    """

    """

    def __init__(
        self,
        address,
        collection,
        decimals,
        start=True,
        rpc_port=8545,
        host="http://localhost",
        #rpc_port=18545,
        #host='http://47.97.213.228',
        delay=0.0001
        ):
        """Initialize the Crawler."""
        logging.debug("Starting erc20parser")
        self.url = "{}:{}".format(host, rpc_port)
        self.headers = {"content-type": "application/json"}
        self.collection = collection
        self.decimals = decimals

        # Initializes to default host/port = localhost/27017
        self.mongo_client = crawler_util.initMongo(MongoClient('mongodb://longhashdba:' + 'longhash123QAZ' + '@localhost/parity'),self.collection)
        #self.mongo_client = crawler_util.initMongo(MongoClient('localhost', 27017))
        # The max block number that is in mongo
        self.max_block_mongo = None
        # The max block number in the public blockchain
        self.max_block_geth = None
        # Record errors for inserting block data into mongo
        self.insertion_errors = list()
        # Make a stack of block numbers that are in mongo
        # The delay between requests to geth
        self.delay = delay
        self.base = 100
        self.address = address
        self.topic = "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef"

        if start:
            self.max_block_mongo = self.highestBlockMongo()
            #self.max_block_mongo = 0
            self.max_block_geth = self.highestBlockEth()
            self.run()

    def _rpcRequest(self, method, params, key):
        """Make an RPC request to geth on port 8545."""
        payload = {
            "method": method,
            "params": params,
            "jsonrpc": "2.0",
            "id": 0
        }
        time.sleep(self.delay)
        res = requests.post(
              self.url,
              data=json.dumps(payload),
              headers=self.headers).json()
        # print(res)
        return res[key]

    def eth_newFilter(self, fromblock,toblock,address,topic):
        data = self._rpcRequest("eth_newFilter",[{"fromBlock":fromblock,"toBlock":toblock,"address":address,"topics":[topic]}], "result")
        return data
    
    def eth_getFilterLogs(self, filter_id):
        data = self._rpcRequest("eth_getFilterLogs",[filter_id], "result")
        return data

    def highestBlockEth(self):
        """Find the highest numbered block in geth."""
        num_hex = self._rpcRequest("eth_blockNumber", [], "result")
        return int(num_hex, 16)

    def saveBlock(self, tx):
        """Insert a given parsed block into mongo."""
        e = crawler_util.insertMongo(self.mongo_client, tx)
        if e:
            print(e)
            self.insertion_errors.append(e)

    def highestBlockMongo(self):
        """Find the highest numbered block in the mongo database."""
        highest_block = crawler_util.highestBlock(self.mongo_client)
        logging.info("Highest block found in mongodb:{}".format(highest_block))
        return highest_block
    
    def getTimestamp(self, n):
        data = self._rpcRequest("eth_getBlockByNumber", [hex(n), True], "result")
        timestamp = int(data['timestamp'], 16)
        time = datetime.utcfromtimestamp(timestamp).strftime('%Y-%m-%d %H:%M:%S')
        return time

    def parse_tx(self, n):
        """Add a block to mongo."""
        fromblock = hex(n*self.base+1)
        toblock = hex((n+1)*self.base)
        filter_id = self.eth_newFilter(fromblock,toblock,self.address,self.topic)
        txs = self.eth_getFilterLogs(filter_id)
        if txs:
            tx_list = []
            for tx in txs:
                new_tx = {}
                new_tx['blockNumber'] = int(tx['blockNumber'],16)
                new_tx['value'] = float(int(tx['data'],16))/float(math.pow(10.0,self.decimals))
                new_tx['from'] = '0x'+tx['topics'][1][26:]
                new_tx['to'] = '0x'+tx['topics'][2][26:]
                new_tx['transactionHash'] = tx['transactionHash']
                tx_list.append(new_tx)
            #print(tx_list)
            self.saveBlock(tx_list)
        else:
            pass

    def run(self):
        """
        Run the process.

        Iterate through the blockchain on geth and fill up mongodb
        with block data.
        """
        logging.debug("Processing geth blockchain:")
        logging.info("Highest block found as: {}".format(self.max_block_geth))


        # Get all new blocks
        print("Processing remainder of the blockchain...")
        print(self.collection," latest block on ethereum:",self.max_block_geth)
        for n in tqdm.tqdm(range(int(self.max_block_mongo/self.base), int(self.max_block_geth/self.base))):
            self.parse_tx(n)
        print("Done!\n")
        
if __name__ == "__main__":
    c = erc20parser(address = '0x0000000000085d4780B73119b644AE5ecd22b376', collection = 'TrueUSD',decimals = 18)
    c1 = erc20parser(address = '0xB8c77482e45F1F44dE1745F52C74426C631bDD52', collection = 'BNB', decimals = 18)
    c2 = erc20parser(address = '0x57ab1e02fee23774580c119740129eac7081e9d3', collection = 'nUSD', decimals = 18)
    c3 = erc20parser(address = '0x056fd409e1d7a124bd7017459dfea2f387b6d5cd', collection = 'GUSD', decimals = 2)
    c4 = erc20parser(address = '0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48', collection = 'USDC', decimals = 6)
    c5 = erc20parser(address = '0x8e870d67f660d95d5be530380d0ec0bd388289e1', collection = 'PAX', decimals = 18)
    c6 = erc20parser(address = '0xd26114cd6EE289AccF82350c8d8487fedB8A0C07', collection = 'OMG', decimals = 18)
    c7 = erc20parser(address = '0xdac17f958d2ee523a2206206994597c13d831ec7', collection = 'USDT', decimals = 6)
    c8 = erc20parser(address = '0x21ab6c9fac80c59d401b37cb43f81ea9dde7fe34', collection = 'BRC', decimals = 8)
    c9 = erc20parser(address = '0x8e1b448ec7adfc7fa35fc2e885678bd323176e34', collection = 'EGT', decimals = 18)
    c10 = erc20parser(address = '0x50d1c9771902476076ecfc8b2a83ad6b9355a4c9', collection = 'FTT', decimals = 18)
    c11 = erc20parser(address = '0x081f67afa0ccf8c7b17540767bbe95df2ba8d97f', collection = 'CET', decimals = 18)
    c12 = erc20parser(address= '0x931ad0628aa11791c26ff4d41ce23e40c31c5e4e', collection='PGS', decimals=8)
    
    
