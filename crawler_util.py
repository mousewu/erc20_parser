"""Util functions for interacting with geth and mongo."""
import pymongo
from collections import deque
import os
import pdb

DB_NAME = "parity"
#COLLECTION = "TrueUSD"


def initMongo(client, collection):
    """
    Given a mongo client instance, create db/collection if either doesn't exist

    Parameters:
    -----------
    client <mongodb Client>

    Returns:
    --------
    <mongodb Client>
    """
    db = client[DB_NAME]
    try:
        db.create_collection(collection)
    except:
        pass
    try:
        # Index the block number so duplicate records cannot be made
        db[collection].create_index([("blockNumber", pymongo.DESCENDING)])
    except:
        pass

    return db[collection]


def insertMongo(client, d):
    """
    Insert a document into mongo client with collection selected.

    Params:
    -------
    client <mongodb Client>
    d <dict>

    Returns:
    --------
    error <None or str>
    """
    try:
        client.insert_many(d, ordered=False)
        return None
    except Exception as err:
        pass


def highestBlock(client):
    """
    Get the highest numbered block in the collection.

    Params:
    -------
    client <mongodb Client>

    Returns:
    --------
    <int>
    """
    n = client.find_one(sort=[("blockNumber", pymongo.DESCENDING)])
    if not n:
        # If the database is empty, the highest block # is 0
        return 0
    assert "blockNumber" in n, "Highest block is incorrectly formatted"
    return n["blockNumber"]


def decodeBlock(block):
    b = block
    if "result" in block:
        b = block["result"]
    transactions = []
    timestamp = int(b['timestamp'], 16)
    for t in b["transactions"]:
        try:
            new_t = {
                "hash": t.get("hash"),
                "nonce": t.get("nonce"),
                "blockHash": t.get('blockHash'),
                "blockNumber": int(t.get('blockNumber'), 16),
                "timestamp": timestamp,
                "transactionIndex": int(t.get('transactionIndex'),16),
                "from": t.get("from"),
                "to": t.get("to"),
                "value": float(int(t.get("value"), 16))/1000000000000000000.,
                "gas":t.get("gas"),
                "gasPrice":t.get("gasPrice"),
                "data": t.get("input"),
                "v":t.get("v"),
                "r":t.get("r"),
                "s":t.get("s")
            }
            transactions.append(new_t)
        except Exception as e:
            print(e)
            return None
    return transactions




