############## List of dependencies ##############

from web3 import Web3
import pandas as pd
import numpy as np
import pandasql as ps
import string
import re
from multiprocessing import Process, Lock

############## Core Procedures ##############

class AIDeFiCore:
    
    def ___init__(self, name):
        self.name = name
    
    def blockchainConnection(self, _connectionString):
        if (re.search("(http(|s)\:\/\/)", _connectionString) != None):
            # HTTP Provider
            w3 = Web3(Web3.HTTPProvider(_connectionString))
        elif (re.search("(\.ipc)", _connectionString) != None):
            # IPC Provider
            w3 = Web3(Web3.IPCProvider(connectionString))
        elif (re.search("(ws\:\/\/)", _connectionString) != None):
            # Web Socket Provider
            w3 = Web3(Web3.WebsocketProvider(_connectionString))    
        else:
            w3 = None
        return w3
    
    def getBlockArgs(self, _w3):
        #return list(_w3.eth.get_block('latest', True).keys())
        return ['difficulty', 'extraData', 'gasLimit', 'gasUsed', 'hash', 'logsBloom', 'miner', 'mixHash', 'nonce', 'number', 'parentHash', 'receiptsRoot', 'sha3Uncles', 'size', 'stateRoot', 'timestamp', 'totalDifficulty', 'transactions', 'transactionsRoot', 'uncles']
    def getTransactionArgs(self, _w3):
        #return list(_w3.eth.get_block('latest', True)["transactions"][0].keys())
        return ['blockHash', 'blockNumber', 'from', 'gas', 'gasPrice', 'hash', 'input', 'nonce', 'r', 's', 'to', 'transactionIndex', 'type', 'v', 'value']

    def getTransactionReceiptArgs(self, _w3):
        #return list(_w3.eth.getTransactionReceipt(str(_w3.eth.get_block('latest', True)["transactions"][0]["hash"].hex())).keys())
        return ['blockHash', 'blockNumber', 'contractAddress', 'cumulativeGasUsed', 'from', 'gasUsed', 'logs', 'logsBloom', 'status', 'to', 'transactionHash', 'transactionIndex', 'type']
    
    def getLastBlockNumber(self, _w3):
        return _w3.eth.get_block('latest', True).number;
    
    def addBlockOnDataFrame(self, _block, _df, _columns):
        _df.loc[_df.shape[0]+1] = None
        for key in _columns:
            if self.is_hex(_block[key]):
                _df[key][_df.shape[0]] = str(_block[key].hex())
            else:
                _df[key][_df.shape[0]] = str(_block[key])
    
    def importTransactionList(self, _w3, _block, _blockArgs, _df_transaction, _transactionArgs, _df_transactionReceipt, _transactionReceiptArgs):
        transactions = _block["transactions"]
        for indexTransaction in range(0, len(transactions)):
            #Importing transactions on df_transaction
            transaction = _w3.eth.getTransaction(str(transactions[indexTransaction].hex()))
            self.addBlockOnDataFrame(transaction, _df_transaction, _transactionArgs)
            #Importing transaction receipts on df_transactionReceipt
            transactionReceipt = _w3.eth.getTransactionReceipt(str(transaction["hash"].hex()))
            self.addBlockOnDataFrame(transactionReceipt, _df_transactionReceipt, _transactionReceiptArgs)
            
    def importBlockInterval(self, _w3, _min, _max):
        blockArgs = self.getBlockArgs(_w3)
        transactionArgs = self.getTransactionArgs(_w3)
        transactionReceiptArgs = self.getTransactionReceiptArgs(_w3)
        df_block = pd.DataFrame(columns = blockArgs)
        df_transaction = pd.DataFrame(columns = transactionArgs)
        df_transactionReceipt = pd.DataFrame(columns = transactionReceiptArgs)
        for indexBlock in range(_min, _max):
            block = _w3.eth.get_block(indexBlock)
            #Importing block data
            self.addBlockOnDataFrame(block, df_block, blockArgs)
            #Importing transaction and transaction receipt data
            self.importTransactionList(_w3, block, blockArgs, df_transaction, transactionArgs, df_transactionReceipt, transactionReceiptArgs)  
        return (df_block, df_transaction, df_transactionReceipt)
            
    def is_hex(self, _arg):
        try:
            _arg.hex()
            return True
        except:
            return False
        
    