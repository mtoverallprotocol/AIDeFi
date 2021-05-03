import re
from web3 import Web3
import pandas as pd
import os
import requests
from datetime import datetime

class ethData:
    
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
            
    def importBlockList(self, _w3, _listBlock, _fullImport):
        if(_fullImport):
            transactionArgs = self.getTransactionArgs(_w3)
            df_transaction = pd.DataFrame(columns = transactionArgs)
            transactionReceiptArgs = self.getTransactionReceiptArgs(_w3)
            df_transactionReceipt = pd.DataFrame(columns = transactionReceiptArgs)
        
        blockArgs = self.getBlockArgs(_w3)
        df_block = pd.DataFrame(columns = blockArgs)
        
        for indexBlock in _listBlock:
            block = _w3.eth.get_block(indexBlock)
            #Importing block data
            self.addBlockOnDataFrame(block, df_block, blockArgs)
            if(_fullImport):
                #Importing transaction and transaction receipt data
                self.importTransactionList(_w3, block, blockArgs, df_transaction, transactionArgs, df_transactionReceipt, transactionReceiptArgs)  
        
        if(_fullImport):
            return (df_block, df_transaction, df_transactionReceipt)
        else:
            return df_block

    
    def is_hex(self, _arg):
        try:
            _arg.hex()
            return True
        except:
            return False
        
        #Import transaction list from address
    def importTransactionListFromAddress(self, _etherscanApiKey, _address, _blockInterval, _blockStart, _blockLimit, _exportFilename):
        ETHERSCAN_API_STRING = 'https://api.etherscan.io/api?module=account&action=txlist&address={0}&startblock={1}&endblock={2}&sort=desc&apikey={3}'
        START_BLOCK = '0'
        END_BLOCK = '9999999999999999'
        if(os.path.isdir('export/TRANSACTION_LIST_{0}'.format(_exportFilename)) == False):
            os.mkdir('export/TRANSACTION_LIST_{0}'.format(_exportFilename))
        r = requests.get(ETHERSCAN_API_STRING.format(_address, START_BLOCK, END_BLOCK, _etherscanApiKey))
        uniswapTransactions = r.json()['result']
        cols = uniswapTransactions[0].keys()
        df = pd.DataFrame(columns = cols)
        lastBlock = int(uniswapTransactions[0]['blockNumber'])
        blockInterval = _blockInterval # Uniswap API is limited to 10000 transictions data each time, max 2 request / s on free api account
        indexBlock = _blockStart
        blockStart = _blockStart
        blockLimit = _blockLimit
        while True:
            if(indexBlock < blockLimit):
                endBlock = lastBlock - blockInterval * (indexBlock - 1)
                startBlock = endBlock - blockInterval
                r = requests.get(ETHERSCAN_API_STRING.format(_address, startBlock, endBlock, _etherscanApiKey))
                uniswapTransactions = r.json()
                if(len(uniswapTransactions['result']) > 0):
                    tmp_df = pd.DataFrame.from_dict(uniswapTransactions).filter(items = cols)
                    print("Index Block Interval n. {0} - Imported {1} items - DateTime: {2}".format(indexBlock, len(tmp_df), datetime.now().strftime("%d/%m/%Y %H:%M:%S")))
                    df = pd.concat([df, tmp_df])
            if(len(tmp_df) == 0 or blockLimit < indexBlock):
                break
            indexBlock = indexBlock + 1
        df.to_csv(r'export/TRANSACTION_LIST_{0}/{0}_{1}_{2}.csv'.format(_exportFilename, blockStart, blockLimit))
        print('#############################################')
        print('Dataframe saved on csv file at export/TRANSACTION_LIST_{0}/{0}_{1}_{2}_{3}.csv'.format(_exportFilename, blockStart, blockLimit, datetime.now().strftime("%Y%m%d_%H%M%S")))