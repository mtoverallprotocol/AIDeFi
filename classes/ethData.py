import re
from web3 import Web3
import pandas as pd
import os
import requests
from datetime import datetime
from os import listdir
from os.path import isfile, join
import os.path
from os import path
from datetime import datetime
import sys

class ethData:
    
    def ___init__(self, name):
        self.name = name
        
    def linearReSearch(self, lys, element):
        for i in range (len(lys)):
            if re.search("({0})".format(element),lys[i]):
                return i
        return -1
    
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

    def getNumberContractAddressMap(self):
        return 7
    
    def getContractAddressMap(self, index):
        indexAddress = 0
        
        nameAddress = None
        address = None
        
        if(index == indexAddress):     #0
            address = '0x7a250d5630B4cF539739dF2C5dAcb4c659F2488D'
            nameAddress = 'UNISWAP'
        indexAddress = indexAddress + 1
        
        if(index == indexAddress):    #1
            address = '0x111111111117dC0aa78b770fA6A738034120C302'
            nameAddress = 'ONE_INCH'
        indexAddress = indexAddress + 1
        
        if(index == indexAddress):    #2
            address = '0xba100000625a3754423978a60c9317c58a424e3D'
            nameAddress = 'BALANCER'
        indexAddress = indexAddress + 1
        
        if(index == indexAddress):     #3
            address = '0x27054b13b1B798B345b591a4d22e6562d47eA75a'
            nameAddress = 'AIRSWAP'
        indexAddress = indexAddress + 1
        
        if(index == indexAddress):     #4
            address = '0x3f5CE5FBFe3E9af3971dD833D26bA9b5C936f0bE'
            nameAddress = 'BINANCE'
        indexAddress = indexAddress + 1
        
        if(index == indexAddress):    #5
            address = '0x3FDA67f7583380E67ef93072294a7fAc882FD7E7'
            nameAddress = 'COMPOUND'
        indexAddress = indexAddress + 1
        
        if(index == indexAddress):     #6
            address = '0xFf6b1cdfD2d3e37977d7938AA06b6d89D6675e27'
            nameAddress = 'ALLBIT2'
        indexAddress = indexAddress + 1
        
        if(index == indexAddress): #7 --> Change getNumberContractAddressMap to this new value
            address = '0xCb039d11Fd38167de19536453a105271A5e44392'
            nameAddress = 'PANCAKESWAP_FINANCE'
        indexAddress = indexAddress + 1
        
        # Add new map addresses after these addresses to avoid index change
        
        return nameAddress, address, indexAddress
    
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
    def importTransactionListFromAddress(self, _etherscanApiKey, _address, _blockInterval, _blockStart, _blockLimit, _exportFileName):
        ETHERSCAN_API_STRING = 'https://api.etherscan.io/api?module=account&action=txlist&address={0}&startblock={1}&endblock={2}&sort=desc&apikey={3}'
        START_BLOCK = '0'
        END_BLOCK = '9999999999999999'
        
        r = requests.get(ETHERSCAN_API_STRING.format(_address, START_BLOCK, END_BLOCK, _etherscanApiKey))
        tx = r.json()['result']
        cols = tx[0].keys()
        df = pd.DataFrame(columns = cols)
        
        lastBlock = int(tx[0]['blockNumber'])
        blockInterval = _blockInterval # Uniswap API is limited to 10000 transictions data each time, max 2 request / s on free api account
        indexBlock = _blockStart
        blockStart = _blockStart
        blockLimit = _blockLimit
        
        while (blockLimit > indexBlock):
            
            endBlock = lastBlock - blockInterval * (indexBlock - 1)
            startBlock = endBlock - blockInterval
            
            try:
                r = requests.get(ETHERSCAN_API_STRING.format(_address, startBlock, endBlock, _etherscanApiKey))
                tx = r.json()['result']
                tmp_df = pd.DataFrame.from_dict(tx).filter(items = cols)
                
                if(len(tmp_df) > 0):
                    
                    df = pd.concat([df, tmp_df])
                    print("Index Block Interval n. {0} - Imported {1} transactions - DateTime: {2}".format(indexBlock, len(tmp_df), datetime.now().strftime("%d/%m/%Y_%H:%M:%S")))
                    
            except:
                r = None
                print("Error on request.")
                
            indexBlock = indexBlock + 1
            
        return df
    
    def importTransactionsFromUsersDex(self, _etherscanApiKey):
        
        EXPORT_FOLDER = 'export'
        
        CUSTOMERS_TRANSACTIONS_FROM_DEX_FOLDER = 'export/CUSTOMERS_TRANSACTIONS_FROM_DEX'
        
        if(path.exists(CUSTOMERS_TRANSACTIONS_FROM_DEX_FOLDER) == False):
            os.mkdir(CUSTOMERS_TRANSACTIONS_FROM_DEX_FOLDER)
        export_files = [f for f in listdir(EXPORT_FOLDER) if isfile(join(EXPORT_FOLDER, f))]
        
        NUMBER_OF_MAPPED_ADDRESS = self.getNumberContractAddressMap() + 1
        
        dfCustomers = pd.DataFrame(columns = ['from'])
        
        for indexMap in range(0,NUMBER_OF_MAPPED_ADDRESS):
            
            addressName = self.getContractAddressMap(indexMap)[0]
            fileIndex = self.linearReSearch(export_files, "TRANSACTION_LIST_{0}".format(addressName))
            
            if(fileIndex >= 0):
                
                csv_transactionList = export_files[fileIndex]
                
                print("Importing transactions from {0}".format(csv_transactionList))
                
                df = pd.read_csv('{0}/{1}'.format(EXPORT_FOLDER, csv_transactionList))
                df = df.filter(items = ['from', 'value']) # Save cache memory space selecting the 2 usefuls columns
                df = pd.DataFrame(df[df['value'] != '0']['from'].unique(), columns = ['from']) # Save cache memory space deleting duplicates and 0 values
                dfCustomers = pd.concat([dfCustomers, df])
                
                print("Imported {0} address transactions".format(len(df)))
                
        dfCustomers = pd.DataFrame(dfCustomers['from'].unique(), columns = ['from']) #Save more memory and duplicates on final result
        
        dfCustomers.to_csv('export/CUSTOMERS_LIST_FROM_DEX.csv')
        
        foundAddresses = len(dfCustomers)
        
        print("Found {0} addresses".format(foundAddresses))
        
        export_files = [f for f in listdir(EXPORT_FOLDER) if isfile(join(EXPORT_FOLDER, f))] # update folders data
        
        fileIndex = self.linearReSearch(export_files, "CUSTOMERS_LIST_FROM_DEX")
        
        saved_data = [f for f in listdir(CUSTOMERS_TRANSACTIONS_FROM_DEX_FOLDER) if isfile(join(CUSTOMERS_TRANSACTIONS_FROM_DEX_FOLDER, f))]
        
        if(fileIndex >= 0):
            
            indexImport = 0
            
            csv_transactionListFromDex = export_files[fileIndex]
            df = pd.read_csv('{0}/{1}'.format(EXPORT_FOLDER, csv_transactionListFromDex))
            cols = ['fromAddress', 'blockNumber', 'timeStamp', 'from', 'to', 'value', 'gas', 'gasPrice']
            dfCustomers = pd.DataFrame(columns = cols)
            
            for fromAddress in df['from']:
                
                if(self.linearReSearch(saved_data, fromAddress) == -1 or len(saved_data) == 0): # Not found
                    
                    ETHERSCAN_API_STRING = 'https://api.etherscan.io/api?module=account&action=txlist&address={0}&startblock={1}&endblock={2}&sort=desc&apikey={3}'
                    START_BLOCK = '0'
                    END_BLOCK = '9999999999999999'
                    
                    try:
                        
                        r = requests.get(ETHERSCAN_API_STRING.format(fromAddress, START_BLOCK, END_BLOCK, _etherscanApiKey))
                        tx = r.json()['result']
                        tmp_df = pd.DataFrame.from_dict(tx).filter(items = cols)
                        tmp_df['fromAddress'] = fromAddress
                        #tmp_df = tmp_df[tmp_df['value'] != '0']
                        
                        dfCustomers = pd.concat([dfCustomers,tmp_df])
                        
                        indexImport = indexImport + 1
                        
                        print("Index Import: {0} - Imported {1} transactions - Datetime: {2} - Cumulative dimension: {3}".format(indexImport, len(tmp_df), datetime.now().strftime("%d/%m/%Y %H:%M:%S"), len(dfCustomers)))
                        
                    except OSError as err:
                        print("OS error: {0}".format(err))
                    except ValueError:
                        print("Could not convert data to an integer.")
                    except:
                        print("Unexpected error:", sys.exc_info()[0])
                        raise
                        
            if(len(dfCustomers) > 0):
                dfCustomers.to_csv('export/CUSTOMERS_TRANSACTIONS_FROM_DEX.csv'.format(fromAddress))
        
    def importAllDEX(self, _etherscanApiKey, _blockInterval, _blockStart, _blockLimit):
        
        for indexMap in range(0,self.getNumberContractAddressMap() + 1):
            
            address = self.getContractAddressMap(indexMap)[1]
            addressName = self.getContractAddressMap(indexMap)[0]
            df = self.importTransactionListFromAddress(_etherscanApiKey, address, _blockInterval, _blockStart, _blockLimit, address)
            
            print("Dataset of {0}'s lenght is: {1}".format(addressName, len(df)))
            
            df.to_csv("export/TRANSACTION_LIST_{0}_{1}.csv".format(addressName, datetime.now().strftime("%Y%m%d %H%M%S")))