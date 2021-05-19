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
import pandasql as psql
import numpy as np
import math

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

    def getNumberOfMaps(self, _source):
        return self.getMaps(-1, _source)[2]
    
    def getMaps(self, _index, _source):
        indexAddress = 0
        
        nameAddress = None
        address = None
        
        if(_source == 'DEX'): ################################################# start dex
            
            dexes = pd.read_csv('data/dex.csv')
            for indexAddress in range(0, len(dexes)):
                if(_index == indexAddress):     
                    address = dexes['Address'].values[_index]
                    nameAddress = dexes['NameAddress'].values[_index]
        
        if(_source == 'TOKEN'): ################################################# start token
            
            tokens = pd.read_csv('data/token.csv')
            for indexAddress in range(0, len(validTokens)):
                if(_index == indexAddress):     
                    address = validTokens['Address'].values[_index]
                    nameAddress = validTokens['NameAddress'].values[_index].replace(".","").replace("$","").replace("+","")
        
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
    def importTransactionListFromAddress(self, _etherscanApiKey, _provider, _action, _source, _address, _blockInterval, _blockStart, _blockLimit, _exportFileName):
        df = pd.DataFrame()
        START_BLOCK = '0'
        END_BLOCK = '9999999999999999'
        
        r = requests.get(self.API_String(_provider, _action, [_address, START_BLOCK, END_BLOCK, _etherscanApiKey]))
        tx = r.json()['result']
            
        if(len(tx) > 0):
            
            cols = tx[0].keys()
            df = pd.DataFrame(columns = cols)
            lastBlock = int(tx[0]['blockNumber'])
            if(_blockInterval == 'MAX'):
                
                TRANSACTION_LIMIT = 100
                if(len(tx) < TRANSACTION_LIMIT):
                    TRANSACTION_LIMIT = len(tx) - 1

                blockInterval = abs(int(tx[0]['blockNumber']) - int(tx[TRANSACTION_LIMIT]['blockNumber']))
            else:
                blockInterval = _blockInterval # Uniswap API is limited to 10000 transictions data each time, max 2 request / s on free api account
            indexBlock = _blockStart
            blockStart = _blockStart
            blockLimit = _blockLimit

            while (blockLimit > indexBlock):

                endBlock = lastBlock - blockInterval * (indexBlock - 1)
                startBlock = endBlock - blockInterval

                try:
                    r = requests.get(self.API_String(_provider, _action, [_address, startBlock, endBlock, _etherscanApiKey]))
                    tx = r.json()['result']
                    #print(tx)
                    #print(len(tx))
                    tmp_df = pd.DataFrame.from_dict(tx).filter(items = cols)
                    if(len(tmp_df) > 0 and len(tmp_df) < 10000):

                        df = pd.concat([df, tmp_df])
                        print("Index Block Interval n. {0} - Imported {1} transactions - DateTime: {2}".format(indexBlock, len(tmp_df), datetime.now().strftime("%d/%m/%Y_%H:%M:%S")))
                    else: #restart
                        
                        df = pd.DataFrame(columns = cols)
                        indexBlock = _blockStart
                        blockInterval = math.ceil(blockInterval / 2)
                        print("Maximim number of transaction. Restarting importing process. Block Interval set to {0}".format(blockInterval))
                except:
                    r = None
                    print("Error on request.")

                indexBlock = indexBlock + 1
            
        return df
    
    def API_String(self, _provider, _action, _data):
        apiString = None
        if(_provider == 'ETHERSCAN'):
            if(_action == 'TXLIST'):
                apiString = 'https://api.etherscan.io/api?module=account&action=txlist&address={0}&startblock={1}&endblock={2}&sort=desc&apikey={3}'.format(_data[0], _data[1], _data[2], _data[3])
                
        if(_provider == 'BSCSCAN'):
            if(_action == 'TXLIST'):
                apiString = 'https://api.bscscan.com/api?module=account&action=txlist&address={0}&startblock={1}&endblock={2}&sort=desc&apikey={3}'.format(_data[0], _data[1], _data[2], _data[3])
                
        return apiString
        
    def importTransactionsFromDexCustomers(self, _etherscanApiKey, _provider, _action, _source): #directFunction
        
        df = pd.DataFrame()
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        EXPORT_FOLDER = 'export'
        
        export_files = [f for f in listdir(EXPORT_FOLDER) if isfile(join(EXPORT_FOLDER, f))]
        
        NUMBER_OF_MAPPED_ADDRESS = self.getNumberOfMaps(_source)
        
        dfCustomers = pd.DataFrame(columns = ['from'])
        
        for indexMap in range(0,NUMBER_OF_MAPPED_ADDRESS):
            
            addressName = self.getMaps(indexMap, _source)[0]
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
        
        dfCustomers.to_csv('export/CUSTOMERS_LIST_FROM_DEX_{0}.csv'.format(dt))
        
        foundAddresses = len(dfCustomers)
        
        print("Found {0} addresses".format(foundAddresses))
            
        indexImport = 0

        df = dfCustomers
        
        cols = ['fromAddress', 'blockNumber', 'timeStamp', 'from', 'to', 'value', 'gas', 'gasPrice']
        dfCustomers = pd.DataFrame(columns = cols) #reset dfCustomers

        for fromAddress in df['from']:

            START_BLOCK = '0'
            END_BLOCK = '9999999999999999'

            try:

                r = requests.get(self.API_String(_provider, _action, [fromAddress, START_BLOCK, END_BLOCK, _etherscanApiKey]))
                tx = r.json()['result']
                if(len(tx) > 0):
                    tmp_df = pd.DataFrame.from_dict(tx).filter(items = cols)
                    tmp_df['fromAddress'] = fromAddress

                    dfCustomers = pd.concat([dfCustomers,tmp_df])

                    indexImport = indexImport + 1

                    print("Index Import: {0} - Imported {1} transactions - Datetime: {2} - Cumulative dimension: {3}".format(indexImport, len(tmp_df), datetime.now().strftime("%Y%m%d_%H%M%S"), len(dfCustomers)))

            except OSError as err:
                print("OS error: {0}".format(err))
            except ValueError:
                print("Could not convert data to an integer.")
            except:
                print("Unexpected error:", sys.exc_info()[0])
                raise

        if(len(dfCustomers) > 0):
            dfCustomers.to_csv('export/CUSTOMERS_TRANSACTIONS_FROM_DEX_{1}.csv'.format(fromAddress, dt))
        
    def importAllDexTransactions(self, _etherscanApiKey, _provider, _action, _source, _blockInterval, _blockStart, _blockLimit): #only DEXes
        
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        
        for indexMap in range(0,self.getNumberOfMaps(_source)):
            
            address = self.getMaps(indexMap, _source)[1]
            addressName = self.getMaps(indexMap, _source)[0]
            df = self.importTransactionListFromAddress(_etherscanApiKey, _provider, _action, _source, address, _blockInterval, _blockStart, _blockLimit, address)
            if(len(df) > 0):
                print("Dataset of {0}'s lenght is: {1}".format(addressName, len(df)))
                df.to_csv("export/TRANSACTION_LIST_{0}_{1}.csv".format(addressName, dt))
            
    def reportDexCustomers(self, _source):
        
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")
        EXPORT_FOLDER = 'export/{0}'.format(_source)
        export_files = [f for f in listdir(EXPORT_FOLDER) if isfile(join(EXPORT_FOLDER, f))]
        
        for indexFile in export_files:
            fileMatch = 'CUSTOMERS_TRANSACTIONS_FROM'
            if(indexFile[0:len(fileMatch)] == fileMatch):
                lastTxDex = indexFile
        
        for indexFile in export_files:
            fileMatch = 'CUSTOMERS_LIST_FROM'
            if(indexFile[0:len(fileMatch)] == fileMatch):
                lastCList = indexFile
                
        print(lastTxDex)
        print(lastCList)
        transactionsFromUsersDex = pd.read_csv('{0}/{1}'.format(EXPORT_FOLDER, lastTxDex))
        print("Opening file {0}/{1}".format(EXPORT_FOLDER, lastTxDex))
        transactionsFromUsersDex.set_index(transactionsFromUsersDex['from'])
        
        transactionsFromUsersDex.rename(columns={'to':'toAddress'}, inplace=True)
        transactionsFromUsersDex.rename(columns={'from':'fromAddress_'}, inplace=True)
        transactionsFromUsersDex['fromAddress'] = transactionsFromUsersDex['fromAddress'].str.upper()
        transactionsFromUsersDex['fromAddress_'] = transactionsFromUsersDex['fromAddress_'].str.upper()
        transactionsFromUsersDex['toAddress'] = transactionsFromUsersDex['toAddress'].str.upper()

        address = []
        name = []
        
        for index in range(0, self.getNumberOfMaps(_source)):
            dex = self.getMaps(index, _source)
            address.append(dex[1].upper())
            name.append(dex[0].upper())
            dfDex = pd.DataFrame({'DEX_Address' : address, 'DEX_Name' : name})
            dfDex.to_csv(r'DEX.csv')

        base = '{0}'.format(10 ** 18) # = 10 ^ 18
        sql = 'Select a.*, CASE WHEN fromAddress = fromAddress_ then "OUT" else "IN" END AS IN_OUT from transactionsFromUsersDex a'
        df = psql.sqldf(sql)
        dexList = []
        caseWhenStatement_IN = []
        caseWhenStatement_OUT = []
        sumCaseWhenStatement = []
            
        for dexIndex in range(0, self.getNumberOfMaps(_source)):
            name = self.getMaps(dexIndex, _source)[0]
            address = self.getMaps(dexIndex, _source)[1]
            df[name + '_IN'] = np.where(df['fromAddress'] == address.upper(), True, False)
            dexList.append(name + '_IN')
            df[name + '_OUT'] = np.where(df['toAddress'] == address.upper(), True, False)
            dexList.append(name + '_OUT')
            caseWhenStatement_IN.append("WHEN {0} = True THEN 'SUM_{0}'".format(name + '_IN'))
            caseWhenStatement_OUT.append("WHEN {0} = True THEN 'SUM_{0}'".format(name + '_OUT'))
            sumCaseWhenStatement.append("SUM(CASE WHEN {0} = True THEN 1 ELSE 0 END) AS CNT_{0}, SUM(CASE WHEN {0} = True THEN value / {2} ELSE 0 END) AS SUM_{0}_ETH, SUM(CASE WHEN {1} = True THEN 1 ELSE 0 END) AS CNT_{1}, SUM(CASE WHEN {1} = True THEN value / {2} ELSE 0 END) AS SUM_{1}_ETH".format(name + '_OUT', name + '_IN', base))
        dexList = ", ".join(dexList)

        caseWhenStatement_IN = " ".join(caseWhenStatement_IN)
        caseWhenStatement_OUT = " ".join(caseWhenStatement_OUT)
        sumCaseWhenStatement = ", ".join(sumCaseWhenStatement)

        sql = 'With toDexTransactions As ' \
              '(Select a.* ' \
              'From df a inner join dfDex b ' \
              'on (a.toAddress = b.DEX_Address or a.fromAddress = b.DEX_Address)) ' \
              'Select fromAddress, count(*) as CNT_{2}_TX, ' \
              'SUM(CASE WHEN IN_OUT = "OUT" THEN value / {1} END) as TOTAL_{2}_OUT_ETH, ' \
              'MIN(blockNumber) as MIN_BLOCK_ANALYZED, ' \
              'MAX(blockNumber) as MAX_BLOCK_ANALYZED, ' \
              '(MAX(blockNumber)-MIN(blockNumber)) / count(*) as FREQ_CNT_{2}_TX, ' \
              'MAX(CASE WHEN IN_OUT = "OUT" THEN value / {1} END) as MAX_{2}_OUT_ETH, ' \
              '{0} ' \
              'From toDexTransactions ' \
              'Group by fromAddress ' \
              'Order By TOTAL_{2}_OUT_ETH desc'.format(sumCaseWhenStatement, base, _source)

        result = psql.sqldf(sql)

        q = [.5, .65, .8, .9, .95, .995]
        totalDex_OUT_ETH_q = np.quantile(result['TOTAL_{0}_OUT_ETH'.format(_source)] , q, axis = 0)
        plot_df = pd.DataFrame(totalDex_OUT_ETH_q, columns = ['TOTAL_{0}_OUT_ETH_Q'.format(_source)])
        plot_df['QUANTILE'] = pd.Series(q)
        sql = "Select *, " \
              "CASE " \
              "WHEN TOTAL_{6}_OUT_ETH >= 0 AND TOTAL_{6}_OUT_ETH <= {0} THEN 'MINIMAL' " \
              "WHEN TOTAL_{6}_OUT_ETH > {0} AND TOTAL_{6}_OUT_ETH <= {1} THEN 'LOW' " \
              "WHEN TOTAL_{6}_OUT_ETH > {1} AND TOTAL_{6}_OUT_ETH <= {2} THEN 'BRONZE' " \
              "WHEN TOTAL_{6}_OUT_ETH > {2} AND TOTAL_{6}_OUT_ETH <= {3} THEN 'SILVER' " \
              "WHEN TOTAL_{6}_OUT_ETH > {3} AND TOTAL_{6}_OUT_ETH <= {4} THEN 'GOLD' " \
              "WHEN TOTAL_{6}_OUT_ETH > {4} AND TOTAL_{6}_OUT_ETH <= {5} THEN 'PLATINUM' " \
              "WHEN TOTAL_{6}_OUT_ETH > {5} THEN 'PRIVILEGE' " \
              "END AS ADDRESS_CATEGORY " \
              "FROM result".format(totalDex_OUT_ETH_q[0], totalDex_OUT_ETH_q[1], totalDex_OUT_ETH_q[2], totalDex_OUT_ETH_q[3], totalDex_OUT_ETH_q[4], totalDex_OUT_ETH_q[5], _source)
        result = psql.sqldf(sql)
        result.to_csv(r"export/CUSTOMER_{0}_ANALYSIS_{1}.csv".format(_source, dt))
        print("File saved at export/CUSTOMER_{0}_ANALYSIS_{1}.csv".format(_source, dt))
        
    def mergeFiles(self, _source):
        
        EXPORT_FOLDER = 'export'
        
        if(path.exists('{0}/{1}'.format(EXPORT_FOLDER, _source)) == False):
            os.mkdir('{0}/{1}'.format(EXPORT_FOLDER, _source))
        
        SAVE_FOLDER = '{0}/{1}'.format(EXPORT_FOLDER, _source)
        dt = datetime.now().strftime("%Y%m%d_%H%M%S")

        export_files = [f for f in listdir(EXPORT_FOLDER) if isfile(join(EXPORT_FOLDER, f))]
        
        fileList = ['TRANSACTION_LIST', 'CUSTOMERS_LIST_FROM', 'CUSTOMERS_TRANSACTIONS_FROM']
        
        for indexFileList in fileList:
            checkedColumns = False
            for indexFile in export_files:
                if(indexFile[0:len(indexFileList)] == indexFileList):
                    if(checkedColumns):
                        df = pd.read_csv('{0}/{1}'.format(EXPORT_FOLDER, indexFile))
                        df_result = pd.concat([df_result, df])
                        os.remove('{0}/{1}'.format(EXPORT_FOLDER, indexFile))
                    else:
                        checkedColumns = True
                        df_result = pd.read_csv('{0}/{1}'.format(EXPORT_FOLDER, indexFile))
                        os.remove('{0}/{1}'.format(EXPORT_FOLDER, indexFile))
            try:
                df_result.drop_duplicates()
                df_result.to_csv('{0}/{1}_{2}_{2}.csv'.format(SAVE_FOLDER, indexFileList ,'MERGED', dt))
                print('File Merged at {0}/{1}_{2}_{2}.csv'.format(SAVE_FOLDER, indexFileList ,'MERGED', dt))
            except:
                print("No File Detected with prefix {0}".format(indexFileList))

    def analyzeDexCustomers(self, _apiKey):
        self.importAllDexTransactions(_apiKey, 'ETHERSCAN', 'TXLIST', 'DEX', 'MAX', 0, 10) # 24 hours
        self.importTransactionsFromDexCustomers(_apiKey, 'ETHERSCAN', 'TXLIST', 'DEX')
        self.mergeFiles('DEX')
        self.reportDexCustomers('DEX')
        
    def analyzeTokenCustomers(self, _apiKey):
        self.importAllDexTransactions(_apiKey, 'BSCSCAN', 'TXLIST', 'TOKEN', 'MAX', 0, 10)
        self.importTransactionsFromDexCustomers(_apiKey, 'BSCSCAN', 'TXLIST', 'TOKEN')
        self.mergeFiles('TOKEN')
        self.reportDexCustomers('TOKEN')