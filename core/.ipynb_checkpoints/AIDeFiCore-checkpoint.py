############## List of dependencies ##############

from web3 import Web3
import pandas as pd
import numpy as np
import pandasql as ps
import string
import re

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
        return list(w3.eth.get_block('latest').keys())
    
    def getEmptyBlockDataframe(self, _w3):
        return pd.DataFrame(columns = getBlockArgs, _w3)
    
    def addBlockOnDataFrame(self, _block, _df, _columns):
        _df.loc[_df.shape[0]+1] = None
        for key in _columns:
            if is_hex(_block[key]):
                _df[key][_df.shape[0]] = str(_block[key].hex())
            else:
                _df[key][_df.shape[0]] = str(_block[key])
                
    def importBlockInterval(self, _w3, _df, _columns, _min, _max):
        for indexBlock in range(_min, _max):
            addBlockOnDataFrame(_w3.eth.get_block(indexBlock), _df, _columns)
            
    def is_hex(self, _arg):
        try:
            _arg.hex()
            return True
        except:
            return False