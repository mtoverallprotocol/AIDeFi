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
            if utilities.is_hex(_block[key]):
                _df[key][_df.shape[0]] = str(_block[key].hex())
            else:
                _df[key][_df.shape[0]] = str(_block[key])