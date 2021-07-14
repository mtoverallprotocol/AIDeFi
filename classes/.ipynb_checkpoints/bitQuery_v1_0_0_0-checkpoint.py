# APIKEY: BQYvYOBEZWH2ymkvKTC3tbDRgprmFeOY -> for test only
import requests
import re
import pandas as pd
from graphene import ObjectType, String, Schema

class bitQuery:
    
    def ___init__(self, name, tables):
        self.name = name
    
# -*- coding: utf-8 -*-

    def __run_query(self, query, _bitQueryApiKey):  # A simple function to use requests.post to make the API call.
        headers = {'X-API-KEY': _bitQueryApiKey}
        request = requests.post('https://graphql.bitquery.io/',
                                json={'query': query}, headers=headers)
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception('Query failed and return code is {}.      {}'.format(request.status_code,
                            query))        
    
    def __getDataframeFromQuery(self, _queryResult):
        listResult = _queryResult['data']['ethereum']['smartContractEvents']
        import pandas as pd
        isFirst = True
        for indexList in range(0, len(listResult)):
            blockNumber = listResult[indexList]['block']['height']
            timestamp_iso8601 = listResult[indexList]['block']['timestamp']['iso8601']
            timestamp_unixtime = listResult[indexList]['block']['timestamp']['unixtime']
            arguments = listResult[indexList]['arguments']
            tmp_df = pd.DataFrame()
            tmp_df['blockNumber'] = [blockNumber]
            tmp_df['timestamp_iso8601'] = [timestamp_iso8601]
            tmp_df['timestamp_unixtime'] = [timestamp_unixtime]

            for indexArgs in range(0, len(arguments)):
                tmp_df[arguments[indexArgs]['argument']] = [arguments[indexArgs]['value']]

            if(isFirst):
                df = tmp_df
                isFirst = False
            else:
                df = pd.concat([df, tmp_df])
                
        return df
    
    # The GraphQL query
    
    def directQuery(self, _query, _bitQueryApiKey, _isRaw = False):
        query = self.__run_query(_query, _bitQueryApiKey)  # Execute the query
        if(_isRaw):
            return query
        else:
            return self.__getDataframeFromQuery(query)
    
    

class query(ObjectType):
        
    def ___init__(self, name, tables):
        self.name = name
        
    getDexTradeByProtocol = String(
        count = String(),
        protocol = String()
    )
    
    def resolve_getDexTradeByProtocol(root, info):
        return '{}'
    
    def dexTradeByProtocol(self):
        query = """
                {
          ethereum {
            dexTrades(options: {limit: 100, desc: "count"}) {
              count
              protocol
            }
          }
        }
        """
        return query
    
    def getEvents(self, _address, _eventName, _limit):
        _address = '"{}"'.format(_address)
        _eventName = '"{}"'.format(_eventName)
        _limit = '{}'.format(_limit)
        query = """
                {
              ethereum {
                smartContractEvents(options: {desc: "block.height", limit: """+_limit+"""},
                  smartContractEvent: {is: """+_eventName+"""},
                  smartContractAddress: 
                  {is: """+_address+"""}) {
                  block {
                    height
                    timestamp {
                      iso8601
                      unixtime
                    }
                  }
                  arguments {
                    value
                    argument
                  }
                }
              }
            }
        """

        return query