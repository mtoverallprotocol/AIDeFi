# APIKEY: BQYvYOBEZWH2ymkvKTC3tbDRgprmFeOY -> for test only
import requests
import re
import pandas as pd
from graphene import ObjectType, String, Schema
from past.builtins import execfile
from . import settings as settings
settings = settings.settings()
execfile(settings.loader('utilities'))
execfile(settings.loader('blockchain'))

frameworkInfo = settings.getFrameworkInfo()
CONNECTION_STRING = frameworkInfo['ConnectionString']['connectionString']
ETHERSCAN_APIKEY = frameworkInfo['APIKeys']['etherscan']
BSCSCAN_APIKEY = frameworkInfo['APIKeys']['bscscan']
BITQUERY_APIKEY = frameworkInfo['APIKeys']['bitquery']

class bitQuery:
    
    def ___init__(self, name, tables):
        self.name = name
    

    def runQuery(self, query):  # A simple function to use requests.post to make the API call.
        _bitQueryApiKey = BITQUERY_APIKEY
        headers = {'X-API-KEY': _bitQueryApiKey}
        request = requests.post('https://graphql.bitquery.io/',
                                json={'query': query}, headers=headers)
        if request.status_code == 200:
            return request.json()
        else:
            raise Exception('Query failed and return code is {}.      {}'.format(request.status_code,
                            query))        
    

class query(ObjectType):
        
    def ___init__(self, name, tables):
        self.name = name
    
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
    
    def getEvents(self, _address, _eventName, _limit, _network = 'ethereum'):
        
        # Encoding _network for bitquery syntax
        networkEncoded = 'ethereum'
        
        if _network == 'ethereum': networkEncoded = 'ethereum' #By default network is already ethereum
        if _network == 'bsc': networkEncoded = 'ethereum(network: {})'.format(_network)
            
            
        _address = '"{}"'.format(_address)
        _eventName = '"{}"'.format(_eventName)
        _limit = '{}'.format(_limit)
        query = """
                {
              """+networkEncoded+""" {
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
    
    def getTransactionsFromAddress(self, _network, _address, _limit):
        _network = '{}'.format(_network)
        _address = '"{}"'.format(_address)
        _limit = '{}'.format(_limit)
        
        query = """
            {
              ethereum(network: """+_network+""") {
                smartContractCalls(
                  options: {desc: "block.timestamp.time", limit: """+_limit+""", offset: 0}
                  date: {since: null, till: null}
                  height: {gt: 0}
                  smartContractAddress: {is: """+_address+"""}
                ) {
                  block {
                    timestamp {
                      time(format: "%Y-%m-%d %H:%M:%S")
                    }
                    height
                  }
                  smartContractMethod {
                    name
                    signatureHash
                  }
                  address: caller {
                    address
                    annotation
                  }
                  transaction {
                    hash
                  }
                  gasValue
                  external
                  amount
                }
              }
            }
        """
        
        return query