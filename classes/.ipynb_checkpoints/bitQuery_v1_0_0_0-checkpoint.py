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
            
    # The GraphQL query
    
    def directQuery(self, _query, _bitQueryApiKey):
        return self.__run_query(_query, _bitQueryApiKey)  # Execute the query

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