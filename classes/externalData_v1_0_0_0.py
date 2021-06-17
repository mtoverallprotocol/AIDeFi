import pandas_datareader.data as web

class externalData:
    
    def ___init__(self, name):
        self.name = name
    
    #################################################################################################################
    
    # @dev: Function that allow us to read data from a provider
    # Input: This ticker symbol, the provider name ('yahoo'), start date ('YYYY-MM-DD'), end date ('YYYY-MM-DD')
    # Output: Web3 connection
    
    def getStockQuote(self, _ticker, _provider, _start, _end):
        return web.DataReader(_ticker, data_source = _provider, start = _start, end = _end)
    
    def getNasdaqData(self):
        
        headers = {
            'authority': 'api.nasdaq.com',
            'accept': 'application/json, text/plain, */*',
            'user-agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/87.0.4280.141 Safari/537.36',
            'origin': 'https://www.nasdaq.com',
            'sec-fetch-site': 'same-site',
            'sec-fetch-mode': 'cors',
            'sec-fetch-dest': 'empty',
            'referer': 'https://www.nasdaq.com/',
            'accept-language': 'en-US,en;q=0.9',
        }

        params = (
            ('tableonly', 'true'),
            ('limit', '25'),
            ('offset', '0'),
            ('download', 'true'),
        )

        r = requests.get('https://api.nasdaq.com/api/screener/stocks', headers=headers, params=params)
        data = r.json()['data']
        return pd.DataFrame(data['rows'], columns=data['headers'])