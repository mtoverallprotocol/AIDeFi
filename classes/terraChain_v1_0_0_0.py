import os
from . import settings as settings
settings = settings.settings()

frameworkInfo = settings.getFrameworkInfo()
CONNECTION_STRING = frameworkInfo['ConnectionString']['connectionString']
ETHERSCAN_APIKEY = frameworkInfo['APIKeys']['etherscan']
BSCSCAN_APIKEY = frameworkInfo['APIKeys']['bscscan']
BITQUERY_APIKEY = frameworkInfo['APIKeys']['bitquery']
CLASSES_FOLDER = frameworkInfo['Folders']['classes']
CONTRACTS_FOLDER = frameworkInfo['Folders']['contracts']
TRANSACTIONS_FOLDER = frameworkInfo['Folders']['transactions']
LISTENERS_FOLDER = frameworkInfo['Folders']['listeners']

from terra_sdk.client.lcd import LCDClient

class terraChain:
    
    def ___init__(self, name):
        self.name = name

    #################################################################################################################
    
    # @dev: Function that allow us to know if a text is hex or not without errors
    # Input: text to be analysed
    # Output: Boolean
    
    def connect(self, _chainID = 'columbus-4'):
        terra = LCDClient(chain_id=_chainID, url="https://lcd.terra.dev")
        return terra
