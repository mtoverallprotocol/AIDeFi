# Run by terminal if these modules are not in your enviroment
# pip install pandas-datareader
# pip3 install tensorflow
# pip install numpy
# pip install pandas
# pip install matplotlib
# pip install scipy
# pip install -U scikit-learn
# pip install keras
# pip install get-all-tickers
# Import the Libraries

from web3 import Web3
import pandas as pd
import numpy as np
import pandasql as ps
import string
import re
import math
import pandas_datareader as web
from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense, LSTM
import matplotlib.pyplot as plt
from get_all_tickers import get_tickers as gt
tickers = gt.get_tickers()
plt.style.use('fivethirtyeight')