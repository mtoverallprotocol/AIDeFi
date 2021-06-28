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
# pip install "graphene>=2.0"

# Web3 libraries

from web3 import Web3

# Data Transformation/Analysis libraries

import pandas as pd
import numpy as np
import pandasql as psql
import string
import re
import math

# Data Reader libraries

import pandas_datareader as web
import requests

# Models libraries

from sklearn.preprocessing import MinMaxScaler
from keras.models import Sequential
from keras.layers import Dense, LSTM
import matplotlib.pyplot as plt

# OS libraries

from datetime import datetime
import os
from os import listdir
from os import path
from os.path import isfile, join, isdir
from datetime import datetime
import sys

# Process libraries

import warnings

# Library default Settings

plt.style.use('fivethirtyeight')