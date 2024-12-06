import os
import shutil
import requests
from datetime import datetime as dt, date
from io import StringIO
import pandas as pd
import pickle
from dataclasses import dataclass
from datetime import date
from typing import Optional, List
import logging

@dataclass
class MasterSymbol:
    symbol: str
    series: str
    isin: Optional[str]
    exchange: str
    lotsize: Optional[int]
    strike: Optional[int]
    expiry: Optional[date]
    metastocksymbol: Optional[str]
    symbolalias: Optional[str]
    token: Optional[int]
    underlying: Optional[str]
    ticksize: Optional[int]

def cache_symbol_id(username , password ):
    sym_cache_dir = '/'.join(__file__.split('\\')[:-1]) + '/cache/sym_cache/'
    url = 'https://api.truedata.in/getAllSymbols?'
    segments = ['all']
    if not os.path.exists(sym_cache_dir) or not os.path.exists(f'{sym_cache_dir}/sym_cache_{dt.now().strftime("%d%m%y")}.pkl'):
        shutil.rmtree(sym_cache_dir, ignore_errors=True, onerror=None)
        get_symbol_id(segments , url , username , password , sym_cache_dir )
    with open( f"{sym_cache_dir}/sym_cache_{dt.now().strftime('%d%m%y')}.pkl" , 'rb') as pkl:
        symbols_map = pickle.load(pkl)
    logging.warning(f'end download master contracts for today')
    return symbols_map

def get_symbol_id(segments , url , user_id , password , cache_dir):
    cache = {}
    params =  { 'user': user_id ,'password' : password,
                'allexpiry':'false', 'csv':'true','csvheader':'true', 'token': 'true',
                'ticksize': 'true', 'underlying': 'true'}
    for segment in segments:
        params.update({'segment': f'{segment}'.lower(),})
        logging.warning(f'please wait two minute to download master contracts for today')
        response = requests.get(url, params= params).text
        df = pd.read_csv(StringIO(response) , dtype='unicode' )
        df.set_index('symbolid', inplace=True)
        seg_dict = df.to_dict(orient='index')
        cache.update(seg_dict)
    os.makedirs(cache_dir , exist_ok=True )
    with open( f"{cache_dir}/sym_cache_{dt.now().strftime('%d%m%y')}.pkl" , 'wb') as pkl:
        pickle.dump(cache , pkl)