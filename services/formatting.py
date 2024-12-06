# region import dependencies
from services.db_connection import FFRawTrade, FFFilterTick, Session, OneMinFilteringTick
from services.masterfile import MasterSymbol, cache_symbol_id
from datetime import datetime as dt
from typing import Optional, List
from dotenv import load_dotenv
from utils.functions import convert_nan_date, convert_nan_int, convert_nan_string
import os
import logging
            
import threading
import pandas as pd
import numpy as np
import traceback
# endregion

# region declaration TickFomatting
load_dotenv()
class TickFomatting:
    def __init__(self):
        self.user_name = os.getenv("TRUEDATA_USERID")
        self.password = os.getenv("TRUEDATA_PASSWORD")
        self.logger = logging
        self.cache_map = cache_symbol_id(self.user_name, self.password)
        self.session = None
        self.cache_pts = {} # cache for previous tick
        self.cache_pfts = {} # cache for previous ticks, maxLength = 60
        self.cache_pfts_one_min = {} # cache for previous ticks to format one minute bar series
        
    def store_prev_ticks(self, row: Optional[FFRawTrade]):
        try:
            if row is None:
                return
            self.cache_pts[row.symbol] = row
        except Exception as ex:
            print(f"TickFormatting/store_prev_ticks Error: {ex}")
            
    def store_prev_filtering_ticks(self, row: Optional[FFFilterTick]):
        try:
            if row is None:
                return 
            pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(row.symbol_id, None)
            if pfts is None:
                pfts = []
                pfts.append(row)
            elif len(pfts) == 60:
                pfts.pop(0)
                pfts.append(row)
            else:
                pfts.append(row)
            self.cache_pfts[row.symbol_id] = pfts
        except Exception as ex:
            print(f"TickFormatting/store_prev_filtering_ticks Error: {ex}")
    
    def store_prev_filtering_ticks_for_one_min(self, row: Optional[FFFilterTick]):
        try:
            if row is None:
                return
            pfts: Optional[List[FFFilterTick]] = self.cache_pfts_one_min.get(row.symbol_id, None)
            if pfts is None:
                pfts = []
                pfts.append(row)
            else:
                pfts.append(row)
            
            self.cache_pfts_one_min[row.symbol_id] = pfts
        except Exception as ex:
            print(f"TickFormatting/store_prev_filtering_ticks_for_one_min Error: {ex}")

    def neighboring_ticks(self, datas):
        self.session = Session()  # Initialize the session once
        batch = []
        for row in datas:
            prev_tick = self.cache_pts.get(row["symbol"], None)
            ct = self.create_tick(row, prev_tick)
            if ct:
                batch.append(ct)
        if batch:  # Commit any remaining ticks
            self.session.bulk_save_objects(batch)
            self.session.commit()
        self.check_second(datas)
        self.session.close()
            
    def create_tick(self, data, pt: Optional[FFRawTrade]):
        try:
            self.session = Session()
            # initilize the filtering tick as rt
            rt = FFFilterTick()
            # format the tick message as FFRawTrade
            ct = FFRawTrade(
                    timestamp = dt.strptime(data["timestamp"], "%Y-%m-%dT%H:%M:%S"),
                    symbol_id = data["symbol_id"],
                    symbol = data["symbol"],
                    ltp = data["ltp"],
                    ltq = data["ltq"],
                    atp = data["atp"],
                    ttq = data["ttq"],
                    day_open = data["day_open"],
                    day_high = data["day_high"],
                    day_low = data["day_low"],
                    prev_day_close = data["prev_day_close"],
                    oi = data["oi"],
                    prev_day_oi = data["prev_day_oi"],
                    turnover = data["turnover"],
                    tick_seq = data["tick_seq"],
                    best_bid_price = data["best_bid_price"],
                    best_bid_qty = data["best_bid_qty"],
                    best_ask_price = data["best_ask_price"],
                    best_ask_qty = data["best_ask_qty"],
                )
            
            self.store_prev_ticks(ct)
            
            cache_ms = self.cache_map.get(str(int(ct.symbol_id)), None)
            if cache_ms is None:
                return
            
            if cache_ms is None:
                return None
            
            if pt is None:
                return None
            
            ms = MasterSymbol(
                symbol = cache_ms['symbol'],
                series = cache_ms['series'],
                isin = convert_nan_string(cache_ms['isin'], 'isin'),
                exchange = cache_ms['exchange'],
                lotsize = convert_nan_int(cache_ms['lotsize'], 'lotsize'),
                strike = convert_nan_int(cache_ms['strike'], 'strike'),
                expiry = convert_nan_date(cache_ms['expiry'], 'expiry'),
                metastocksymbol = convert_nan_string(cache_ms['metastocksymbol'], 'metasocksymbol'),
                symbolalias = convert_nan_string(cache_ms['symbolalias'], 'symbolalias'),
                token = convert_nan_int(cache_ms['token'], 'token'),
                underlying = convert_nan_string(cache_ms['underlying'], 'underlying'),
                ticksize = convert_nan_int(cache_ms['ticksize'], 'ticksize'),
            )
            
            rt.symbol_id = ct.symbol_id
            rt.timestamp = ct.timestamp
            rt.ltp = ct.ltp
            rt.bid = float(ct.best_bid_price)
            rt.bid_qty = ct.best_bid_qty
            rt.ask = float(ct.best_ask_price)
            rt.ask_qty = ct.best_ask_qty
            rt.volume = ct.ttq
            rt.oi = ct.oi
            rt.last_size = ct.ltq
            rt.lot_size = ms.lotsize
            rt.symbol = ms.symbol
            rt.symbol_name = f"{ms.symbol}{ms.exchange}"
            rt.exchange_token = ms.token
            rt.underlier_symbol = self.calc_underlying_symbol(ms.series, ms.underlying, ms.symbol)
            rt.underlier_price = self.calc_underlying_price(rt.underlier_symbol, ct.ltp)
            rt.tag = self.calc_tag(int(ct.symbol_id), ct.ltp, pt.ltp)
            rt.aggressor = self.calc_aggressor(ct, pt)
            rt.trade_value = ct.ltp * ct.ltq
            rt.delta_volume = self.calc_delta_volume(rt.tag, ct.ltq)
            if rt.delta_volume is not None:
                rt.delta_volume_value = ct.ltp * rt.delta_volume
            rt.sell_volume = self.calc_sell_buy("sell", rt.delta_volume)
            rt.sell_value = self.calc_sell_buy("sell", rt.delta_volume_value)
            rt.buy_volume = self.calc_sell_buy("buy", rt.delta_volume)
            rt.buy_value = self.calc_sell_buy("buy", rt.delta_volume_value)
            rt.symbol_type = ms.series
            rt.exchange = ms.exchange
            rt.tick_size = ms.ticksize
            rt.expiry = ms.expiry
            rt.option_type = ms.series
            rt.strike = ms.strike
            rt.cumulative_delta = self.calc_cumulative_delta(ct, rt.delta_volume_value)
            rt.oi_change = ct.oi - pt.oi
            rt.moneyness = self.calc_moneyness(ms.series, ms.strike, ct.ltp)
            rt.strike_difference = self.calc_strike_difference(ms.strike, ct.ltp)
            rt.symbol_root = ms.symbolalias
            rt.local_id = f"{ms.symbol}_{ms.series}_{ms.exchange}"
            rt.is_contract_active = self.calc_is_contract_active(ms.expiry)
            rt.oi_build_up = self.calc_oi_build_up(ct.ltp, ct.prev_day_close, ct.oi, ct.prev_day_oi)
            rt.sentiment = self.calc_sentiment(rt.oi_build_up)
            rt.tick_seq = ct.tick_seq
            
            self.store_prev_filtering_ticks(rt)
            self.store_prev_filtering_ticks_for_one_min(rt)
            rt.sweep1 = self.calc_sweep1(ct)
            rt.sweep1_volume = self.calc_sweep1_volume(rt.sweep1, ct)
            rt.sweep1_value = self.calc_sweep1_value(rt.sweep1, ct)
            rt.sweep2 = self.calc_sweep2(ct)
            rt.sweep2_volume = self.calc_sweep2_volume(rt.sweep2, ct)
            rt.sweep2_value = self.calc_sweep2_value(rt.sweep2, ct)
            rt.sweep3 = self.calc_sweep3(ct)
            rt.sweep3_volume = self.calc_sweep3_volume(rt.sweep3, ct)
            rt.sweep3_value = self.calc_sweep3_value(rt.sweep3, ct)
            rt.sweep4 = self.calc_sweep4(ct)
            rt.sweep4_volume = self.calc_sweep4_volume(rt.sweep4, ct)
            rt.sweep4_value = self.calc_sweep4_value(rt.sweep4, ct)
            rt.power_sweep = self.calc_power_sweep(ct)
            rt.power_sweep_volume = self.calc_power_sweep_volume(rt.power_sweep, ct)
            rt.power_sweep_value = self.calc_power_sweep_value(rt.power_sweep, ct)
            rt.block1 = self.calc_block1(ct)
            rt.block1_volume = self.calc_block1_volume(rt.block1, rt.delta_volume)
            rt.block1_value = self.calc_block1_value(rt.block1, rt.delta_volume_value)
            rt.block2 = self.calc_block2(ct)
            rt.block2_volume = self.calc_block2_volume(rt.block2, rt.delta_volume)
            rt.block2_value = self.calc_block2_value(rt.block2, rt.delta_volume_value)
            rt.power_block = self.calc_power_block(ct)
            rt.power_block_volume = self.calc_power_block_volume(rt.power_block, rt.delta_volume)
            rt.power_block_value = self.calc_power_block_value(rt.power_block, rt.delta_volume_value)
            
            # store previous tick as cache to improve the performance
            return rt
        except Exception as ex:
            print(f"TickFormatting/create_tick: {ex}")
            traceback.print_exc()
            return None
    
    def check_second(self, datas):
        try:
            target_tmp = datas[0]["timestamp"]
            time = dt.strptime(target_tmp, "%Y-%m-%dT%H:%M:%S")
            if time.second == 0:
                print(f"Check the second of timestamp {target_tmp}")
                one_min_thread = threading.Thread(target=self.create_minute_filtering_tick, args=(self.cache_pfts_one_min,))
                one_min_thread.start()
                # initialize the cache for one minute filtering after proessing
                self.cache_pfts_one_min = {}
        except Exception as ex:
            print(f"TickFormatting/check_second Error: {ex}")
            
    def create_minute_filtering_tick(self, cache_pfts_one_min):
        try:
            session = Session()  # Start a new session
            batch = []  # To store all objects to be committed later

            for symbol_id, ticks in cache_pfts_one_min.items():
                # Extract tick data from SQLAlchemy objects
                tick_data = [tick.__dict__ for tick in ticks]
                
                # Create a DataFrame, excluding internal SQLAlchemy keys
                rows = pd.DataFrame([{k: v for k, v in t.items() if not k.startswith('_')} for t in tick_data])

                # Create a new instance of the OneMinFilteringTick
                ot = OneMinFilteringTick()

                # Populate attributes
                ot.symbol_id = symbol_id
                ot.timestamp = rows.iloc[-1]['timestamp'].floor('min').to_pydatetime()  # Replace with the appropriate timestamp logic if needed
                ot.open = rows.iloc[0]['ltp']
                ot.high = rows['ltp'].max()
                ot.low = rows['ltp'].min()
                ot.close = rows['ltp'].iloc[-1]
                ot.volume = rows['last_size'].sum()  # Summing volume
                ot.oi = rows['oi'].iloc[-1]
                ot.symbol = rows['symbol'].iloc[-1]
                ot.underlier_symbol = rows['underlier_symbol'].iloc[-1]
                ot.underlier_price = rows['underlier_price'].iloc[-1]
                ot.trade_value = rows['trade_value'].sum()
                ot.delta_volume = rows['delta_volume'].sum()
                ot.delta_volume_value = rows['delta_volume_value'].sum()
                ot.buy_volume = rows['buy_volume'].sum()
                ot.buy_value = rows['buy_value'].sum()
                ot.sell_volume = rows['sell_volume'].sum()
                ot.sell_value = rows['sell_value'].sum()
                ot.oi_change = rows['oi'].iloc[-1] - rows['oi'].iloc[0]
                ot.min_delta = rows['cumulative_delta'].min()
                ot.max_delta = rows['cumulative_delta'].max()
                ot.oi_build_up = ""  # This appears to be a placeholder

                # Convert any numpy types to native Python types
                self.convert_numpy_types(ot)

                # Add to the batch for bulk saving
                batch.append(ot)

            # Commit the batch if there are objects to save
            if batch:
                session.bulk_save_objects(batch)
                session.commit()

        except Exception as ex:
            session.rollback()  # Rollback in case of an error
            print(f"TickFormatting/create_minute_filtering_tick Error: {ex}")
        finally:
            session.close()  # Always close the session

    def convert_numpy_types(self, obj):
        """Recursively convert numpy types to native Python types."""
        for attr in dir(obj):
            # Ignore SQLAlchemy internal attributes
            if attr.startswith('_'):
                continue
            value = getattr(obj, attr)
            if isinstance(value, (np.integer, np.int64)):
                setattr(obj, attr, int(value))
            elif isinstance(value, (np.floating, np.float64)):
                setattr(obj, attr, float(value))
            elif isinstance(value, list):
                setattr(obj, attr, [self.convert_numpy_types(v) if isinstance(v, (np.integer, np.floating)) else v for v in value])
            elif isinstance(value, dict):
                setattr(obj, attr, {k: self.convert_numpy_types(v) if isinstance(v, (np.integer, np.floating)) else v for k, v in value.items()})
        
    def calc_underlying_symbol(self, exchange, underlying, symbol):
        try:
            if exchange is None:
                return None
            if exchange == "IN":
                return symbol
            else:
                if underlying is None:
                    return symbol
                if underlying == "NIFTY":
                    return "NIFTY 50"
                elif underlying == "BANKNIFTY":
                    return "NIFTY BANK"
                elif underlying == "FINNIFTY":
                    return "NIFTY FIN SERVICE"
                elif underlying == "MIDCPNIFTY":
                    return "NIFTY MID SELECT"
                else:
                    return underlying
        except Exception as ex:
            print(f"TickFormatting/calc_underlying_symbol Error: {ex}")
        
    def calc_underlying_price(self, underlying, ltp):
        try:
            if underlying is None:
                return None
            pt: Optional[FFRawTrade] = self.cache_pts.get(underlying, None)
            if pt is None:
                return None
            return pt.ltp
        except Exception as ex:
            print(f"TickFormatting/calc_underlying_price Error: {ex}")
              
    def calc_delta_volume(self, tag, cur_ltq):
        try:
            if tag is None:
                return 0
            if tag == "Buy":
                return cur_ltq
            else:
                return -cur_ltq
        except Exception as ex:
            print(f"TickFormatting/calc_delta_volumn Error: {ex}")
            return None
    
    def calc_moneyness(self, option, strike, underlying_price):
        try:
            if option is None or strike is None or underlying_price is None:
                return None
            if option == "CE":
                if underlying_price > strike:
                    return "OTM"
                elif underlying_price < strike:
                    return "ITM"
                else:
                    return "ATM"
            elif option == "PE":
                if underlying_price > strike:
                    return "ITM"
                elif underlying_price < strike:
                    return "OTM"
                else:
                    return "ATM"
        except Exception as ex:
            print(f"TickFormatting/calc_moneyness Error: {ex}")
    
    def calc_tag(self, symbol_id, cur_ltp, prev_ltp):
        try:
            if cur_ltp > prev_ltp:
                return "Buy"
            elif cur_ltp < prev_ltp:
                return "Sell"
            else:
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(symbol_id, None)
                if pfts is None or len(pfts) == 0:
                    return None
                else:
                    return pfts[len(pfts) - 1].tag
        except Exception as ex:
            print(f"TickFormatting/calc_tag Error: {ex}")
            
    def calc_aggressor(self, ct: FFRawTrade, pt: FFRawTrade):
        if ct.ltp >= pt.best_ask_price:
            return "Buy"
        elif ct.ltp <= pt.best_bid_price:
            return "Sell"
        else:
            return None
    
    def calc_strike_difference(self, strike, ltp):
        try:
            if strike is None or strike == 0:
                return None
            else:
                return round((ltp - float(strike)) / float(strike) * 100.0, 4)
        except Exception as ex:
            print(f"Error in calc_strike_difference: {ex}")
            return None
        
    def calc_is_contract_active(self, expiry):
        try:
            if expiry is None:
                return None
            return 1
        except Exception as ex:
            print(f"TickFomatting/calc_is_contract_activate Error: {ex}")
    
    def calc_sell_buy(self, direction, value):
        try:
            if direction == "sell" and value < 0:
                return value
            elif direction == "buy" and value > 0:
                return value
            else:
                return 0
        except Exception as ex:
            print(f"TickFormatting/calc_sell_buy Error: {ex}")
            
    def calc_cumulative_delta(self, ct: FFRawTrade, delta_value):
        try:
            pfts : Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
            if pfts is None:
                return 0.0
            else:
                last_pft: FFFilterTick = pfts[-1]
                return last_pft.delta_volume_value + delta_value
            pass
        except Exception as ex:
            print(f"TickFormatting/calc_cumulative_delta Error: {ex}")
    
    def calc_oi_build_up(self, ltp, prev_c, oi, prev_oi):
        try:
            if ltp > prev_c and oi > prev_oi:
                return "LongBuildup"
            elif ltp < prev_c and oi < prev_oi:
                return "LongUnwinding"
            elif ltp < prev_c and oi > prev_oi:
                return "ShortBuildup"
            elif ltp > prev_c and oi < prev_oi:
                return "ShortCovering"
        except Exception as ex:
            print(f"TickFormatting/calc_oi_build_up Error: {ex}")
    
    def calc_sweep1(self, ct: FFRawTrade):
        try:
            pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
            if pfts is None:
                return None
            elif len(pfts) < 3:
                return None
            else:
                ln = len(pfts)
                if pfts[ln - 1].aggressor == pfts[ln - 2].aggressor and pfts[ln - 2].aggressor == pfts[ln - 3].aggressor:
                    if pfts[ln - 1].aggressor == "Buy":
                        return "BuySweep"
                    elif pfts[ln - 1].aggressor =="Sell":
                        return "SellSweep"
                    else:
                        return None
                else:
                    return None
        except Exception as ex:
            return print(f"TickFormatting/calc_sweep1 Error: {ex}")
        
    def calc_sweep1_volume(self, sweep1, ct: FFRawTrade):
        try:
            if sweep1 is None:
                return None
            elif sweep1 == "BuySweep" or "SellSweep":
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
                ln = len(pfts)
                return pfts[ln - 1].delta_volume + pfts[ln - 2].delta_volume + pfts[ln - 3].delta_volume
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_sweep1_volume Error: {ex}")
    
    def calc_sweep1_value(self, sweep1, ct: FFRawTrade):
        try:
            if sweep1 is None:
                return None
            elif sweep1 == "BuySweep" or "SellSweep":
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
                ln = len(pfts)
                return pfts[ln - 1].delta_volume_value + pfts[ln - 2].delta_volume_value + pfts[ln - 3].delta_volume_value
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_sweep1_value Error: {ex}")
        
    def calc_sweep2(self, ct:FFRawTrade):
        try:
            pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
            if pfts is None:
                return None
            elif len(pfts) < 5:
                return None
            else:
                ln = len(pfts)
                if pfts[ln - 1].aggressor == pfts[ln - 2].aggressor and pfts[ln - 2].aggressor == pfts[ln - 3].aggressor and pfts[ln - 3].aggressor == pfts[ln - 4].aggressor and pfts[ln - 4].aggressor == pfts[ln - 5].aggressor:
                    if pfts[ln - 1].aggressor == "Buy":
                        return "BuySweep"
                    elif pfts[ln - 1].aggressor =="Sell":
                        return "SellSweep"
                    else:
                        return None
                else:
                    return None
        except Exception as ex:
            print(f"TickFormatting/calc_sweep2 Error: {ex}")
   
    def calc_sweep2_volume(self, sweep2, ct: FFRawTrade):
        try:
            if sweep2 is None:
                return None
            elif sweep2 == "BuySweep" or "SellSweep":
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
                ln = len(pfts)
                return pfts[ln - 1].delta_volume + pfts[ln - 2].delta_volume + pfts[ln - 3].delta_volume + pfts[ln - 4].delta_volume + pfts[ln - 5].delta_volume
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_sweep2_volume Error: {ex}")
    
    def calc_sweep2_value(self, sweep2, ct: FFRawTrade):
        try:
            if sweep2 is None:
                return None
            elif sweep2 == "BuySweep" or "SellSweep":
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
                ln = len(pfts)
                return pfts[ln - 1].delta_volume_value + pfts[ln - 2].delta_volume_value + pfts[ln - 3].delta_volume_value + pfts[ln - 4].delta_volume_value + pfts[ln - 5].delta_volume_value
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_sweep2_value Error: {ex}")
           
    def calc_sweep3(self, ct:FFRawTrade):
        try:
            pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
            if pfts is None:
                return None
            elif len(pfts) < 7:
                return None
            else:
                ln = len(pfts)
                if pfts[ln - 1].aggressor == pfts[ln - 2].aggressor and pfts[ln - 2].aggressor == pfts[ln - 3].aggressor and pfts[ln - 3].aggressor == pfts[ln - 4].aggressor and pfts[ln - 4].aggressor == pfts[ln - 5].aggressor and pfts[ln - 5].aggressor == pfts[ln - 6].aggressor and pfts[ln - 6].aggressor == pfts[ln - 7].aggressor:
                    if pfts[ln - 1].aggressor == "Buy":
                        return "BuySweep"
                    elif pfts[ln - 1].aggressor =="Sell":
                        return "SellSweep"
                    else:
                        return None
                else:
                    return None
        except Exception as ex:
            print(f"TickFormatting/calc_sweep3 Error: {ex}")

    def calc_sweep3_volume(self, sweep3, ct: FFRawTrade):
        try:
            if sweep3 is None:
                return None
            elif sweep3 == "BuySweep" or "SellSweep":
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
                ln = len(pfts)
                return pfts[ln - 1].delta_volume + pfts[ln - 2].delta_volume + pfts[ln - 3].delta_volume + pfts[ln - 4].delta_volume + pfts[ln - 5].delta_volume + pfts[ln - 6].delta_volume + pfts[ln - 7].delta_volume
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_sweep3_volume Error: {ex}")
    
    def calc_sweep3_value(self, sweep3, ct: FFRawTrade):
        try:
            if sweep3 is None:
                return None
            elif sweep3 == "BuySweep" or "SellSweep":
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
                ln = len(pfts)
                return pfts[ln - 1].delta_volume_value + pfts[ln - 2].delta_volume_value + pfts[ln - 3].delta_volume_value + pfts[ln - 4].delta_volume_value + pfts[ln - 5].delta_volume_value + pfts[ln - 6].delta_volume_value + pfts[ln - 7].delta_volume_value
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_sweep3_value Error: {ex}")

    def calc_sweep4(self, ct:FFRawTrade):
            try:
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
                if pfts is None:
                    return None
                elif len(pfts) < 9:
                    return None
                else:
                    ln = len(pfts)
                    if pfts[ln - 1].aggressor == pfts[ln - 2].aggressor and pfts[ln - 2].aggressor == pfts[ln - 3].aggressor and pfts[ln - 3].aggressor == pfts[ln - 4].aggressor and pfts[ln - 4].aggressor == pfts[ln - 5].aggressor and pfts[ln - 5].aggressor == pfts[ln - 6].aggressor and pfts[ln - 6].aggressor == pfts[ln - 7].aggressor and pfts[ln - 7].aggressor == pfts[ln - 8].aggressor and pfts[ln - 8].aggressor == pfts[ln - 9].aggressor:
                        if pfts[ln - 1].aggressor == "Buy":
                            return "BuySweep"
                        elif pfts[ln - 1].aggressor =="Sell":
                            return "SellSweep"
                        else:
                            return None
                    else:
                        return None
            except Exception as ex:
                print(f"TickFormatting/calc_sweep4 Error: {ex}")

    def calc_sweep4_volume(self, sweep4, ct: FFRawTrade):
        try:
            if sweep4 is None:
                return None
            elif sweep4 == "BuySweep" or "SellSweep":
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
                ln = len(pfts)
                return pfts[ln - 1].delta_volume + pfts[ln - 2].delta_volume + pfts[ln - 3].delta_volume + pfts[ln - 4].delta_volume + pfts[ln - 5].delta_volume + pfts[ln - 6].delta_volume + pfts[ln - 7].delta_volume + pfts[ln - 8].delta_volume + pfts[ln - 9].delta_volume
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_sweep4_volume Error: {ex}")
    
    def calc_sweep4_value(self, sweep4, ct: FFRawTrade):
        try:
            if sweep4 is None:
                return None
            elif sweep4 == "BuySweep" or "SellSweep":
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
                ln = len(pfts)
                return pfts[ln - 1].delta_volume_value + pfts[ln - 2].delta_volume_value + pfts[ln - 3].delta_volume_value + pfts[ln - 4].delta_volume_value + pfts[ln - 5].delta_volume_value + pfts[ln - 6].delta_volume_value + pfts[ln - 7].delta_volume_value + pfts[ln - 8].delta_volume_value + pfts[ln - 9].delta_volume_value
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_sweep4_value Error: {ex}")
   
    def calc_power_sweep(self, ct:FFRawTrade):
            try:
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
                if pfts is None:
                    return None
                elif len(pfts) < 10:
                    return None
                else:
                    ln = len(pfts)
                    if pfts[ln - 1].aggressor == pfts[ln - 2].aggressor and pfts[ln - 2].aggressor == pfts[ln - 3].aggressor and pfts[ln - 3].aggressor == pfts[ln - 4].aggressor and pfts[ln - 4].aggressor == pfts[ln - 5].aggressor and pfts[ln - 5].aggressor == pfts[ln - 6].aggressor and pfts[ln - 6].aggressor == pfts[ln - 7].aggressor and pfts[ln - 7].aggressor == pfts[ln - 8].aggressor and pfts[ln - 8].aggressor == pfts[ln - 9].aggressor and pfts[ln - 9].aggressor == pfts[ln - 10].aggressor:
                        if pfts[ln - 1].aggressor == "Buy":
                            return "BuySweep"
                        elif pfts[ln - 1].aggressor =="Sell":
                            return "SellSweep"
                        else:
                            return None
                    else:
                        return None
            except Exception as ex:
                print(f"TickFormatting/calc_power_sweep Error: {ex}")
    
    def calc_power_sweep_volume(self, power_sweep, ct: FFRawTrade):
        try:
            if power_sweep is None:
                return None
            elif power_sweep == "BuySweep" or "SellSweep":
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
                ln = len(pfts)
                return pfts[ln - 1].delta_volume + pfts[ln - 2].delta_volume + pfts[ln - 3].delta_volume + pfts[ln - 4].delta_volume + pfts[ln - 5].delta_volume + pfts[ln - 6].delta_volume + pfts[ln - 7].delta_volume + pfts[ln - 8].delta_volume + pfts[ln - 9].delta_volume + pfts[ln - 10].delta_volume
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_power_sweep_volume Error: {ex}")
    
    def calc_power_sweep_value(self, power_sweep, ct: FFRawTrade):
        try:
            if power_sweep is None:
                return None
            elif power_sweep == "BuySweep" or "SellSweep":
                pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
                ln = len(pfts)
                return pfts[ln - 1].delta_volume_value + pfts[ln - 2].delta_volume_value + pfts[ln - 3].delta_volume_value + pfts[ln - 4].delta_volume_value + pfts[ln - 5].delta_volume_value + pfts[ln - 6].delta_volume_value + pfts[ln - 7].delta_volume_value + pfts[ln - 8].delta_volume_value + pfts[ln - 9].delta_volume_value + pfts[ln - 10].delta_volume_value
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_power_sweep_value Error: {ex}")
   
    def calc_block1(self, ct: FFRawTrade):
        try:
            pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
            if pfts is None:
                return None
            elif len(pfts) < 6:
                return None
            else:
                ln = len(pfts)
                if pfts[ln - 1].last_size > pfts[ln - 2].last_size + pfts[ln - 3].last_size + pfts[ln - 4].last_size + pfts[ln - 5].last_size + pfts[ln - 6].last_size:
                    return "Block"
                else:
                    return None
        except Exception as ex:
            print(f"TickFormatting/calc_block1 Error: {ex}")

    def calc_block1_volume(self, block1, volume):
        try:
            if block1 is None:
                return None
            elif block1 == "Block":
                return volume
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_block1_volume Error: {ex}")

    def calc_block1_value(self, block1, value):
        try:
            if block1 is None:
                return None
            elif block1 == "Block":
                return value
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_block1_value Error: {ex}")

    def calc_block2(self, ct: FFRawTrade):
        try:
            pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
            if pfts is None:
                return None
            elif len(pfts) < 11:
                return None
            else:
                ln = len(pfts)
                if pfts[ln - 1].last_size > pfts[ln - 2].last_size + pfts[ln - 3].last_size + pfts[ln - 4].last_size + pfts[ln - 5].last_size + pfts[ln - 6].last_size + pfts[ln - 7].last_size + pfts[ln - 8].last_size + pfts[ln - 9].last_size + pfts[ln - 10].last_size + pfts[ln - 11].last_size:
                    return "Block"
                else:
                    return None
        except Exception as ex:
            print(f"TickFormatting/calc_block2 Error: {ex}")

    def calc_block2_volume(self, block2, volume):
        try:
            if block2 is None:
                return None
            elif block2 == "Block":
                return volume
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_block2_volume Error: {ex}")

    def calc_block2_value(self, block2, value):
        try:
            if block2 is None:
                return None
            elif block2 == "Block":
                return value
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_block2_value Error: {ex}")
   
    def calc_power_block(self, ct: FFRawTrade):
        try:
            pfts: Optional[List[FFFilterTick]] = self.cache_pfts.get(ct.symbol_id, None)
            if pfts is None:
                return None
            elif len(pfts) < 21:
                return None
            else:
                ln = len(pfts)
                if pfts[ln - 1].last_size > pfts[ln - 2].last_size + pfts[ln - 3].last_size + pfts[ln - 4].last_size + pfts[ln - 5].last_size + pfts[ln - 6].last_size + pfts[ln - 7].last_size + pfts[ln - 8].last_size + pfts[ln - 9].last_size + pfts[ln - 10].last_size + pfts[ln - 11].last_size + pfts[ln - 12].last_size + pfts[ln - 13].last_size + pfts[ln - 14].last_size + pfts[ln - 15].last_size + pfts[ln - 16].last_size + pfts[ln - 17].last_size + pfts[ln - 18].last_size + pfts[ln - 19].last_size + pfts[ln - 20].last_size + pfts[ln - 21].last_size:
                    return "Block"
                else:
                    return None
        except Exception as ex:
            print(f"TickFormatting/calc_block2 Error: {ex}")

    def calc_power_block_volume(self, power_block, volume):
        try:
            if power_block is None:
                return None
            elif power_block == "Block":
                return volume
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_power_block_volume Error: {ex}")

    def calc_power_block_value(self, power_block, value):
        try:
            if power_block is None:
                return None
            elif power_block == "Block":
                return value
            else:
                return None
        except Exception as ex:
            print(f"TickFormatting/calc_power_block_value Error: {ex}")
     
    def calc_sentiment(self, oi_build_up):
        try: 
            if  oi_build_up == "LongBuildup" or oi_build_up == "ShortCovering":
                return "Bullish"
            elif oi_build_up == "LongUnwinding" or oi_build_up == "ShortBuildup":
                return "Bearish"
            else:
                return ""
        except Exception as ex:
            return (f"TickFormatting/calc_sentiment Errror: {ex}")
    
    def update_cache_map(self):
        self.cache_map = cache_symbol_id(self.user_name, self.password)
# endregion
