from breeze_connect import BreezeConnect
import urllib
breeze = BreezeConnect(api_key="77%U3I71634^099gN232777%316Q~v4=")
breeze.generate_session(api_secret="9331K77(I8_52JG2K73$5438q95772j@",
                        session_token="47437327")

import numpy as np
import pandas as pd
import pandas_ta as ta
import time
from datetime import datetime, date, timedelta, time as t
import csv, re, time, math

import warnings
warnings.filterwarnings("ignore")

time_1 = t(3, 45)  # 9:17 AM IST -> 3:47 AM UTC
time_2 = t(9, 50)  # 3:01 PM IST -> 9:31 AM UTC
order = 0
order2 = 0
expiry = '2024-09-26'
fut_expiry = '2024-09-26'

while True:
    now = datetime.now()
    if t(9, 30)<t(datetime.now().time().hour, datetime.now().time().minute)<t(15, 30) :
        today = datetime.now().strftime("%Y-%m-%d")
        yesterday = (datetime.now() - timedelta(days=5)).strftime("%Y-%m-%d")
        
        
        if order == 0 and now.second == 0 and t(datetime.now().time().hour, datetime.now().time().minute)<t(15, 0) :
            SL = 0
            initial_point = 0
            data = breeze.get_historical_data_v2(interval="1minute",
                                                     from_date= f"{yesterday}T00:00:00.000Z",
                                                     to_date= f"{today}T17:00:00.000Z",
                                                     stock_code="NIFTY",
                                                     exchange_code="NFO",
                                                     product_type="futures",
                                                     expiry_date=f'{fut_expiry}T07:00:00.000Z',
                                                     right="others",
                                                     strike_price="0")
        
            olhc = data['Success']
            olhc = pd.DataFrame(olhc)
            olhc['datetime'] = pd.to_datetime(olhc['datetime'])
            olhc = olhc[(olhc['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                           (olhc['datetime'].dt.time <= pd.to_datetime('15:29').time())]
        
            olhc['12_EMA'] = olhc['close'].ewm(span=12, adjust=False).mean()
            olhc['26_EMA'] = olhc['close'].ewm(span=26, adjust=False).mean()
            olhc['MACD_Line'] = olhc['12_EMA'] - olhc['26_EMA']
            olhc['Signal_Line'] = olhc['MACD_Line'].ewm(span=9, adjust=False).mean()
            olhc['MACD_Histogram'] = olhc['MACD_Line'] - olhc['Signal_Line']
            olhc['MACD'] = olhc['MACD_Line']
        
            olhc['close'] = pd.to_numeric(olhc['close'])
            olhc.ta.rsi(close='close', length=14, append=True)
            olhc['ATR'] = ta.atr(olhc['high'], olhc['low'], olhc['close'], length=14)
        
            last_row = olhc.iloc[-1]
            tsl = last_row['ATR']

            
            atm = round(last_row['close']/50) * 50
            candles_7 = olhc.iloc[-7:]
            rsi_resistance = candles_7['RSI_14'].max()
            rsi_support = candles_7['RSI_14'].min()

            last_7_rows = olhc.tail(7)

            if any(value > 60 for value in last_7_rows['RSI_14']):
                if len(olhc) > 7:
                    subsequent_rows = olhc.iloc[-len(olhc) + 7:]
                    if any(value < 60 for value in subsequent_rows['RSI_14']):
                        if last_row['Signal_Line']>last_row['MACD'] :
                       
                            leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                        exchange_code="NFO",
                                                        product_type="options",
                                                        expiry_date=f'{expiry}T06:00:00.000Z',
                                                        right="put",
                                                        strike_price=atm)
                            leg = leg['Success']
                            leg = pd.DataFrame(leg)
                            buy_premium = float(leg['ltp'][0])
                            SL = buy_premium - 15
        
                            order = -1
                            entry_time = datetime.now().strftime('%H:%M:%S')
                            print(now, 'Buy', atm, 'Put at:', buy_premium)
                        else:
                            print('waiting for MACD...')
        
            elif any(value < 40 for value in last_7_rows['RSI_14']):
                if len(olhc) > 7:
                    subsequent_rows = olhc.iloc[-len(olhc) + 7:]
                    if any(value > 40 for value in subsequent_rows['RSI_14']):
                        if last_row['Signal_Line']<last_row['MACD'] :
                       
                            leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                        exchange_code="NFO",
                                                        product_type="options",
                                                        expiry_date=f'{expiry}T06:00:00.000Z',
                                                        right="call",
                                                        strike_price=atm)
                            leg = leg['Success']
                            leg = pd.DataFrame(leg)
                            buy_premium = float(leg['ltp'][0])
                            SL = buy_premium - 15
        
                            order = 1
                            entry_time = datetime.now().strftime('%H:%M:%S')
                            print(now, 'Buy', atm, 'call at:', buy_premium)
                        else:
                            print('waiting for MACD...')
        
            else:
                print(now, 'No open position')
            
        
                
        if order == 1 :
            time.sleep(20)
            data = breeze.get_historical_data_v2(interval="1minute",
                                                     from_date= f"{yesterday}T00:00:00.000Z",
                                                     to_date= f"{today}T17:00:00.000Z",
                                                     stock_code="NIFTY",
                                                     exchange_code="NFO",
                                                     product_type="futures",
                                                     expiry_date=f'{fut_expiry}T07:00:00.000Z',
                                                     right="others",
                                                     strike_price="0")
        
            olhc = data['Success']
            olhc = pd.DataFrame(olhc)
            olhc['datetime'] = pd.to_datetime(olhc['datetime'])
            olhc = olhc[(olhc['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                           (olhc['datetime'].dt.time <= pd.to_datetime('15:29').time())]
        
            olhc['12_EMA'] = olhc['close'].ewm(span=12, adjust=False).mean()
            olhc['26_EMA'] = olhc['close'].ewm(span=26, adjust=False).mean()
            olhc['MACD_Line'] = olhc['12_EMA'] - olhc['26_EMA']
            olhc['Signal_Line'] = olhc['MACD_Line'].ewm(span=9, adjust=False).mean()
            olhc['MACD_Histogram'] = olhc['MACD_Line'] - olhc['Signal_Line']
            olhc['MACD'] = olhc['MACD_Line']
        
            olhc['close'] = pd.to_numeric(olhc['close'])
            olhc.ta.rsi(close='close', length=14, append=True)
            olhc['ATR'] = ta.atr(olhc['high'], olhc['low'], olhc['close'], length=14)
        
            last_row = olhc.iloc[-1]

            
            
            leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                        exchange_code="NFO",
                                                        product_type="options",
                                                        expiry_date=f'{expiry}T06:00:00.000Z',
                                                        right="call",
                                                        strike_price=atm)
            leg = leg['Success']
            leg = pd.DataFrame(leg)
            exit_premium = float(leg['ltp'][0])
            pnl = exit_premium - buy_premium
            if pnl > initial_point :
                initial_point = pnl
                SL = exit_premium - 15
            if exit_premium <= SL or (t(datetime.now().time().hour, datetime.now().time().minute) == t(15,20)) :
                order = 0
                exit_time = datetime.now().strftime('%H:%M:%S')
                print(now, 'SL Hits, PNL is:', pnl)
                
                csv_file = "Mean_reversion_macd.csv"
                try:
                    with open(csv_file, 'x', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(['Date', 'Entry Time', 'Strike', 'CE/PE','Entry Premium', 'Exit Time', 'Exit premium', 'PnL', 'RSI reverse point'])
                except FileExistsError:
                    pass
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([today, entry_time, atm, 'CE', buy_premium, exit_time, exit_premium, pnl, rsi_support])
                
            
            if last_row['Signal_Line']>last_row['MACD'] or pnl<(-15):
                order = 0
                exit_time = datetime.now().strftime('%H:%M:%S')
                print(now, 'Exit position, PNL is', pnl)
                
                csv_file = "Mean_reversion_macd.csv"
                try:
                    with open(csv_file, 'x', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(['Date', 'Entry Time', 'Strike', 'CE/PE','Entry Premium', 'Exit Time', 'Exit premium', 'PnL', 'RSI resverse point'])
                except FileExistsError:
                    pass
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([today, entry_time, atm, 'CE', buy_premium, exit_time, exit_premium, pnl, rsi_support])
                
            else:
                print(now, 'No Exit Condition')
                
        if order == -1 :
            time.sleep(20)
            data = breeze.get_historical_data_v2(interval="1minute",
                                                     from_date= f"{yesterday}T00:00:00.000Z",
                                                     to_date= f"{today}T17:00:00.000Z",
                                                     stock_code="NIFTY",
                                                     exchange_code="NFO",
                                                     product_type="futures",
                                                     expiry_date=f'{fut_expiry}T07:00:00.000Z',
                                                     right="others",
                                                     strike_price="0")
        
            olhc = data['Success']
            olhc = pd.DataFrame(olhc)
            olhc['datetime'] = pd.to_datetime(olhc['datetime'])
            olhc = olhc[(olhc['datetime'].dt.time >= pd.to_datetime('09:15').time()) &
                           (olhc['datetime'].dt.time <= pd.to_datetime('15:29').time())]
        
            olhc['12_EMA'] = olhc['close'].ewm(span=12, adjust=False).mean()
            olhc['26_EMA'] = olhc['close'].ewm(span=26, adjust=False).mean()
            olhc['MACD_Line'] = olhc['12_EMA'] - olhc['26_EMA']
            olhc['Signal_Line'] = olhc['MACD_Line'].ewm(span=9, adjust=False).mean()
            olhc['MACD_Histogram'] = olhc['MACD_Line'] - olhc['Signal_Line']
            olhc['MACD'] = olhc['MACD_Line']
        
            olhc['close'] = pd.to_numeric(olhc['close'])
            olhc.ta.rsi(close='close', length=14, append=True)
            olhc['ATR'] = ta.atr(olhc['high'], olhc['low'], olhc['close'], length=14)
        
            last_row = olhc.iloc[-1]
            
            leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                        exchange_code="NFO",
                                                        product_type="options",
                                                        expiry_date=f'{expiry}T06:00:00.000Z',
                                                        right="put",
                                                        strike_price=atm)
            leg = leg['Success']
            leg = pd.DataFrame(leg)
            exit_premium = float(leg['ltp'][0])
            pnl = exit_premium - buy_premium
            if pnl > initial_point :
                initial_point = pnl
                SL = exit_premium - 15
            if exit_premium <= SL or (t(datetime.now().time().hour, datetime.now().time().minute) == t(15,20)) :
                order = 0
                exit_time = datetime.now().strftime('%H:%M:%S')
                print(now, 'SL Hits, PNL is:', pnl)
                csv_file = "Mean_reversion_macd.csv"
                try:
                    with open(csv_file, 'x', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(['Date', 'Entry Time', 'Strike', 'CE/PE','Entry Premium', 'Exit Time', 'Exit premium', 'PnL','RSI reverse point'])
                except FileExistsError:
                    pass
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([today, entry_time, atm, 'PE', buy_premium, exit_time, exit_premium, pnl, rsi_resistance])
                
            if last_row['Signal_Line']<last_row['MACD'] or pnl<(-15) :
                order = 0
                exit_time = datetime.now().strftime('%H:%M:%S')
                leg = breeze.get_option_chain_quotes(stock_code="NIFTY",
                                                        exchange_code="NFO",
                                                        product_type="options",
                                                        expiry_date=f'{expiry}T06:00:00.000Z',
                                                        right="put",
                                                        strike_price=atm)
                leg = leg['Success']
                leg = pd.DataFrame(leg)
                exit_premium = float(leg['ltp'][0])
                pnl = exit_premium - buy_premium
                
                print(now, 'Exit position, PNL is', pnl)
                
                csv_file = "Mean_reversion_macd.csv"
                try:
                    with open(csv_file, 'x', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow(['Date', 'Entry Time', 'Strike', 'CE/PE','Entry Premium', 'Exit Time', 'Exit premium', 'PnL', 'RSI reverse point'])
                except FileExistsError:
                    pass
                    with open(csv_file, 'a', newline='') as file:
                        writer = csv.writer(file)
                        writer.writerow([today, entry_time, atm, 'PE', buy_premium, exit_time, exit_premium, pnl, rsi_resistance])
                
            else:
                print(now, 'No Exit Condition')
                
        
                
  
