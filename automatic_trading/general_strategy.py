# This has been edited to show the general structure of the program, without showing the full code.
import requests  # pip install requests
import hashlib
import hmac
import json
import time
import os
import csv
from datetime import datetime, timedelta
from dateutil.relativedelta import relativedelta
import math
import pandas as pd
import sys
import ast # for reading dictionary from file
import smtplib, ssl # for sending emails
from email.mime.text import MIMEText
from email.utils import formataddr
from email.mime.multipart import MIMEMultipart # New line
from email.mime.base import MIMEBase # New line
from email import encoders # New line

# Global variables
##################
# users
user_filename = "users.txt"
         
# INPUT API CREDENTIALS:
BASE_URL = "https://api.bitfinex.com/"

# parameters
min_minutes = 1 # used to define periods for reading tickers and volumes
cooldown = 60
max_requests = { 'candles' : 60, 'wallet' : 45, 'book' : 30, 'retrieve_orders' : 45, 'ledgers' : 45, 'movements' : 45, 'history' : 45, 'trades' : 45 }
percent_limit_mod = 0.0 # the price of the limit orders are modified by this amount to ensure that they are executed almost immediately

# saved variables
last_time = 0
request_times = { 'candles' : [], 'wallet' : [], 'book' : [], 'retrieve_orders' : [], 'ledgers' : [], 'movements' : [], 'history' : [], 'trades' : [] }

def wait_if_necessary(endpoint):

    return 0

def _nonce():

    return str(int(round(time.time() * 1000)))
    
def _headers(path, nonce, body):

    return {
        "bfx-nonce": nonce,
        "bfx-apikey": API_KEY,
        "bfx-signature": signature,
        "content-type": "application/json"
    }

def check_wallet():

    return 1

def append_and_shorten_if_necessary(candles,max_length,val):

    return candles

def RSI(close, period):

    return RSI2
    
def calculate_obv(closes,volumes):

    return obvs

def sma_obv_trigger(sma,obvsma):

    return 1

def obv_trigger(obvsma):

    return 1

def d2sma_trigger(df):

    return 1

def macd_trigger(sma,fast_sma):

    return 1

def rsi_trigger(rsi,os,ob):

    return 1

def list_differences(lst):

    return dlst
                
def general_strategy(periods,which_strategies,candles):

    return trigger

def apply_strategy(i,is_calculate,candles):

    return 1

def strategy1(is_calculate,candles):

    return 1
    
def strategy2(is_calculate,candles):

    return 1

def which_cryptos_pair(pair):

    return crypto1, crypto2
    
def send_email(users,username,text,pair):

    return
    
def check_candles_hist_append(min_minutes,max_minutes,first_time,candles,pair):
        
    return candles
    
def check_candles_hist(min_minutes,max_minutes,first_time,pair):

    return candles

def check_book(pair):

    return 1

def retrieve_orders2():

    return 1

def ledgers(symbol):

    return 1

def movements(symbol,start):

    return 1

def order_history(order_id):

    return 1

def order_trades(symbol,order_id):

    return 1

def retrieve_orders(order_id):

    return 1

def submit_order(symbol,price,amount,is_limit,is_postonly):

    return 1

def cancel_order(order_id):

    return 1

def withdraw(symbol,amount,address):

    return 1

def withdrawal_methods():

    return response.json()

def minimum_sizes():
    
    return response.json()

def read_user_parameters(user_filename,users):

    return users

def read_user_transactions(users):

    return(users)

def create_transactions_file(users):

    return

def create_cryptos_set(users):

    return cryptos

def find_last_transaction_time(users):

    return last_time
   
def find_last_transaction_time2(crypto,user):

    return 1

def check_new_withdrawals(users):

    return 0

def find_movement(last_time0,crypto,user_hash,amount,is_withdrawal):

    return last_movement

def check_new_deposits(users):

    return 0

def find_balance(crypto,user,transactions):

    return balance

def write_trade(order_id,pair,users,proportions):

    return 0
   
def calculate_trade_amount_simple(users,pair,strategy,buy_or_sell):

    return sum_amount, proportions
   
def calculate_trade_amount(users,pair,strategy,buy_or_sell):
 
    return amount

def analayze_partial_order(execution):
    
    return amount, avg_price

def plot_balances(pair,d2,d3,columns2,columns3,datetimes=[datetime(2018,1,1), datetime(2019,1,1)]):

    return 0

def plot_transactions(d1,d2,d3,columns1,columns2,columns3,datetimes=[datetime(2018,1,1), datetime(2019,1,1)]):

    return 0

if __name__ == "__main__":
    candles = {}
    users = {}
    min_minutes, max_minutes = 1e10, 0
    while True:

        # update user info
        users = read_user_parameters(user_filename,users)
        users = read_user_transactions(users)

        check_new_deposits(users)
        check_new_withdrawals(users)

        # create list of pairs to be checked and find the max (for length of candle series) and min minutes (for candle periods)
        pairs = set()
        last_min_minutes = min_minutes
        last_max_minutes = max_minutes
        for user in users.keys():
            for pair in users[user]['pairs']:
                pairs.add(pair)
                
                # check minimum periods 
                for strategy in users[user]['pairs'][pair]['strategy']:
                    
                    i = strategy[strategy.find('strategy')+8:]
                    n_minutes, n_periods = apply_strategy(i,False,{})
                    if n_minutes < min_minutes:
                        min_minutes = n_minutes
                        print("min_minutes = ", min_minutes)
                    if n_periods*n_minutes > max_minutes:
                        max_minutes = n_periods*n_minutes
                        print("max_minutes = ", max_minutes)

        # create preliminary list from previous data
        if last_max_minutes != max_minutes or last_min_minutes != min_minutes:
            print("last times not equal to maxes")
            
            # now is set to the time of the end of the last minute
            now = math.floor(datetime.timestamp(datetime.now())/60)*60
            last_time = now

        # continue if min_minutes has not elapsed since last check. min_minutes is added to the last time in the conditional to ensure that there will be enough time elapsed to get a full candle. 
        if datetime.timestamp(datetime.now()) < last_time: #+ 60*min_minutes: 
            time.sleep(1)
            continue

        # loop until reaching current time step. min_minutes is added to the last time in the conditional to ensure that there will be enough time elapsed to get a full candle.  
        if datetime.timestamp(datetime.now()) > last_time:
            candles = check_candles_hist(min_minutes,max_minutes,last_time - 60*max_minutes,pair)
            last_time = datetime.timestamp(datetime.now()) + 60*min_minutes # increase the time by a time step
            for idx, t in enumerate(candles[pair]['end']):
                pass
                
        # loop through pairs and apply strategies
        for pair in pairs:

            # find strategies for this pair
            strategies = []
            for user in users.keys():
                if pair in users[user]['pairs']:
                    for strategy in users[user]['pairs'][pair]['strategy']:
                        if strategy not in strategies:
                            strategies.append(strategy)

            for idx_strategy, strategy in enumerate(strategies):
            
                # buy or sell?
                i = strategy[strategy.find('strategy')+8:]
                buy_or_sell, _ = apply_strategy(i,True,candles[pair])
                if buy_or_sell == 1:
                    print('Trigger to buy')
                elif buy_or_sell == -1:
                    print('Trigger to sell')
                if buy_or_sell == 0:
                    print('No trigger ', datetime.now())
                    continue
                    
                # check price in book
                books = check_book(pair)

                # how much.  amount is in terms of the currently held crypto
                total_amount, user_proportions = calculate_trade_amount_simple(users,pair,strategy,buy_or_sell) 
                print("total_amount = ",total_amount)
                if total_amount <= 0:
                    continue

                # submit orders starting at 1% over the buy price or 1% below the sell price.  If the order is instantly matched to an existing order and is thus cancelled, close gap between best buy or sell price by 0.1% increments until reaching best price.  If none of the 10 orders sticks or 30 seconds has elapsed, create market order.
                post_count = 40
                traded_amount = 0
                traded_price = 1
                minimums = minimum_sizes()
                while post_count >= 0:

                    if buy_or_sell == 1:
                        price = books[0][0]*(1 + 0.0001*post_count)
                        trading_amount = total_amount/price
                    elif buy_or_sell == -1:
                        price = books[1][0]*(1 - 0.0001*post_count)
                        trading_amount = -total_amount
                    
                    if trading_amount == 0:
                        continue
                        
                    # minimum trade sizes
                    if abs(trading_amount) < float(list(filter(lambda crypto_pair: crypto_pair['pair'] == pair.lower(), minimums))[0]['minimum_order_size']):
                        continue

                    r = submit_order("t"+pair,price,trading_amount,True,True)
                    if r[6] != 'SUCCESS':
                        continue
                    order_id = r[4][0][0]
                    print("r submit = ",r)
                    print("order_id = ",order_id)

                    # ensure that the order has been received before
                    time.sleep(1)

                    # find if order was post only cancelled
                    r = order_history(order_id)
                    print("r history = ",r)

                    if not r:
                        break
                        
                    if r[0][13] != 'POSTONLY CANCELED':
                        execution = r[0][13]
                        """
                        if "PARTIALLY FILLED" in execution:
                            amount, avg_price = analayze_partial_order(execution)
                        else:
                            amount = r[0][7]
                            avg_price = r[0][17]
                        """
                        amount = r[0][7]
                        avg_price = r[0][17]

                        traded_amount += amount
                        traded_price += avg_price*abs(amount)

                        # update balances
                        write_trade(order_id,pair,users,user_proportions)

                        break

                    post_count -= 1

                # if order was postonly cancelled, place final limit order at best price
                if r:
                    if r[0][13] == 'POSTONLY CANCELED':
                        books = check_book(pair)

                        if buy_or_sell == 1:
                            price = books[0][0]
                            trading_amount = total_amount/price
                        elif buy_or_sell == -1:
                            price = books[1][0]
                            trading_amount = -total_amount

                        r = submit_order("t"+pair,price,trading_amount,True,False)
                        if r[6] != 'SUCCESS':
                            continue
                        order_id = r[4][0][0]
                        print("r submit = ",r)
                        print("order_id = ",order_id)

                        r = order_history(order_id)
                        print("order_history = ", r)

                        if r:
                            execution = r[0][13]
                            if "PARTIALLY FILLED" in execution:
                                amount, avg_price = analayze_partial_order(execution)
                            else:
                                amount = r[0][7]
                                avg_price = r[0][17]

                            traded_amount += amount
                            traded_price += avg_price*abs(amount)

                            # update balances
                            write_trade(order_id,pair,users,user_proportions)

                # if order is active, sleep up to 30 seconds
                if not r:
                    sleep_time = 0
                    while sleep_time < 30:
                        print("sleep time = ",sleep_time)
                        time.sleep(2)
                        
                        # if not active orders, break
                        r = retrieve_orders(order_id)
                        print("retrieve_orders = ", r)
                        if not r:
                            r = order_history(order_id)
                            print("order_history = ", r)

                            execution = r[0][13]
                            if "PARTIALLY FILLED" in execution:
                                amount, avg_price = analayze_partial_order(execution)
                            else:
                                amount = r[0][7]
                                avg_price = r[0][17]

                            traded_amount += amount
                            traded_price += avg_price*abs(amount)

                            # update balances
                            write_trade(order_id,pair,users,user_proportions)

                            r = []

                            break

                        sleep_time += 2

                # if only got post only canceled or not fully traded, put market order
                r = order_history(order_id)
                print("order_history = ", r)
                print("traded_amount, trading_amount", traded_amount, trading_amount)
                if abs(trading_amount) > 0 and abs(traded_amount) < abs(trading_amount):

                    # cancel orders that did not go through
                    if not r or 'EXECUTED' not in r[0][13]:
                        cancel_order(order_id)

                    print('submitting market order')

                    # if buy, use price of best sell
                    if buy_or_sell == 1:
                        price = books[1][0]
                        trading_amount = total_amount/price - traded_amount/traded_price
                    elif buy_or_sell == -1:
                        price = books[0][0]
                        trading_amount = -total_amount - traded_amount

                    r = submit_order("t"+pair,price,trading_amount,False,False)
                    if r[6] != 'SUCCESS':
                        continue
                    order_id = r[4][0][0]
                    print("r submit = ",r)
                    print("order_id = ",order_id)

                    time.sleep(1)
                    r = order_history(order_id)
                    print("order_history = ", r)

                    execution = r[0][13]
                    if "PARTIALLY FILLED" in execution:
                        amount, avg_price = analayze_partial_order(execution)
                    else:
                        amount = r[0][7]
                        avg_price = r[0][17]

                    traded_amount += amount
                    traded_price += avg_price*abs(amount)

                    print("amount, traded_amount", amount, traded_amount)
                    print("avg_price, price", avg_price, price)

                    # update balances
                    write_trade(order_id,pair,users,user_proportions)

                # save info
                if abs(traded_amount) > 0:
                    traded_price /= abs(traded_amount)
                else:
                    continue
                print("average price, total amount", traded_price, traded_amount)