import sys
import pandas as pd
from datetime import datetime
import time
import csv
import os

def find_total_price(amount,amounts,prices):
    """
    Given the amount of the sale, this returns the agglomerate buy price and returns this price plus the modified vectors having the sale amount removed from them.
    Inputs: amount = sell amount, amounts = all transaction amounts for this crypto, prices = prices for the crypto on transaction day in euros.
    Outputs: total_price = agglomerate price for buy amount corresponding to "amount", amounts = modified list of all transaction amounts for this crypto removing the sell amounts and used up buy amounts, prices  = modified list of all transaction prices for this crypto removing the sell prices and used up buy prices
    """
    total_amount, total_price, previous_amount = 0, 0, 0
    idx = 0
    sales = []
    amount = abs(amount)
    
    # loop until sell amount has been reached by summing up buy amounts.
    while total_amount < amount and len(amounts) > 0:
        # remove sells
        if amounts[0] <= 0:
            amounts.pop(0)
            prices.pop(0)
            continue 
        # add buy to agglomeration
        if total_amount + amounts[0] <= amount:
            total_amount += amounts[0]
            total_price += amounts[0]*prices[0]
            amounts.pop(0)
            prices.pop(0)
            previous_amount = total_amount # traces the previous total amount
        # add part of buy to agglomeration so that sell amount is equal to buy amount
        else:
            total_amount = amount
            total_price += (total_amount - previous_amount)*prices[0]
            amounts[0] -= (total_amount - previous_amount)

    # calculate price of agglomerated buy
    if total_amount > 0:
        total_price /= total_amount
    else:
        print("Error: must have forgotten to add a deposit to the input files. total_price = ", total_price, "total_amount = ", total_amount)
        sys.exit()
        total_price = 1
        
    return total_price, amounts, prices

def create_sales(dates,ttypes,amounts,prices,fees,totals):
    """
    Create sales list for writing to file
    Inputs: dates = dates of each sale, ttypes = transaction types, amounts = amount of each transaction, prices = price of each transaction, fees = fee of each transaction, totals = total of each transaction.
    Outputs: sales = list of each sale including sale date, sale amount, sale price, buy price, buy amount in euros , sale amount in euros, gains
    """
    sales = []
    dummy_amounts = amounts[:]
    dummy_prices = prices[:]
    for idx, amount in enumerate(amounts):
        if ttypes[idx] == 'Venta' and amount < 0:  # if it is a sell
            remaining_amounts = dummy_amounts[idx:]
            remaining_prices = dummy_prices[idx:]
            dummy_amounts = dummy_amounts[:idx]
            dummy_prices = dummy_prices[:idx]
            total_price, dummy_amounts, dummy_prices = find_total_price(amount,dummy_amounts,dummy_prices)   
            dummy_amounts = dummy_amounts + remaining_amounts
            dummy_prices = dummy_prices + remaining_prices
            sales.append([ dates[idx], amount, total_price, prices[idx], -amount*total_price, totals[idx], totals[idx] + amount*total_price])

    return sales

def create_coinbase_dataframe(fn):
    """
    Create dataframe from Coinbase file.
    Inputs: fn = filename for Coinbase transactions
    Outputs: dfCoinbase = dataframe with Coinbase transactions
    """
    dfCoinbase = pd.read_csv(fn)
    dfCoinbase['Timestamp'] = dfCoinbase['Timestamp'].str.replace("T",' ')
    dfCoinbase['Timestamp'] = dfCoinbase['Timestamp'].str.replace("Z",' ')

    return dfCoinbase

def create_bitfinex_dataframe(fn):
    """
    Create dataframe from Bitfinex file.
    Inputs: fn = filename for Bitfinex transactions
    Outputs: dfBitfinex = dataframe with Bitfinex transactions
    """
    dfBitfinex = pd.read_csv(fn)
    return dfBitfinex

def convert_ticker(ticker):
    """
    Convert 3-letter tickers to 4-letter tickers and vice versa
    Inputs: ticker = ticker either in 3- or 4-letter format
    Outputs: complementary ticker
    """
    if ticker == 'UST':
        return  'USDT'
    elif ticker == 'USDT':
        return 'UST'
    elif ticker == 'VSY':
        return 'VSYS'
    elif ticker == 'VSYS':
        return 'VSY'
    elif ticker == 'AMP':
        return 'AMPL'
    elif ticker == 'AMPL':
        return 'AMP'
    elif ticker == 'QTM':
        return 'QTUM'
    elif ticker == 'QTUM':
        return 'QTM'
    elif ticker == 'DAT':
        return 'DATA'
    elif ticker == 'DATA':
        return 'DAT'
    elif ticker == 'GNT':
        return 'GNTO'
    elif ticker == 'GNTO':
        return 'GNT'
    elif ticker == 'IOT':
        return 'MIOTA'
    elif ticker == 'MIOTA':
        return 'IOT'
    elif ticker == 'ALG':
        return 'ALGO'
    elif ticker == 'ALGO':
        return 'ALG'
    elif ticker == 'DSH':
        return 'DASH'
    elif ticker == 'DASH':
        return 'DSH'
    elif ticker == 'XCH':
        return 'XCHF'
    elif ticker == 'XCHF':
        return 'XCH'
    elif ticker == 'QSH':
        return 'QASH'
    elif ticker == 'QASH':
        return 'QSH'
    elif ticker == 'MNA':
        return 'MANA'
    elif ticker == 'MANA':
        return 'MNA'
    else:
        return ticker

def read_prices(crypto):
    """
    Takes crypto name in uppercase letters and returns price data.
    Inputs: crypto = crypto name
    Outputs: df = dataframe with price data in euros for each day of that crypto
    """   
    # define file name
    if crypto != "USD" and crypto != "JPY" and crypto != "GBP":
        fn = crypto.lower() + "-eur-max.csv"
    else:
        if crypto == "USD":
            fn = "EUR_USD_HistoricalData.csv"
        elif crypto == "JPY":
            fn = "EUR_JPY_HistoricalData.csv"
        elif crypto == "GBP":
            fn = "EUR_GBP_HistoricalData.csv"

    # read dataframe
    df = pd.read_csv(fn)
   
    # clean up dataframe and sort
    if crypto != "USD" and crypto != "JPY" and crypto != "GBP":
        df = df.rename(columns={"snapped_at": "Timestamp"})
    else:
        df = df.rename(columns={"Date": "Timestamp"})
        df = df.rename(columns={"Price": "price"})
        df['price'] = 1.0/df['price']

    df.Timestamp=pd.to_datetime(df.Timestamp).dt.strftime('%Y-%m-%d')
    df.sort_values(by=['Timestamp'], inplace=True)

    return df

def find_price(df_prices,date):
    """
    Takes prices data and date to be checked, and returns price in euros of that crypto on that day.  If that day is not available, it returns the next day that is available.
    Inputs: df_prices = dataframe with prices of crypto for each day, date = date that we wish to analyze
    Outputs: price at that date for the specified price dataframe.  
    """   
    # find row of date that is the same or the closest date after
    d = df_prices.loc[df_prices["Timestamp"] >= date]

    return d['price'].iloc[0]

if __name__ == "__main__":

    start_date = "2018-01-01"
    end_date = "2020-12-31"

    # remove output file from previous runs
    if os.path.exists("transacciones.csv"):
        os.remove("transacciones.csv")

    # load transactions from Coinbase
    dfCoinbase = create_coinbase_dataframe('CoinbaseTransactions-2021-05-09-16_14_47.csv')

    # load transactions from Bitfinex
    df = create_bitfinex_dataframe("ebache_trades_FROM_Sun-Dec-31-2017_TO_Wed-Dec-30-2020_ON_2021-05-09T12-29-15.226Z.csv")
    df = df.rename(columns={"DATE": "Timestamp"})
    df['Timestamp'] = pd.to_datetime(df['Timestamp'], format='%y-%m-%d %H:%M:%S').dt.strftime('%Y-%m-%d %H:%M:%S')

    # sort by order ID
    df.sort_values(by=['#'], inplace=True)

    # add Coinbase transactions to Bitfinex dataframe
    df = pd.concat([ df, dfCoinbase] ,ignore_index=True)

    # load transactions from Binance
    # None for 2020!!!

    # load transactions from Bitmax
    # None for 2020!!!

    # load transactions from Bybit
    # None for 2020!!!

    # load transactions from Metamask
    # None for 2020!!!

    # sort and clean up dataframe
    df.sort_values(by=['Timestamp'], inplace=True)
    df.drop(['#', 'FEE PERC','ORDER ID'], axis=1)
    df = df[df['Timestamp'].between(start_date, end_date)]

    df.Timestamp=pd.to_datetime(df.Timestamp).dt.strftime('%Y-%m-%d')

    # create list of all currencies in 3-letter format
    cryptos = list(df.Asset.dropna().unique())
    for idx, crypto in enumerate(cryptos):
        cryptos[idx] = convert_ticker(crypto)
    pairs = df.PAIR.dropna().unique()
    for pair in pairs:
        if pair[:3] not in cryptos:
            cryptos.append(pair[:3])
        if pair[4:] not in cryptos:
            cryptos.append(pair[4:])

    # create list of all 2nd currencies in pairs (needed to store these cryptos prices continually)
    stable_cryptos = {}
    for pair in pairs:
        if pair[4:] != 'EUR' and pair[4:] not in stable_cryptos:
            stable_cryptos[pair[4:]] = read_prices(convert_ticker(pair[4:]))

    # loop through each currency
    total_gains, total_fees = 0, 0
    for crypto in cryptos:

        if crypto == 'EUR':
            continue

        unstable_cryptos = {}

        print("crypto = ",crypto)
        
        # read daily prices for this crypto if not already in stable_cryptos
        if crypto not in stable_cryptos and crypto != 'EUR':
            print("reading prices for ",crypto)
            df_crypto = read_prices(convert_ticker(crypto))

        dates, ttypes, amounts, prices, fees, totals = [], [], [], [], [], []
        # loop through transaction list
        for index, row in df.iterrows():

            
            if (row.notnull()['PAIR'] and crypto in row['PAIR']) or (row.notnull()['Asset'] and row['Asset'] == convert_ticker(crypto)) or (row.notnull()['Notes'] and convert_ticker(crypto) in row['Notes']):

                dates.append(row['Timestamp'].encode("utf-8"))

                # Coinbase transactions
                if row.notnull()['Asset']:
                    transaction_type = row['Transaction Type']

                    # create lists that hold transactions that affect gains.  Receiving money is not necessary since it does not require any transaction fees
                    if transaction_type == 'Buy':
                        ttypes.append('Compra')
                        amounts.append(float(row['Quantity Transacted']))
                        prices.append(float(row['EUR Spot Price at Transaction']))
                        fees.append(-float(row['EUR Fees']))
                        totals.append(-float(row['EUR Subtotal']))
                    if transaction_type == 'Sell':
                        ttypes.append('Venta')
                        print("Error, sell in Coinbase is not yet implemented")
                        sys.exit()
                    if transaction_type == 'Send':
                        ttypes.append('Envio')
                        amounts.append(-float(row['Quantity Transacted']))
                        prices.append(float(row['EUR Spot Price at Transaction']))
                        if row.notnull()['EUR Fees']:
                            fees.append(-float(row['EUR Fees']))
                        else:
                            fees.append(0)
                        totals.append(0)
                    if transaction_type == 'Receive':
                        ttypes.append('Recepcion')
                        amounts.append(float(row['Quantity Transacted']))
                        prices.append(float(row['EUR Spot Price at Transaction']))
                        fees.append(0)
                        totals.append(0)
                    if transaction_type == 'Convert':
                        text = row['Notes'].split(' ')
                        i = text.index(crypto)
                        if i == 2:
                            ttypes.append('Venta')
                            amounts.append(-float(row['Quantity Transacted']))
                            prices.append(float(row['EUR Spot Price at Transaction']))
                            fees.append(-float(row['EUR Fees']))
                            totals.append(float(row['EUR Total (inclusive of fees)']))
                        elif i == 5:
                            ttypes.append('Compra')
                            amounts.append(float(text[i-1]))
                            prices.append(float(row['EUR Subtotal']/amounts[-1])) # the cost of crypto1 after fees / quantity of crypto2
                            fees.append(0) # the fees are applied to crypto1
                            totals.append(-float(row['EUR Subtotal'])) # same as the cost of crypto1
                        else:
                            print("Converted text is faulty")
                            sys.exit()
                
                # Bitfinex transactions
                if row.notnull()['PAIR']:
                    pair = row['PAIR'] 
                    if crypto == pair[:3]:
                        amounts.append(float(row['AMOUNT']))
                        if amounts[-1] > 0:
                            ttypes.append('Compra')
                        else:
                            ttypes.append('Venta')
                        if pair[4:] == 'EUR':
                            prices.append(float(row['PRICE']))
                        elif pair[:3] == 'EUR':
                            prices.append(1)
                        else:
                            prices.append(float(row['PRICE'])*find_price(stable_cryptos[pair[4:]],dates[-1]))
                    else:
                        amounts.append(-float(row['AMOUNT'])*float(row['PRICE']))
                        if amounts[-1] > 0:
                            ttypes.append('Compra')
                        else:
                            ttypes.append('Venta')
                        if pair[4:] == 'EUR':
                            prices.append(1)
                        elif pair[:3] == 'EUR':
                            prices.append(float(row['PRICE']))
                        else:
                            prices.append(find_price(stable_cryptos[pair[4:]],dates[-1]))
                    if row['FEE CURRENCY'] != "EUR":
                        if row['FEE CURRENCY'] not in stable_cryptos:
                            if crypto != row['FEE CURRENCY']:
                                if row['FEE CURRENCY'] not in unstable_cryptos:
                                    print("reading prices for ",row['FEE CURRENCY'])
                                    unstable_cryptos[row['FEE CURRENCY']] = read_prices(convert_ticker(row['FEE CURRENCY']))
                                df_price = unstable_cryptos[row['FEE CURRENCY']]
                            else:
                                df_price = df_crypto
                        else:
                            df_price = stable_cryptos[row['FEE CURRENCY']]
                        fees.append(float(row['FEE'])*find_price(df_price,dates[-1]))
                    else:
                        fees.append(float(row['FEE']))

                    # only apply fees once to Buy
                    if ttypes[-1] == 'Venta':
                        fees[-1] = 0

                    totals.append(-amounts[-1]*prices[-1])

        print("sum totals for ",crypto," = ",sum(totals))

        # open output file
        with open('transacciones.csv', 'a') as f:
            writer = csv.writer(f)

            # print all transactions for this crypto
            writer.writerow(['Todas_las_transacciones_con_'+crypto])
            writer.writerow(["Fecha_de_transaccion", "Tipo_de_transaccion", "Cantidad", "Precio", "Comisiones_en_euros"])
            for idx, i in enumerate(dates):
                writer.writerow([ dates[idx],ttypes[idx],abs(amounts[idx]),prices[idx],abs(fees[idx])])
                total_fees += abs(fees[idx])

            # print sales for this crypto
            sales = create_sales(dates,ttypes,amounts,prices,fees,totals)
            writer.writerow("")
            writer.writerow(['Todas_las_ventas_y_sus_beneficios_'+crypto])
            writer.writerow(["Fecha_de_venta", "Cantidad_de_venta", "Precio_de_compra", "Precio_de_venta", "Cantidad_de_compra_en_euros", "Cantidad_de_venta_en_euros", "Beneficios"])
            gains = 0
            for idx, i in enumerate(sales):
                writer.writerow(sales[idx])
                gains += sales[idx][6]
            
            print("sum gains for ",crypto," = ",gains)

            writer.writerow("")
            writer.writerow("")

        total_gains += gains
        print("sum total gains = ",total_gains)

    with open('transacciones.csv', 'a') as f:
        writer = csv.writer(f)

        # print all transactions for this crypto
        writer.writerow(["total_beneficios", total_gains])
        writer.writerow("")
        writer.writerow(["total_comisiones", total_fees])
