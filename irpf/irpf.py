import csv
from datetime import datetime
from datetime import date
from typing import Dict
from collections import defaultdict

# global variables
##################
is_debug = False
year = 2024
last_years_declaration = datetime(2023, 4, 29)
this_years_declaration = datetime.now()
beginning_of_time_year = 2021
file_name = str(year) + '/mim - tx.csv'
file_name_next_year = str(year) + '/mim2025 - tx.csv'
file_name_bitget = str(year) + '/bitget.csv'
#file_name = "C:/Users/ellio/Downloads/mim - tx.csv"
#file_name = str(year) + '/cryptos.csv'
is_fifo = True

old_coins = ['USD','ETH','USDC','USDT','BNB','SOL','BUSD','TOK','VLX','Deeznuts','METIS','LEAFTY','B20','SPI','PAINT','XED','DDIM','RLY','Minifootball','KISHU','Highrise','ERN','GLMR','LOOKS','YUZU','BTC','SOL','EUR','OVR','XYM','BLOK','EWT','ENJ','KKT','GMS','IOT','DPR','STUD','Cryptoshack','Highrise creature','Osiris cosmic kids','ChubbyKaiju','Bridge over troubled water','POLP','KSM','CRO']

# CoinGecko query headers
headers = {"accept": "application/json","x-cg-demo-api-key": ""}

# functions
###########
def read_csv(file_name):
    """
    This will read a csv file
    Input: file_name = the file name including extension
    Output: a list of lists containing all transaction data and headers
    """
    import csv

    data = list(csv.reader(open(file_name)))

    return data

def clean_data(data):
    """
    Removes:
    1.  The first line of the data that contains the column headers
    2.  The line that contains the text 'active management'
    Input: data = list of lists containing all transaction data
    """
    # remove header
    data = data[1:]

    # remove active management line
    i = 0
    while data[i][0] != 'active management':
        i = i + 1
    del data[i]

    return data

def create_sales(data):
    """
    This is the principal function.  It loops through each row of the data and does the following:
    1.  Skips unnecessary rows.
    2.  Keeps track of 'current' balances whenever a buy occurs.
    3.  Whenever a sale occurs, calculate the buy price and add the sale and purchase price with quantities and fees to dedicated lists.
    After looping, we remove from sales those that were at a loss and where a repurchase occured within two months (until they are sold)
    We then calculate the same from the previous year and check if those repurchases have been sold.  If so, those losses are carried over to this year.
    Input: data = list of lists containing all transaction data
    """

    from datetime import datetime

#    futures_sales2 = parse_bitget_file2(file_name_bitget)
    futures_sales = read_bitget_csv(file_name_bitget)
    for key in futures_sales:
        futures_sales[key].reverse()

    min_prices, buys, last_years_buys, sales, all_sales, last_years_sales, carry_over, fees, balances, last_years_balances = {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
    idx_futures = 0
    for idx, row in enumerate(data):
        
        # define variables for this row
        ticker = row[2].rstrip()

        # define amount bought or sold.  Positive is purchase
        amount = float(row[3])

        # if fee is incorrectly listed as a positive amount, change to negative
        if ticker[:3] == 'fee' and amount > 0:
            amount = -amount

        # define wallet, chain, and notes variables
        wallet = row[8]
        chain = row[9]
        notes = row[11]
        
        """
        # check if futures
        if 'Futures' in notes:
            is_futures = True
        else:
            is_futures = False
        """
        is_futures = False

        # set purchase price.  If no purchase price is available, is_price is False
        is_price, price = set_price(is_futures,ticker,amount,min_prices,row)

        # skip internal transfer rows
        if ('-' in wallet and chain != "Hedera") or '-' in chain:
            # check that we don't have a transfer with prices.  A transfer with prices directly preceded by a transfer w/o prices would be the corresponding transfer fees & would be valid
            if (row[4] or row[5]) and idx > 0 and data[idx-1][8] != wallet:
                print("We shouldn't have a transfer with sell prices.", row)
                exit()
            continue

        # update min prices.  This min price is used in cases where we don't have a buy price.  FIFO min price is defind in reduce_balances
        if is_price:
            min_prices = update_min_prices(ticker,price,min_prices)

        # update balances
        if ticker[:3] == 'fee':
            bticker = ticker[3:]
        else:
            bticker = ticker

        beginning_of_time = datetime(beginning_of_time_year, 1, 1)
        begin = datetime(year, 1, 1)
        end = datetime(year, 12, 31)
        last_year_end = datetime(year-1, 12, 31)

        # update balances unless futures (except their fees) 
        if not is_futures:

            """
            # this is where we did not consider sales closing repurchases after year end
            if datetime.strptime(row[0], '%d-%m-%Y') <= last_year_end:
                buy_price, buy_idx, last_years_balances = update_balances(is_futures,idx,bticker,price,amount,min_prices,last_years_balances,row)


            if datetime.strptime(row[0], '%d-%m-%Y') <= end:
                buy_price, buy_idx, balances = update_balances(is_futures,idx,bticker,price,amount,min_prices,balances,row)
            """
            # this is where we consider sales closing repurchases after year end and before declaration date
            if datetime.strptime(row[0], '%d-%m-%Y') <= last_years_declaration:
                buy_price, buy_idx, last_years_balances = update_balances(is_futures,idx,bticker,price,amount,min_prices,last_years_balances,row)

            if datetime.strptime(row[0], '%d-%m-%Y') <= this_years_declaration:
                buy_price, buy_idx, balances = update_balances(is_futures,idx,bticker,price,amount,min_prices,balances,row)

        # if we have transferred to someone else, then no need to look at sales
        if 'Sent' in row[10]:
            continue

        # only look at rows that are sales and are from the current year
        if amount < 0 and datetime.strptime(row[0], '%d-%m-%Y') <= end and datetime.strptime(row[0], '%d-%m-%Y') >= beginning_of_time:

            if not is_price:
                price = buy_price

            if not is_futures:

                # add sale to list of sales
                all_sales = add_sale(is_futures,idx,bticker,price,amount,buy_idx,row,all_sales)

                if datetime.strptime(row[0], '%d-%m-%Y') <= last_year_end:

                    # add sale to list of sales
                    last_years_sales = add_sale(is_futures,idx,bticker,price,amount,buy_idx,row,last_years_sales)

                if datetime.strptime(row[0], '%d-%m-%Y') >= begin:

                    # add sale to list of sales
                    sales = add_sale(is_futures,idx,bticker,price,amount,buy_idx,row,sales)

                    if ticker[:3] == 'fee':
                        # add fee to list of fees
                        fees = add_fee(ticker,amount,row,fees)

            """
            # if futures, update sales and fees and continue
            else:

                if datetime.strptime(row[0], '%d-%m-%Y') >= begin:

                    futures_sales = add_sale(is_futures,idx,bticker,price,amount,buy_idx,row,futures_sales)
            """
        
    """
    print(futures_sales, "\n")
    print(futures_sales2)
    """

    # for current year's sales minus those where the 2-month rule applies
    sales, reduced_losses = reduce_losses(year,balances,sales)

    # for sales up to last year minus those where the 2-month rule applies
    last_years_sales, _ = reduce_losses(year-1,last_years_balances,last_years_sales)
#    last_years_sales, _ = reduce_losses(year-1,balances,last_years_sales)

    # for sales up to this year minus those where the 2-month rule applies
    all_sales, _ = reduce_losses(year,balances,all_sales)

    # carry over losses from previous years
    carry_over = carry_over_losses(last_year_end,last_years_sales, all_sales)

    # remove indices from sales, reduced_losses, and carry_over
    sales_write, futures_sales_write, reduced_losses_write, carry_over_write = {}, {}, {}, {}
    for key in sales:
        if key not in sales_write:
            sales_write[key] = []
        for sale in sales[key]:
            sales_write[key].append(sale[2:])
    for key in futures_sales:
        if key not in futures_sales_write:
            futures_sales_write[key] = []
        for sale in futures_sales[key]:
            futures_sales_write[key].append(sale[2:])
    """
    for key in futures_sales:
        if key not in futures_sales_write:
            futures_sales_write[key] = []
        for sale in futures_sales[key]:
            futures_sales_write[key].append(sale[2:])
    """
    for key in reduced_losses:
        if key not in reduced_losses_write:
            reduced_losses_write[key] = []
        for reduced_loss in reduced_losses[key]:
            reduced_losses_write[key].append(reduced_loss[2:])
    for key in carry_over:
        if key not in carry_over_write:
            carry_over_write[key] = []
        for sale in carry_over[key]:
            carry_over_write[key].append(sale[2:])

    # write csv file with gains and fees
    write_output_file(sales_write,futures_sales_write,fees,reduced_losses_write,carry_over_write)

    return 0

def merge_files(existing_data, second_filename):
    """
    Takes existing data from Google sheets and adds Bitget futures data to it
    Inputs: existing_data = data from Google sheets (e.g. 'mim - Tx.csv') in list of lists; second_filename = Bitget futures export
    Outputs: merged_data = new list of lists with the merged data
    """
    import time

    # get exchange rates for dates
    rates = read_exchange_file(file_name)

    new_data = []
    # Parse second file
    with open(second_filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                closed_time = datetime.strptime(row['Closed time'], '%Y-%m-%d %H:%M:%S')
                closed_date = closed_time.strftime('%d-%m-%Y')  # same format as first file

                # find dollar to euro rate for this date
                closed_rate = get_exchange_rate_for_date(closed_date, rates)

                # calculate USDT gains
                gain = float(row['Realized PnL'].replace('USDT', '').strip())

                # if gains are positive then declare as USDT purchase, otherwise sale
                new_row = [closed_date]  # start with the date
                if gain > 0:
                    new_row += [closed_rate,'USDT',gain,'','',1,closed_rate,'Bitget','','','Futures USDT profit','','','']
                elif gain < 0:
                    new_row += [closed_rate,'USDT',gain,1,closed_rate,'?','?','Bitget','','','Futures USDT loss','','','']

                if gain != 0:
                    new_data.append(new_row)

                # calculate fees
                fee = float(row['Position Pnl'].replace('USDT', '').strip())- float(row['Realized PnL'].replace('USDT', '').strip())

                # only add negative fees.  Otherwise declare as non-fee
                new_row = [closed_date]  # start with the date
                if fee < 0:
                    new_row += [closed_rate,'feeUSDT',fee,1,closed_rate,'?','?','Bitget','','','Futures fee','','','']
                elif fee > 0:
                    new_row += [closed_rate,'USDT',fee,'','',1,closed_rate,'Bitget','','','Futures positive fee (gain)','','','']

                if fee != 0:
                    new_data.append(new_row)

            except Exception as e:
                print(f"Skipping row due to error: {e}")
                continue

    # Add a tag so we know which source the row came from
    tagged_existing = [(datetime.strptime(row[0], '%d-%m-%Y'), 0, row) for row in existing_data]
    tagged_new = [(datetime.strptime(row[0], '%d-%m-%Y'), 1, row) for row in new_data]
    
    # Merge
    combined = tagged_existing + tagged_new
    combined.sort(key=lambda x: (x[0], x[1]))  # sort by date first, then source (existing first)

    # Return only the data (not the date and tag)
    merged_data = [row for _, _, row in combined]
    
    return merged_data

def read_exchange_file(filename: str) -> Dict[datetime, float]:
    exchange_rates = {}
    with open(filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        for row in reader:
            try:
                date = datetime.strptime(row['Date'], '%d-%m-%Y')
                rate = float(row['USD/EUR'])
                exchange_rates[date] = rate
            except (ValueError, KeyError):
                continue  # skip malformed rows
    return exchange_rates

def get_exchange_rate_for_date(target_date: str, rates: Dict[datetime, float]) -> float:
    target = datetime.strptime(target_date, '%d-%m-%Y')
    closest_date = min(rates.keys(), key=lambda d: abs(d - target))
    return rates[closest_date]


def read_bitget_csv(filename):

    rates = read_exchange_file(file_name)

    result = defaultdict(list)

    with open(filename, newline='', encoding='utf-8') as csvfile:
        reader = csv.DictReader(csvfile)
        
        for row in reader:
            # Extract ticker and position type
            futures = row['Futures']
            if 'USDT' not in futures:
                continue
            ticker = futures.split('USDT')[0]
            is_short = 'short' in futures.lower()

            # Format dates
            open_date = datetime.strptime(row['Opening time'], '%Y-%m-%d %H:%M:%S').strftime('%d-%m-%Y')
            close_date = datetime.strptime(row['Closed time'], '%Y-%m-%d %H:%M:%S').strftime('%d-%m-%Y')

            # check if the future was closed in this fiscal year
            begin = datetime(year, 1, 1)
            end = datetime(year, 12, 31)
            date = datetime.strptime(row['Closed time'], '%Y-%m-%d %H:%M:%S')
            if date < begin or date > end:
                continue

            # Rates
            open_rate = get_exchange_rate_for_date(open_date, rates)
            close_rate = get_exchange_rate_for_date(close_date, rates)

            # Parse prices
            entry_price = float(row['Average entry price'])
            close_price = float(row['Average closing price'])

            buy_price = close_price if is_short else entry_price
            sale_price = entry_price if is_short else close_price

            # Closed amount without the ticker symbol (e.g., '39SOL' -> '39')
            closed_amount = ''.join(c for c in row['Closed amount'] if c.isdigit() or c == '.')

            # Parse PnL
            position_pnl = float(row['Position Pnl'].replace('USDT', '').strip())

            # Construct row
            entry = [0, 0, open_date, close_date, ticker, float(closed_amount), buy_price*float(open_rate), sale_price*float(close_rate), -float(closed_amount)*(buy_price*float(open_rate)-sale_price*float(close_rate))]

            # Append to ticker group
            result[ticker].append(entry)

    return dict(result)

def parse_bitget_file2(file_path):

    import csv

    result = {}

    with open(file_path, mode='r', encoding='utf-8') as file:
        reader = csv.DictReader(file)
        
        for row in reader:
            futures_field = row['Futures']
            ticker = futures_field.split('USDT')[0]
            position_type = 'short' if 'Short' in futures_field.lower() else 'long'

            opening_date = row['Opening time']
            closing_date = row['Closed time']
            position_pnl = row['Position Pnl'].replace('USDT', '').strip()
            closed_amount = row['Closed amount'].rstrip(ticker)
            avg_entry = float(row['Average entry price'])
            avg_close = float(row['Average closing price'])

            # Determine buy and sale prices based on position type
            if position_type == 'short':
                buy_price = avg_close
                sale_price = avg_entry
            else:
                buy_price = avg_entry
                sale_price = avg_close

            # Build the list entry
            entry = [
                0,
                0,
                opening_date,
                closing_date,
                ticker,
                float(closed_amount),
                buy_price,
                sale_price,
                float(position_pnl),
                0
            ]

            # Append to dictionary
            if ticker not in result:
                result[ticker] = []
            result[ticker].append(entry)

    return result


def parse_bitget_file(file_path):
    import pandas as pd
    import re

    df = pd.read_csv(file_path)
    result = {}

    for _, row in df.iterrows():
        future = row["Futures"]
        is_short = "Short" in future
        ticker = future.split("USDT")[0]

        closed_amount = re.findall(r"[\d\.]+", str(row["Closed amount"]))[0]
        closed_amount = float(closed_amount)

        avg_entry = float(row["Average entry price"])
        avg_close = float(row["Average closing price"])
        position_pnl = float(str(row["Position Pnl"]).replace("USDT", "").strip())

        buy_price = avg_close if is_short else avg_entry
        sale_price = avg_entry if is_short else avg_close

        row_data = [0, 0, ticker, closed_amount, buy_price, sale_price, position_pnl, 0]

        if ticker not in result:
            result[ticker] = []
        result[ticker].append(row_data)

    return result

def carry_over_losses(last_year_end,last_years_sales, all_sales):
    """
    We assume that all sales that were present from the beginning of time to last year should be present in the sales from the beginning of time to the present year.
    If they are not present, that means that they were reduced since they did not meet the 2-month rule.  These losses compose the carry_over list of lists 
    Inputs: last_years_sales = a list of lists with all sales that were declared up till last year, 
    all_sales = a dictionary where each ticker has a list of lists containing the ID, date, ticker, sale amount, buy price, sale price, gains, excluding those that were removed due to 2-month rule
    Outputs: carry_over = list of lists with the losses that were not declared in previous years since they met the 2-month rule, and now they no longer meet the 2-month rule
    """
    from datetime import datetime

    # create dictionary of sets with indices from last_years_sales
    last_years_sales_mapping = {}
    for key in last_years_sales:

        if len(last_years_sales[key]) > 0:
            last_years_sales_mapping[key] = dict()

        # loop through all sales for this key
        for idx, row in enumerate(last_years_sales[key]):

            # add index to dictionary
            last_years_sales_mapping[key][row[0]] = idx

    carry_over = {}
    # loop through all sales key
    for key in all_sales:

        # loop through all sales for this key
        for idx, row in enumerate(all_sales[key]):
            
            # if the sale from previous years is present in the all_sales dictionary but not last_year_sales
            if datetime.strptime(row[2], '%d-%m-%Y') <= last_year_end and (key not in last_years_sales_mapping or row[0] not in last_years_sales_mapping[key] or row not in last_years_sales[key]):

                if key not in carry_over:
                    carry_over[key] = []

                if is_debug:
                    print("\nthis row ", row, " is not in last years sales")

                # calculate gains difference between the two years and append to carry_over
                new_sale = row.copy()
                if key in last_years_sales_mapping and row[0] in last_years_sales_mapping[key]:
                    new_sale[4] = row[4] - last_years_sales[key][last_years_sales_mapping[key][row[0]]][4]
                    new_sale[7] = row[7] - last_years_sales[key][last_years_sales_mapping[key][row[0]]][7]
                carry_over[key].append(new_sale)

    return carry_over

def reduce_losses(year,balances,sales):
    """
    Old method
    ----------
    We look for positive remaining balances.  Each of these is a potential unclosed repurchase.  We then find the purchase just before the remaining balance's purchase.  
    If there was a sale between these two purchases and the remaining balance's purchase was within two months after the sale, then the loss must not be included in the IRPF.  

    New method
    ----------
    If sale at loss
        Search for unresolved purchase 2 months before or after
            Search through balances from max(2 months before,purchase date) to 2 months after
                Sum up balances until sale amount
                Remove this amount from balances
                Remove this amount from sales
                Add this amount to reduced_losses

    Inputs: year = the end year up to which the sales will be considered, balances = a dictionary for each token of dictionaries (id, date, price, and amount), each containing a list with each transaction, 
    buys = each of the purchases, 
    sales = a dictionary where each ticker has a list of lists containing the ID, date, ticker, sale amount, buy price, sale price, gains, buy_idx
    Outputs: sales = the sales original sales minus the ones don't count, reduced_losses = the undeclarable losses
    """
    from datetime import datetime
    from dateutil.relativedelta import relativedelta
    import time
    import copy

    end = datetime(year, 12, 31)

    # Create dictionary for saving losses
    reduced_losses = {}

    # loop through tickers
    for key in sales:

        """
        if is_debug and key == 'TIA':
            print("\nkey = ", key)
        """

        sales_key = list(sales[key])

        # if no sales then no problem!
        if len(sales_key) == 0:
            continue

        # if there are no balances for this key, then no problem!
        if key not in balances:
            continue

        # copy the current key's balances dict to a new variable
        balances_key = dict(balances[key])
        balances_key_idx = list(balances_key['idx'])
        balances_key_dates = list(balances_key['dates'])
        balances_key_amounts = list(balances_key['amounts'])
        balances_key_prices = list(balances_key['prices'])

        # if no remaining balances then no problem!
        if not balances_key or len(balances_key) == 0:
            continue

        # initialize index difference between sales and current sale.  We need this since we will be deleting sales while looping through a fixed list of sales
        idx_diff = 0

        # go forward through sales looking for sales at a loss with repurchase 2 months before or after
        for isale, sale in enumerate(sales_key):

            # copy list of current sale
            this_sale = list(sale)

            """
            if is_debug and key == 'DOT':
                print("\nsales[key] = ", sales[key])
                print("this_sale = ", isale, this_sale)
            """

            # only sales up till current year
            if datetime.strptime(this_sale[2], '%d-%m-%Y') > end:
                continue

            # if this sale is gain, continue
            if this_sale[7] > 0:
                continue

            # initialize loop variables
            sum_balances = 0
                
            # loop through remaining purchases 2 months before and after negative sale
            ibalance = 0
            while ibalance < len(balances_key_idx):

                # define the index of the current remaining purchase
                idx = balances_key_idx[ibalance]

                """
                if is_debug and key == 'DOT':
                    print("idx = ", idx)
                    print("balances_key = ", balances_key)
                """

                # define the date of the current remaining purchase
                date = datetime.strptime(balances_key_dates[ibalance], '%d-%m-%Y')

                if is_debug and key == 'SPX':
                    print("SPX date = ", date)
                    print("datetime.strptime(sale[1], '%d-%m-%Y') + relativedelta(months=+2) = ", datetime.strptime(this_sale[2], '%d-%m-%Y') + relativedelta(months=+2))

                # continue if the current remaining purchase is not before the purchase that led to a negative sale
                if this_sale[1] >= idx:
                    """
                    if is_debug and key == 'DOT':
                        print("this sale is too soon", this_sale[1],  idx)
                    """
                    ibalance += 1
                    continue 

                # continue if the current remaining purchase is before 2 months before the negative sale 
                if date < datetime.strptime(sale[2], '%d-%m-%Y') + relativedelta(months=-2):
                    ibalance += 1
                    continue 
                """
                # continue if the current remaining purchase is before the negative sale 
                if idx < this_sale[0]:
                    ibalance += 1
                    continue 
                """

                # continue if the current remaining purchase is after 2 months after the negative sale 
                if date > datetime.strptime(this_sale[2], '%d-%m-%Y') + relativedelta(months=+2):
                    ibalance += 1
                    continue

                """
                if is_debug and key == 'DOT':
                    print("sales[key][idx_sale] = ", sales[key][isale-idx_diff])
                    print("balances_key = ", balances_key)
                    print("balances_key['amounts'][ibalance] = ", balances_key_amounts[ibalance])
                """


                # create list for each sale in loss that is going to be reduced due to a still open purchase within 2 months
                if balances_key_amounts[ibalance] > 0:

                    # create losses list
                    if key not in reduced_losses:
                        reduced_losses[key] = []

                    # initialize losses list
                    if len(reduced_losses[key]) == 0 or reduced_losses[key][-1][0] != this_sale[0]:
                        reduced_losses[key].append(list(this_sale))
                        reduced_losses[key][-1][4] = 0
                        reduced_losses[key][-1][7] = 0

                # sum up current balance
                sum_balances += balances_key_amounts[ibalance]

                """
                if is_debug and key == 'DOT':
                    print("sum_balances = ", sum_balances)
                """

                # update sales & reduced_losses
                if sum_balances >= this_sale[4]:

                    """
                    if is_debug and key == 'DOT':
                        print("sum_balances >= this_sale[3] ",sum_balances , this_sale[4])
                    """

                    # remove sale
                    del sales[key][isale-idx_diff]
                    idx_diff += 1

                    """
                    if is_debug and key == 'DOT':
                        print("idx_diff = ", idx_diff)
                    """


                    # set the undeclared losses to reduced_losses
                    reduced_losses[key][-1][4] = this_sale[4]
                    reduced_losses[key][-1][7] = -reduced_losses[key][-1][4]*(this_sale[5] - this_sale[6])

                else:

                    # remove sales where the 2-month rule applies
                    sales[key][isale-idx_diff][4] = sales[key][isale-idx_diff][4] - balances_key_amounts[ibalance]
                    sales[key][isale-idx_diff][7] = -sales[key][isale-idx_diff][4]*(sales[key][isale-idx_diff][5] - sales[key][isale-idx_diff][6])


                    # update the undeclared losses to reduced_losses
                    reduced_losses[key][-1][4] += balances_key_amounts[ibalance]
                    reduced_losses[key][-1][7] = -reduced_losses[key][-1][4]*(this_sale[5] - this_sale[6])

                """
                if is_debug and key == 'DOT':
                    print("reduced_losses[key] = ", reduced_losses[key])
                    print("sales[key][isale-idx_diff] = ", this_sale)
                """

                if sum_balances > this_sale[4]:

                    # set the remaining balance to the sum of the balances minus the sale amount
                    balances_key_amounts[ibalance] = sum_balances - this_sale[4]

                    # cap the sum of the balances at the sale amount
                    sum_balances = this_sale[4]

                    """
                    if is_debug and key == 'DOT':
                        print("balances_key['amounts'][ibalance] = ", balances_key_amounts[ibalance])
                        print("sum_balances = ", sum_balances, "\n")
                    """

#                        time.sleep(1)


                    break

                else:

                    # remove this balance
                    del balances_key_idx[ibalance]
                    del balances_key_dates[ibalance]
                    del balances_key_amounts[ibalance]
                    del balances_key_prices[ibalance]

                """
                if is_debug and key == 'DOT':
                    print("balances_key['amounts'] = ", balances_key_amounts, "\n")
                    print("sum_balances = ", sum_balances, "\n")
                """

                """
                if key == 'TIA':
                    print("balances[key] = ", balances[key])
                """

#                    time.sleep(1)
        """
        if is_debug and key == 'DOT':
            print("balances_key = ", balances_key)
            print("sales[key] = ", sales[key], "\n")
        """
        
    return sales, reduced_losses

def write_output_file(sales,futures_sales,fees,reduced_losses,carry_over):

    import csv

    outfile = str(year) + '/txCriptos' + str(year) + '.csv'

    with open(outfile, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows([['Ventas'],['Fecha','Token','Cantidad','Precio de compra','Precio de venta','Ganancias']])

    total_gains = 0
    with open(outfile,'a') as file:
        for key in sales:
            token_gains = 0
            for i in sales[key]:

                token_gains = token_gains + i[5]
                total_gains = total_gains + i[5]

                if abs(i[5]) > 5e-3:
                    file.write(','.join([str(x) for x in i]))
                    file.write('\n')

#            file.write(key + ' total gains ' + str(token_gains) + '\n')
        """
        with open(outfile, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(sales[key])
#            writer.writerows([[],['Ganancias totales',col_totals[5]]])
        """            

    with open(outfile, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows([[],['Ganancias totales',total_gains]])
        writer.writerows([[],[],['Comisiones'],['Fecha','Token','Cantidad','Precio','Comisiones']])

    total_fees = 0
    for key in fees:
        for i in fees[key]:
            total_fees = total_fees + i[4]
        with open(outfile, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(fees[key])

    with open(outfile, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows([[],['Comisiones totales',total_fees]])
        writer.writerows([[],[],['Futuros'],['Fecha apertura','Fecha cierre','Token','Cantidad','Precio de compra','Precio de venta','Ganancias']])

    total_gains = 0
    with open(outfile,'a') as file:
        for key in futures_sales:
            token_gains = 0
            for i in futures_sales[key]:

                token_gains = token_gains + i[6]
                total_gains = total_gains + i[6]

                if abs(i[6]) > 5e-3:
                    file.write(','.join([str(x) for x in i]))
                    file.write('\n')

    with open(outfile, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows([[],['Ganancias totales futuros',total_gains]])
        writer.writerows([[],[],['Perdidas compensadas de anos anteriores'],['Fecha','Token','Cantidad','Precio de compra','Precio de venta','Ganancias']])

    total_gains = 0
    for key in carry_over:
        for i in carry_over[key]:
            total_gains = total_gains + i[5]

        with open(outfile, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(carry_over[key])
#            writer.writerows([[],['Ganancias totales',col_totals[5]]])

    with open(outfile, 'a', newline='') as f:
        writer = csv.writer(f)
        writer.writerows([[],['Perdidas compensadas totales',total_gains]])
        writer.writerows([[],[],['Perdidas declaradas pero no a efectos liquidatorios'],['Fecha','Token','Cantidad','Precio de compra','Precio de venta','Ganancias']])

    for key in reduced_losses:

        with open(outfile, 'a', newline='') as f:
            writer = csv.writer(f)
            writer.writerows(reduced_losses[key])
#            writer.writerows([[],['Ganancias totales',col_totals[5]]])

    return 0

def add_fee(ticker,amount,row,fees):

    if row[5]:
        sell_price = float(row[5])
    else:
        if row[4]:
            sell_price = float(row[4])*float(row[1])
        else:
            print("Why is there no sale price?",row)
            if not is_debug:
                exit()

    # add new ticker to fees
    if ticker[3:] not in fees:
        fees[ticker[3:]] = []

    fees[ticker[3:]].append([row[0],ticker[3:],-amount,sell_price,-amount*(sell_price)])    
    
    return fees

def add_sale(is_futures,idx,ticker,price,amount,buy_idx,row,sales):
    """
    Adds a sale to the sales dictionary.
    Inputs: idx, ..., buy_idx = index of the last purchase corresponding to a sale
    Outputs: sales = a dictionary where each ticker has a list of lists containing the ID, date, ticker, sale amount, buy price, sale price, gains, buy_idx
    """

    if row[5]:
        sell_price = float(row[5])
    else:
        if row[4]:
            sell_price = float(row[4])*float(row[1])
        else:
            print("Why is there no sale price?",row)
            if not is_debug:
                exit()

    # add new ticker to sales
    if ticker not in sales:
        sales[ticker] = []

    # don't declare stuff that is very small
    if abs(amount*(price - sell_price)) > 1e-6:
        if is_futures:
            sales[ticker].append([idx,buy_idx,row[10],row[0],ticker,-amount,price,sell_price,amount*(price - sell_price)])
        else:
            sales[ticker].append([idx,buy_idx,row[0],ticker,-amount,price,sell_price,amount*(price - sell_price)])

    return sales

def define_remaining_tokens(balances):
    """
    Takes the remaining balances and creates list of the remaining tickers
    Input: balances = a dictionary for each token of dictionaries (id, date, price, and amount), each containing a list with each transaction
    Output: a list of tickers that have remaining balances
    """
    # loop through balances and append each ticker to list
    tickers = []
    for ticker in balances:
        if ticker not in tickers:
            tickers.append(ticker)

    return tickers

def read_api():
    """
    Read the API key from file.  This must be located in the running directory.
    Inputs: -
    Outputs: the API key
    """
    file_path = 'api.txt'
    
    with open(file_path, 'r') as file:
        api_key = file.read()
    
    return api_key

def query_coingecko(url,headers):
    """
    Queries CoinGecko at the specified URL with the specified headers.
    Inputs: url = URL to be queried; headers = headers to send during the query.
    Outputs: a list of lists of the returned data
    """
    import requests
    import time

    done = False
    while not done:
        try:
            response = requests.get(url, headers=headers)
            data = response.json()
            done = True
        except ValueError:
            print("valueError = ", ValueError)
            time.sleep(60)
            continue

    return data

def define_url(query_type,query_list):
    """
    Defines the URL to be queried
    Inputs: query_type = defines what type of query we will perform ('apis'/'prices'); query_list = list of API IDs
    Outputs: URL to be queried
    """
    if query_type == 'apis':
        url = "https://api.coingecko.com/api/v3/coins/list"
    elif query_type == 'prices':
#        url = "https://api.coingecko.com/api/v3/coins/markets?vs_currency=usd&ids="
        url = "https://api.coingecko.com/api/v3/simple/price?ids="
        
        # add tokens to url string
        for token in query_list:
            url += token
            url += '%2C'

        # remove final '%2C' string
        url = url[:-3]

        url += "&vs_currencies=usd&include_market_cap=true"
    else:
        print("This query type is not supported")
        import sys
        sys.exit()

    return url

def find_api_ids(tickers):
    """
    Takes a list of tickers and converts it to a list of API IDs
    Inputs: tickers = a list of tickers that have remaining balances
    Outputs: apis = a dictionary with each API ID and its corresponding ticker, apis_list = a list of API IDs corresponding to tickers
    """
    # read API key from file
    api_key = read_api()

    # set headers for querying
    headers['x-cg-demo-api-key'] = api_key + "\t"

    # query CoinGecko for all tickers and API IDs
    url = define_url('apis',[])
    data = query_coingecko(url,headers)

    # create list with lowercase tickers
    lower_tickers = []
    for ticker in tickers:
        lower_tickers.append(ticker.lower())

    # create list of API IDs from ticker list
    apis, apis_list = {}, []
    for token in data:
        if token['symbol'] in lower_tickers and token['id'] not in apis_list:
            apis[token['id']] = token['symbol']
            apis_list.append(token['id'])

    return apis, apis_list

def find_api_prices(data,apis,apis_list):
    """
    Reduce API list removing all API IDs that are not the tokens we are looking for.  e.g. We want altair and not aircoin-on-blast for the ticker air.
     We choose the one with the biggest market cap.  We then store the price for this token.
    Inputs: data = dictionary of dictionaries with price and market cap for each API ID; apis = a dictionary with each API ID and its corresponding ticker;
     apis_list = a list of API IDs corresponding to tickers
    Outputs: a dictionary with the price for each ticker
    """
    ticker_prices, ticker_mcs = {}, {}
    # loop through each token
    for api in apis_list:

        # check symbol for each API ID
        symbol = apis[api]

        # if price has not been stored for this symbol, store price
        if symbol not in ticker_prices:
            if data[api]:
                ticker_prices[symbol] = data[api]['usd']
                ticker_mcs[symbol] = data[api]['usd_market_cap']
        else:
            # if currently stored API ID has a lower market cap, replace
            if data[api] and ticker_mcs[symbol] < data[api]['usd_market_cap']:
                ticker_prices[symbol] = data[api]['usd']
                ticker_mcs[symbol] = data[api]['usd_market_cap']

    return ticker_prices

def query_tokens(tickers):
    """
    Takes the tickers and queries CoinGecko for the current price of each
    Input: tickers = a list of tickers that have remaining balances
    Output: a dictionary with the current price for each ticker
    """
    # find API IDs corresponding to tickers list
    apis, apis_list = find_api_ids(tickers)

    # query CoinGecko with API IDs list
    url = define_url('prices',apis_list)
    data = query_coingecko(url,headers)

    # create dictionary with prices for each ticker
    current_prices = find_api_prices(data,apis,apis_list)

    return current_prices

def write_potential_token_losses(balances):
    """
    Takes the remaining balances and writes a csv file with each remaining token and the amount that should be sold to maximize losses
    Inputs: balances = a dictionary for each token of dictionaries (id, date, price, and amount), each containing a list with each transaction
    Outputs: 0
    """
    import csv

    # make a list of the remaining tokens
    tickers = define_remaining_tokens(balances)

    # create a dictionary with the current price for each of the remaining tokens
    current_prices = query_tokens(tickers)
    print("current_prices = ", current_prices)
    exit()

    # create file to write losses
    outfile = str(year) + '/potentialLosses' + str(year) + '.csv'
    with open(outfile, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows([['Ticker','Potential Loss','Amount (ticker)','Current price ($)', 'Remaining (ticker)', 'Remaining ($)']])

    # loop through remaining tickers
    for ticker in tickers:

        # if token is no longer listed, give it a 0 price
        if ticker.lower() not in current_prices:
            current_price = 0
        else:
            current_price = current_prices[ticker.lower()]

        # calculate potential loss and amount that must be sold to obtain this loss
        loss, amount = calculate_potential_token_loss(current_price,balances[ticker])
    
        # write to file token loss and amount that must be sold to obtain this loss
        with open(outfile,'a') as file:
            i = [ticker, loss, amount, current_price, sum(balances[ticker]['amounts']), sum(balances[ticker]['amounts'])*current_price]
            file.write(','.join([str(x) for x in i]))
            file.write('\n')
            
    return 0

def calculate_potential_token_loss(current_price,balance):
    """
    Takes the current price and the price and amount for each of the remaining purchases for the current token, and calculates the maximum losses that could be declared by selling each lot sequentially
    Input: current_price = the current price for this token; balance = a dictionary for a token of dictionaries (id, date, price, and amount), each containing a list with each transaction; 
    Output: the potential loss for a specific ticker and the amount that must be sold to achieve this loss
    """
    if not balance:
        return 0, 0

    sum_loss, sum_amount, max_loss, max_amount = 0, 0, 0, 0
    for idx, price in enumerate(balance['prices']):
        amount = balance['amounts'][idx]
        sum_loss += amount*(current_price - price)
        sum_amount += amount
        if max_loss > sum_loss:
            max_loss = sum_loss
            max_amount = sum_amount

    return max_loss, max_amount

def update_balances(is_futures,idx,ticker,price,amount,min_prices,balances,row):
    """
    Updates the balances, adding to the list for purchases and removing from the list or reducing the amount for sales.  Futures are not added or removed.
    Input: idx = id of the transaction; ticker = ticker of the current token; price = price of the buy or sell, 
     amount = the amount bought or sold (positive for buy); min_prices = a dictionary containing the minimum price ever experienced since the first transaction in the csv file for each ticker;
     balances = a dictionary for each token of dictionaries (id, date, price, and amount), each containing a list with each transaction; buys = a dictionary containing a list for each ticker with pairs of transaction id and amount;
     row = a list of the data for the current transaction
    Output: price = if sell, the price at which the token was bought (this can be a mean), if buy, garbage; buys = a dictionary containing a list for each ticker with pairs of transaction id and amount;
     balances = a dictionary for each token of dictionaries (id, date, price, and amount), each containing a list with each transaction; buys = a dictionary containing a list for each ticker with pairs of transaction id and amount;
     buy_idx = idx of the purchase for this sale.  If multiple purchases, then the last one
    """
    # if positive, add to end of list.
    buy_idx = idx
    if amount > 0:
        date = row[0]
        balances = increase_balances(ticker,idx,date,price,amount,balances)
        """
        if ticker not in buys:
            buys[ticker] = []
        buys[ticker].append([idx,amount])
        """
    
    if amount < 0:
        price, buy_idx, balances = reduce_balances(is_futures,ticker,amount,min_prices,balances,row)

    return price, buy_idx, balances

def increase_balances(ticker,idx,date,price,amount,balances):
    # add to ticker's dictionary key
    if ticker not in balances:
        balances[ticker] = {}
        balances[ticker]['idx'] = []
        balances[ticker]['dates'] = []
        balances[ticker]['prices'] = []
        balances[ticker]['amounts'] = []

    # if positive, add to end of list.
    if  amount > 0:
        balances[ticker]['idx'].append(idx)
        balances[ticker]['dates'].append(date)
        balances[ticker]['prices'].append(price)
        balances[ticker]['amounts'].append(amount)

    return balances

def reduce_balances(is_futures,ticker,amount,min_prices,balances,row):
    """
    Reduce the balances for each sale and calculates the purchase price.  Futures are not taken into account
    Inputs: balances = a dictionary for each token of dictionaries (id, date, price, and amount), each containing a list with each transaction; ; 
    buy_idx = idx of the purchase for this sale.  If multiple purchases, then the last one
    """
    from datetime import datetime

    # initialize index of purchase to -1 in case no purchase is found.  This allows any repurchase to be considered when applying the 2-month rule
    buy_idx = -1

    if is_futures:
        return float(row[7]), buy_idx, balances

    # keep looping while there is still amount to be subtracted from first elements and while there are still elements
    sum_price = 0 # this is a sum of the prices times the amounts
    remaining_amount = amount # the remaining amount to be subtracted

    # error catching for 0 amounts
    if amount == 0:
        print("Must change the amount for this row.",row)
        exit()

    """
    if is_debug and ticker == 'DOT':
        print("\n", ticker)
        print(row)
        print(balances[ticker])
    """

    # if no balance is available for this ticker, set price and return
    if not balances or ticker not in balances or len(balances[ticker]['amounts']) == 0:

        # if we have a minimum price for this ticker, set this price.  Otherwise, the price is 0
        if ticker in min_prices:
            sum_price = min_prices[ticker]

        # if the ticker is not one of the old coins that were not well documented and the euro gains are more than 1 cent, write warning
        if ((ticker[:3] != 'fee' and ticker not in old_coins) or (ticker[:3] == 'fee' and ticker[3:] not in old_coins)) and abs(float(row[3])*float(row[5])) > 0.01:
            if ticker in balances:
                print(balances[ticker])
            print(row)
            print("No balance is available for this row.  We are trying to reduce balance by ",abs(float(row[3])*float(row[5]))," euros",row, "\n")

        return sum_price, buy_idx, balances

    # error catching for larger sell amounts than currently in balance.  The lowest price is used for the missing amounts
    if abs(amount) > sum(balances[ticker]['amounts']):

        # print warning if the maximum possible sale amount is greater than 1 cent
        if abs(max(balances[ticker]['prices'])*(abs(amount) - sum(balances[ticker]['amounts']))) > 50 and datetime.strptime(row[0], '%d-%m-%Y') >= datetime(year, 1, 1):
            if is_debug:
                print(row)
                print("current balance for ", ticker, " is ",balances[ticker])
            print("resetting balance to reflect that we are missing a purchase for ", amount, " ", ticker, ".  Old balance = ",sum(balances[ticker]['amounts']), ". New balance = ",abs(amount),". Balance difference is ", abs(amount) - sum(balances[ticker]['amounts']), " Difference is $",abs(max(balances[ticker]['prices'])*(abs(amount) - sum(balances[ticker]['amounts']))),"\n")

            """
            # print an additional warning if this is not an old coin
            if (ticker[:3] != 'fee' and ticker not in old_coins) or (ticker[:3] == 'fee' and ticker[3:] not in old_coins):
                print("Sell amount is larger than current balance by ",max(balances[ticker]['prices'])*(abs(amount) - sum(balances[ticker]['amounts']))," euros at max price ",max(balances[ticker]['prices']),row)
            """

        # min_price is the minimum of all prices unless FIFO in which case min_price is the first price
        if is_fifo:
            min_price = balances[ticker]['prices'][0]
        else:
            min_price = min(balances[ticker]['prices'])

        # increment balance for missing balances
        idx = balances[ticker]['prices'].index(min_price)
        balances[ticker]['amounts'][idx] = balances[ticker]['amounts'][idx] + abs(amount) - sum(balances[ticker]['amounts'])
        buy_idx = balances[ticker]['idx'][-1]

    # calculate price and update balances
    while remaining_amount < 0 and balances[ticker] and len(balances[ticker]['prices']) > 0:

        """
        if is_debug and ticker == 'DOT':
                print("balances = ", balances[ticker])
                print("remaining_amount = ", remaining_amount)
                print("balances[ticker]['prices'][0] = ", balances[ticker]['prices'][0])
        """

        buy_idx = balances[ticker]['idx'][0]

        # if the specified amount is less than the first element in list
        if abs(remaining_amount) < balances[ticker]['amounts'][0]:
            balances[ticker]['amounts'][0] = balances[ticker]['amounts'][0] + remaining_amount
            sum_price = sum_price - remaining_amount*balances[ticker]['prices'][0]
            remaining_amount = 0
        else:
            remaining_amount = remaining_amount + balances[ticker]['amounts'][0]
            sum_price = sum_price + balances[ticker]['amounts'][0]*balances[ticker]['prices'][0]
            del balances[ticker]['amounts'][0], balances[ticker]['prices'][0], balances[ticker]['dates'][0], balances[ticker]['idx'][0]

    """
    if is_debug and ticker == 'DOT':
        print("\nprice = ",sum_price/-amount)
        print("buy_idx = ", buy_idx)
        print("balances = ", balances[ticker], "\n")
    """


    return sum_price/-amount, buy_idx, balances

def set_price(is_futures,ticker,amount,min_prices,row):
    # set buy price depending on conditions
    # outputs: is_price = True if we have a valid buy price (False for FIFO); price = buy price (min_price for FIFO)
    try:
        # if futures, use price
        if is_futures:
            price = float(row[7])
            is_price = True
        else:
            
            # if FIFO and sale, force exception and thus never have a buy price without calculating
            if amount < 0 and is_fifo:
                price = float('?')

            price = float(row[7])
            is_price = True
    except:
        is_price = False
        if min_prices and ticker in min_prices:
            price = max(0,min_prices[ticker])
        else:
            price = ''
        if amount > 0 and '-' not in row[8] and '-' not in row[9]:
            print('This data is a purchase but with no buy price in euros. Fix this!!!')
            print(row)
            if not is_debug:
                exit()

    return is_price, price

def update_min_prices(ticker,price,min_prices):
    # update min price for that ticker
    # inputs: price = buy price
    if price and price != -1:
        if ticker not in min_prices:
            min_prices[ticker] = price
        else:
            # if FIFO then use the last price as min_price
            if is_fifo:
                min_prices[ticker] = price
            else:
                min_prices[ticker] = min(price,min_prices[ticker])

    return min_prices

def write_csv(outfile,a):

    with open(outfile, 'w', newline='') as f:
        writer = csv.writer(f)
        writer.writerows(a)

    return 0

# main code
###########
if __name__ == '__main__':

    # read data from csv
    data = read_csv(file_name)

    # read following years data and append
    data_next_year = read_csv(file_name_next_year)
    data += data_next_year[1:]

    # clean data
    data = clean_data(data)

    # loop through data and fill in price gaps
#    up_to_data = remove_end_dates(data)
    up_to_data = list(data)

    write_csv(str(year) + '/noFutures.csv',up_to_data)
    # merge Bitget futures data with rest of data
    up_to_data = merge_files(up_to_data, file_name_bitget)
    write_csv(str(year) + '/futures.csv',up_to_data)

    create_sales(up_to_data)
    
### This function is not ready.  Something is wrong with the query_coingecko function.  The query returns a ValueError.  Maybe from too many arguments?  Maybe b/c it starts with "-3"?
    # write a file with the maximized potential losses for each token if sold correctly
#    write_potential_token_losses(balances)