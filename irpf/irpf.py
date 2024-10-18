# global variables
##################
is_debug = True
year = 2023
beginning_of_time_year = 2023
file_name = str(year) + '/cryptos - Tx.csv'
#file_name = str(year) + '/cryptos.csv'

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

    min_prices, buys, last_years_buys, sales, all_sales, last_years_sales, carry_over, fees, balances, last_years_balances = {}, {}, {}, {}, {}, {}, {}, {}, {}, {}
    for idx, row in enumerate(data):
        
        # define variables for this row
        ticker = row[2]
        amount = float(row[3])

        # if fee is incorrectly listed as a positive amount, change to negative
        if ticker[:3] == 'fee' and amount > 0:
            amount = -amount

        is_price, price = set_price(ticker,amount,min_prices,row)
        wallet = row[8]
        chain = row[9]

        # skip internal transfer rows
        if '-' in wallet or '-' in chain:
            # check that we don't have a transfer with prices.  A transfer with prices directly preceded by a transfer w/o prices would be the corresponding transfer fees & would be valid
            if (row[4] or row[5]) and idx > 0 and data[idx-1][8] != wallet:
                print("We shouldn't have a transfer with sell prices.", row)
                exit()
            continue

        # update min prices in case we don't have a buy price, we use the min price of all time for that token
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

        if datetime.strptime(row[0], '%d-%m-%Y') <= last_year_end:
            buy_price, last_years_buys, last_years_balances = update_balances(idx,bticker,price,amount,min_prices,last_years_balances,last_years_buys,row)

        if datetime.strptime(row[0], '%d-%m-%Y') <= end:
            buy_price, buys, balances = update_balances(idx,bticker,price,amount,min_prices,balances,buys,row)

        # if we have transferred to someone else, then no need to look at sales
        if 'Sent' in row[10]:
            continue

        # only look at rows that are sales and are from the current year
        if amount < 0 and datetime.strptime(row[0], '%d-%m-%Y') <= end and datetime.strptime(row[0], '%d-%m-%Y') >= beginning_of_time:

            if not is_price:
                price = buy_price

            # add sale to list of sales
            all_sales = add_sale(idx,bticker,price,amount,balances,min_prices,row,all_sales)

            if datetime.strptime(row[0], '%d-%m-%Y') <= last_year_end:

                # add sale to list of sales
                last_years_sales = add_sale(idx,bticker,price,amount,balances,min_prices,row,last_years_sales)

            if datetime.strptime(row[0], '%d-%m-%Y') >= begin:

                # add sale to list of sales
                sales = add_sale(idx,bticker,price,amount,balances,min_prices,row,sales)

                if ticker[:3] == 'fee':
                    # add fee to list of fees
                    fees = add_fee(ticker,price,amount,balances,min_prices,row,fees)

    # for current year's sales, if we have a sale at a loss and there are purchases within two months, then we don't compute until they are sold
    sales, reduced_losses = reduce_losses(year,balances,buys,sales)

    # for sales up to last year, if we have a sale at a loss and there are purchases within two months, then we don't compute until they are sold
    last_years_sales, _ = reduce_losses(year-1,last_years_balances,last_years_buys,last_years_sales)

    # for sales up to this year, if we have a sale at a loss and there are purchases within two months, then we don't compute until they are sold
    all_sales, _ = reduce_losses(year,balances,buys,all_sales)

    # carry over losses from previous years
    carry_over = carry_over_losses(last_years_sales, all_sales)

    # remove indices from sales, reduced_losses, and carry_over
    sales_write, reduced_losses_write, carry_over_write = {}, {}, {}
    for key in sales:
        if key not in sales_write:
            sales_write[key] = []
        for sale in sales[key]:
            sales_write[key].append(sale[1:])
    for key in reduced_losses:
        if key not in reduced_losses_write:
            reduced_losses_write[key] = []
        for reduced_loss in reduced_losses[key]:
            reduced_losses_write[key].append(reduced_loss[1:])
    for key in carry_over:
        if key not in carry_over_write:
            carry_over_write[key] = []
        for sale in carry_over[key]:
            carry_over_write[key].append(sale[1:])

    # write csv file with gains and fees
    write_output_file(sales_write,fees,reduced_losses_write,carry_over_write)

    return sales, balances

def carry_over_losses(last_years_sales, all_sales):
    """
    We assume that all sales that were present from the beginning of time to last year should be present in the sales from the beginning of time to the present year.
    If they are not present, that means that they were reduced since they did not meet the 2-month rule.  Since they are present in all sales up to the current year, 
    this means that the following repurchase must have been closed allowing them to meeet the 2-month rule. 
    """
    carry_over = {}
    # loop through last year's sales key
    for key in last_years_sales:

        # loop through last year's sales for this key
        for idx, row in enumerate(last_years_sales[key]):
            
            # if this year's sales is different from last year's
            if len(last_years_sales[key]) > 0 and row != all_sales[key][idx]:

                if key not in carry_over:
                    carry_over[key] = []

                if is_debug:
                    print("\nthis row is not in last years sales", row, '\n', all_sales[key][idx])

                # calculate gains difference between the two years and append to carry_over
                new_sale = row.copy()
                new_sale[3] = all_sales[key][idx][3] - row[3]
                new_sale[6] = all_sales[key][idx][6] - row[6]
                carry_over[key].append(new_sale)

    return carry_over

def reduce_losses(year,balances,buys,sales):

    from datetime import datetime
    from dateutil.relativedelta import relativedelta

    end = datetime(year, 12, 31)

    # loop through tickers
    reduced_losses = {}
    original_sales = dict(sales)
    for key in balances:

        if key in sales:

            sales_key = list(original_sales[key])

            # if no sales then no problem!
            if len(sales_key) == 0:
                continue
            
        # if key not in sales
        else:
            continue

        balances_key = dict(balances[key])

        # if no remaining balances then no problem!
        if not balances_key or len(balances_key['idx']) == 0:
            continue

        buys_key = list(buys[key])

        # if we have no buys then continue
        if not buys_key or len(buys_key) == 0:
            continue

        # take sum of remaining_balance
        total_remaining_balance = sum(balances_key['amounts'])

        # go backwards through buys while we get to the buy before the first one in balances 
        ifirst = balances_key['idx'][0]
        idx = len(buys_key) - 1
        while idx > 0 and buys_key[idx][0] >= ifirst:
            idx = idx - 1

        # go forwards through last_sales
        for isale, sale in enumerate(sales_key):

            # only sales up till current year
            if datetime.strptime(sale[1], '%d-%m-%Y') > end:
                continue

            # if no remaining balances then no problem!
            if not balances_key or len(balances_key['idx']) == 0:
                break

            # only look at sales after the buy previous to the first remaining balance
            if sale[0] < idx:
                continue

            # if isale is gain, continue
            if sale[6] > 0:
                continue

            # we will subtract from first balance, then continue until the sale amount is satisfied while the date is less than 2 months before
            amount = abs(sale[3])
            current_buy, total_buy = 0, 0
            date = datetime.strptime(balances_key['dates'][0], '%d-%m-%Y')
            while total_buy < amount and date < end and date < datetime.strptime(sale[1], '%d-%m-%Y') + relativedelta(months=+2) and len(balances_key['amounts']) > 0:

                if balances_key['idx'][0] < sale[0]:
                    del balances_key['amounts'][0], balances_key['prices'][0], balances_key['idx'][0], balances_key['dates'][0]
                    if len(balances_key['amounts']):
                        date = datetime.strptime(balances_key['dates'][0], '%d-%m-%Y')
                    continue 

                if total_buy + balances_key['amounts'][0] <= amount:
                    if is_debug:
                        print('balances_key  = ',balances_key)
                    current_buy = balances_key['amounts'][0]
                    total_buy = total_buy + current_buy
                    del balances_key['amounts'][0], balances_key['prices'][0], balances_key['idx'][0], balances_key['dates'][0]
                    if len(balances_key['amounts']):
                        date = datetime.strptime(balances_key['dates'][0], '%d-%m-%Y')

                else:
                    if is_debug:
                        print('balances_key  = ',balances_key)
                    current_buy = amount - total_buy
                    total_buy = amount
                    balances_key['amounts'][0] = balances_key['amounts'][0] - current_buy

                # subtract amount from isale
                if is_debug:
                    print('REDUCING SALE!!!!', key )
                    print('before', sales[key][isale] )
                    print('current buy = ',current_buy)

                sales[key][isale][3] = sales[key][isale][3] - current_buy
                sales[key][isale][6] = -sales[key][isale][3]*(sales[key][isale][4] - sales[key][isale][5])

                if is_debug:
                    print('after', sales[key][isale] )

                # define and initialize list for each sale in loss that is going to be reduced due to a still open purchase within 2 months
                if key not in reduced_losses:
                    reduced_losses[key] = []
                if len(reduced_losses[key]) == 0 or reduced_losses[key][-1][0] != sale[0]:
                    reduced_losses[key].append(list(sale))
                    reduced_losses[key][-1][3] = 0
                    reduced_losses[key][-1][6] = 0

                # add the undeclared losses to reduced_losses
                reduced_losses[key][-1][3] = reduced_losses[key][-1][3] + current_buy
                reduced_losses[key][-1][6] = -reduced_losses[key][-1][3]*(sales[key][isale][4] - sales[key][isale][5])

                if is_debug:
#                    print('reduced_losses', reduced_losses[key][0] )
                    print('reduced_losses', reduced_losses[key])

    return sales, reduced_losses

def write_output_file(sales,fees,reduced_losses,carry_over):

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

def add_fee(ticker,price,amount,balances,min_prices,row,fees):

    if row[5]:
        sell_price = float(row[5])
    else:
        if row[4]:
            sell_price = float(row[4])*float(row[1])
        else:
            print("Why is there no sale price?",row)
            exit()

    # add new ticker to fees
    if ticker[3:] not in fees:
        fees[ticker[3:]] = []

    fees[ticker[3:]].append([row[0],ticker[3:],-amount,sell_price,-amount*(sell_price)])    
    
    return fees

def add_sale(idx,ticker,price,amount,balances,min_prices,row,sales):

    if row[5]:
        sell_price = float(row[5])
    else:
        if row[4]:
            sell_price = float(row[4])*float(row[1])
        else:
            print("Why is there no sale price?",row)
            exit()

    # add new ticker to sales
    if ticker not in sales:
        sales[ticker] = []

    # don't declare stuff that is very small
    if abs(amount*(price - sell_price)) > 1e-6:
        sales[ticker].append([idx,row[0],ticker,-amount,price,sell_price,amount*(price - sell_price)])    

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
    print(current_prices)

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

def update_balances(idx,ticker,price,amount,min_prices,balances,buys,row):
    """
    Updates the balances, adding to the list for purchases and removing from the list or reducing the amount for sales
    Input: idx = id of the transaction; ticker = ticker of the current token; price = price of the buy or sell, 
     amount = the amount bought or sold (positive for buy); min_prices = a dictionary containing the minimum price ever experienced since the first transaction in the csv file for each ticker;
     balances = a dictionary for each token of dictionaries (id, date, price, and amount), each containing a list with each transaction; buys = a dictionary containing a list for each ticker with pairs of transaction id and amount;
     row = a list of the data for the current transaction
    Output: price = if sell, the price at which the token was bought (this can be a mean), if buy, garbage; buys = a dictionary containing a list for each ticker with pairs of transaction id and amount;
     balances = a dictionary for each token of dictionaries (id, date, price, and amount), each containing a list with each transaction; buys = a dictionary containing a list for each ticker with pairs of transaction id and amount;
    """
    # if positive, add to end of list.
    if amount > 0:
        date = row[0]
        balances = increase_balances(ticker,idx,date,price,amount,balances)
        if ticker not in buys:
            buys[ticker] = []
        buys[ticker].append([idx,amount])
    
    # else subtract from first element. 
        # when first element is 0, remove and continue with next
        # if no more elements, continue to next row and set price as min overall price for that ticker
        # sum(price*amount)/total amount
    if amount < 0:
        price, balances = reduce_balances(ticker,amount,min_prices,balances,row)

    return price, buys, balances

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

def reduce_balances(ticker,amount,min_prices,balances,row):
    # keep looping while there is still amount to be subtracted from first elements and while there are still elements
    sum_price = 0 # this is a sum of the prices times the amounts
    remaining_amount = amount # the remaining amount to be subtracted

    # error catching for 0 amounts
    if amount == 0:
        print("Must change the amount for this row.",row)
        exit()

    # if no balance is available for this ticker
    if not balances or ticker not in balances or len(balances[ticker]['amounts']) == 0:
        if ticker in min_prices:
            sum_price = min_prices[ticker]
        if ((ticker[:3] != 'fee' and ticker not in old_coins) or (ticker[:3] == 'fee' and ticker[3:] not in old_coins)) and abs(float(row[3])*float(row[5])) > 0.01:
            print("No balance is available for this row, but we are reducing balance by ",abs(float(row[3])*float(row[5]))," euros",row)
        return sum_price, balances

    # error catching for larger sell amounts than currently available.  The lowest price is used for the missing amounts
    if abs(amount) > sum(balances[ticker]['amounts']):
        min_price = min(balances[ticker]['prices'])
#        min_price = min_prices[ticker]
        idx = balances[ticker]['prices'].index(min_price)
        balances[ticker]['amounts'][idx] = balances[ticker]['amounts'][idx] + abs(amount) - sum(balances[ticker]['amounts'])
        if ((ticker[:3] != 'fee' and ticker not in old_coins) or (ticker[:3] == 'fee' and ticker[3:] not in old_coins)) and abs(max(balances[ticker]['prices'])*(abs(amount) - sum(balances[ticker]['amounts']))) > 0.01:
            print("Sell amount is larger than current balance by ",max(balances[ticker]['prices'])*(abs(amount) - sum(balances[ticker]['amounts']))," euros at max price ",max(balances[ticker]['prices']),row)

    while remaining_amount < 0 and balances[ticker] and len(balances[ticker]['prices']) > 0:
        # if the specified amount is less than the first element in list
        if abs(remaining_amount) < balances[ticker]['amounts'][0]:
            balances[ticker]['amounts'][0] = balances[ticker]['amounts'][0] + remaining_amount
            sum_price = sum_price - remaining_amount*balances[ticker]['prices'][0]
            remaining_amount = 0
        else:
            remaining_amount = remaining_amount + balances[ticker]['amounts'][0]
            sum_price = sum_price + balances[ticker]['amounts'][0]*balances[ticker]['prices'][0]
            del balances[ticker]['amounts'][0], balances[ticker]['prices'][0], balances[ticker]['dates'][0], balances[ticker]['idx'][0]

    return sum_price/-amount, balances

def set_price(ticker,amount,min_prices,row):

    # set buy price depending on conditions
    try:
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
            exit()

    return is_price, price

def update_min_prices(ticker,price,min_prices):
    # update min price for that ticker
    if price and price != -1:
        if ticker not in min_prices:
            min_prices[ticker] = price
        else:
            min_prices[ticker] = min(price,min_prices[ticker])

    return min_prices

# main code
###########
if __name__ == '__main__':

    # read data from csv
    data = read_csv(file_name)

    # clean data
    data = clean_data(data)

    # loop through data and fill in price gaps
#    up_to_data = remove_end_dates(data)
    up_to_data = list(data)

    sales, balances = create_sales(up_to_data)
    
    # write a file with the maximized potential losses for each token if sold correctly
    write_potential_token_losses(balances)