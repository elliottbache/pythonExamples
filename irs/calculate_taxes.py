import pandas as pd
from collections import defaultdict, deque
from datetime import datetime

# Load your file
file_path = "cryptos - tx.csv"
df = pd.read_csv(file_path)

df['Timestamp'] = pd.to_datetime(df['Date'])
#df = df.sort_values(by='Timestamp')

# Prepare FIFO ledger for each token
fifo = defaultdict(deque)

# Output for Form 8949
form8949 = []

# Utility to record a sale
def record_sale(token, amount, proceeds, cost_basis, acquisition_date, sale_date):
    if proceeds >= 0.005 or cost_basis >= 0.005:
        form8949.append({
            "Description": f"{amount:.8f} {token}",
            "Date Acquired": acquisition_date.strftime("%m/%d/%Y"),
            "Date Sold": sale_date.strftime("%m/%d/%Y"),
            "Proceeds": f"{proceeds:.2f}",
            "Cost Basis": f"{cost_basis:.2f}",
            "Gain or Loss": f"{proceeds - cost_basis:.2f}",
            "Code": "",
            "Adjustment Amount": ""
        })
    return 0

def update_fifo(sell_amount,token,fifo,proceeds,timestamp):
    
    # Pull from FIFO queue
    remaining = sell_amount
    while remaining > 0 and fifo[token]:

        # set the current lot
        lot = fifo[token][0]
        lot_amount = lot['amount']

        # define used amount, its cost, date, and proceeds
        used = min(remaining, lot_amount)
        cost = used/lot_amount * lot['cost']
        acquisition_date = lot['timestamp']
        this_proceeds = used/sell_amount * proceeds


        record_sale(
            token=token,
            amount=used,
            proceeds=this_proceeds,
            cost_basis=cost,
            acquisition_date=acquisition_date,
            sale_date=timestamp
        )

        lot['amount'] -= used
        lot['cost'] -= cost
        if lot['amount'] == 0:
            fifo[token].popleft()

        remaining -= used

    return fifo

# Main loop
len_df = len(df)
idx = 0
while idx < len_df:

    # define variables for checking content
    row = df.iloc[idx].copy(deep=True)
    if isinstance(row['Amount (token)'], str):
        amount0 = float(row['Amount (token)'].replace(',', ''))
    else:
        amount0 = float(row['Amount (token)'])
    row = df.iloc[idx+1].copy(deep=True)
    if isinstance(row['Amount (token)'], str):
        amount1 = float(row['Amount (token)'].replace(',', ''))
    else:
        amount1 = float(row['Amount (token)'])

    # separate into blocks of purchase, sale, transfer
    if str(df.iloc[idx]['Wallet']) == 'Metamask' and df.iloc[idx]['Token'].lower().startswith("fee"):
        block_type = 'approved_exchange'
        n_tx = 4
    elif '-' in str(df.iloc[idx]['Wallet']):
        block_type = 'transfer'
        n_tx = 2
    elif df.iloc[idx]['Token'] == 'USD' and amount1 > 0:
        block_type = 'purchase'
        n_tx = 3
    elif idx + 1 < len_df and df.iloc[idx+1]['Token'] == 'USD' and amount0 < 0:
        block_type = 'sale'
        n_tx = 3        
    elif idx + 1 < len_df and df.iloc[idx]['Token'] != 'USD' and df.iloc[idx+1]['Token'] != 'USD' and amount0 < 0 and amount1 > 0:
        block_type = 'exchange'
        n_tx = 3        
    else:
        print(df.iloc[idx])
        print("unidentified block type")
        sys.exit()
    
    # fill list with df rows for this block converting strings to floats
    rows = list()
    i_tx = 0
    while i_tx < n_tx:
        rows.append(df.iloc[idx + i_tx].copy(deep=True))
        if isinstance(rows[i_tx]['Amount (token)'], str):
            rows[i_tx]['Amount (token)'] = float(rows[i_tx]['Amount (token)'].replace(',', ''))
        if isinstance(rows[i_tx]['Sell Price ($)'], str):
            rows[i_tx]['Sell Price ($)'] = float(rows[i_tx]['Sell Price ($)'].replace(',', ''))
        if isinstance(rows[i_tx]['Buy price ($)'], str):
            rows[i_tx]['Buy price ($)'] = float(rows[i_tx]['Buy price ($)'].replace(',', ''))        
        i_tx += 1

    # increment idx so we go to next block after completing this one
    idx += n_tx

    # if no fee, throw error
    if not rows[-1]['Token'].lower().startswith("fee"):
        print(rows)
        print("Why are we missing a fee?")
        sys.exit()

    timestamp = rows[0]['Timestamp']


    # if Metamask approval
    if block_type == 'approved_exchange':

        # check that first and last are fees
        if not (rows[0]['Token'].lower().startswith("fee") and rows[3]['Token'].lower().startswith("fee")):
            print("This is not an approval and exchange")
            sys.exit()

        # add first row to last row.  Here we are assuming that the approval comes after the TX, which is untrue, but insignificant most likely
        rows[3]['Amount (token)'] = rows[3]['Amount (token)'] + rows[0]['Amount (token)']

        # delete first row
        del rows[0]

        # redefine type to treat as exchange
        block_type = 'exchange'

    # if purchase
    if block_type == 'purchase':

        fee_token = rows[2]['Token'][3:]
        fee_amount = rows[2]['Amount (token)']


        # define token, price, amount
        token = rows[1]['Token']
        price = rows[1]['Buy price ($)']
        cost = -rows[0]['Amount (token)']
        if fee_token != token: # if feeUSD
            cost -= fee_amount
        else: # if feeXXX
            cost -= fee_amount*price

        # calculate amount after subtracting possible fees
        amount = rows[1]['Amount (token)']
        if fee_token == token:
            amount += fee_amount

        fifo[token].append({
            "amount": amount,
            "price": price,
            "cost": cost, 
            "timestamp": timestamp
        })

    # if sale
    elif block_type == 'sale':

        fee_token = rows[2]['Token'][3:]
        fee_amount = rows[2]['Amount (token)']

        # define token, price, amount
        token = rows[0]['Token']
        price = rows[0]['Sell Price ($)']
        proceeds = rows[1]['Amount (token)']
        if fee_token != token: # if feeUSD
            proceeds += fee_amount
        else: # if feeXXX
            proceeds += fee_amount*price

        # calculate amount after subtracting possible fees
        amount = rows[0]['Amount (token)']
        if fee_token == token:
            amount += fee_amount

        # loop through purchases until filling the amount
        sell_amount = -amount

        # Pull from FIFO queue
        fifo = update_fifo(sell_amount,token,fifo,proceeds,timestamp)

    # if exchange
    elif block_type == 'exchange':

        fee_token = rows[2]['Token'][3:]
        fee_amount = rows[2]['Amount (token)']
        fee_price = rows[2]['Sell Price ($)']

        # define token, price, amount
        buy_token = rows[1]['Token']
        sell_token = rows[0]['Token']
        buy_price = rows[1]['Buy price ($)']
        sell_price = rows[0]['Sell Price ($)']
        buy_amount = rows[1]['Amount (token)']
        sell_amount = rows[0]['Amount (token)'] # negative

        proceeds = -sell_price*sell_amount

        # reduce proceeds by fees
        proceeds += fee_amount*fee_price

        cost = proceeds

        # calculate amount after subtracting possible fees
        if fee_token == sell_token: # if fees are charged on sale token
            sell_amount += fee_amount
        elif fee_token == buy_token: # if fees are charged on buy token
            buy_amount += fee_amount

        # append purchase to fifo
        fifo[buy_token].append({
            "amount": buy_amount,
            "price": buy_price,
            "cost": cost, 
            "timestamp": timestamp
        })

        # loop through purchases until filling the sale amount
        sell_amount = -sell_amount # positive
        fifo = update_fifo(sell_amount,sell_token,fifo,proceeds,timestamp)

        # if fee_token is not one of the exchanged tokens, loop through purchases until filling the fee amount
        if fee_token != sell_token and fee_token != buy_token:
            sell_amount = -fee_amount
            proceeds = -fee_amount * fee_price
            fifo = update_fifo(sell_amount,fee_token,fifo,proceeds,timestamp)

    # if transfer
    elif block_type == 'transfer':

        # define fee variables
        fee_token = rows[1]['Token'][3:]
        fee_amount = rows[1]['Amount (token)']
        fee_price = rows[1]['Sell Price ($)']

        # update FIFO, recording sale
        sell_amount = -fee_amount
        proceeds = -fee_amount * fee_price
        fifo = update_fifo(sell_amount,fee_token,fifo,proceeds,timestamp)

# Create .csv with output for f8949
form8949_df = pd.DataFrame(form8949)
form8949_df.to_csv("form8949_output.csv", index=False)
print("✅ Form 8949 data saved to form8949_output.csv")