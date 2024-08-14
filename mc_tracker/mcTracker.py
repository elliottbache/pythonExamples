# This has been edited to show the general structure of the program, without showing the full code.

# global variables
##################
# Debugging flags
is_query = True # make False if we don't want to query Coingecko
is_timer = True # make False if we do not with to use timer and continually try to query and graph
is_once = False # make True if we only want to run once 
is_less_graphs = True # make True if we only want to create a smaller subset of graphs

# CoinGecko query headers
headers = {"accept": "application/json","x-cg-demo-api-key": ""}

base_tokens = ['bitcoin','ethereum'] # these must be in decreasing MC order
narratives = ['artificial-intelligence','real-world-assets-rwa','gaming','zero-knowledge-zk','depin','modular-blockchain','brc-20','meme-token','solana-meme-coins','base-meme-coins','politifi']
normalizations = ['self']
normalizations += base_tokens
cap_names = ['micro', 'small', 'mid', 'large']
lower_cap_limits = [1e6, 1e8, 1e9, 1e10]
top_ranges = {'mine': {'large': ['bitcoin','ethereum','binancecoin','solana','chainlink'],'mid': ['kaspa','fetch-ai','render-token','arbitrum','filecoin','bittensor','arweave','ondo-finance','celestia'], 'small': ['coredaoorg','mantra-dao','aioz-network','golem','arkham','nosana','centrifuge','zksync'] ,'micro': ['limewire-token','commune-ai','drunk-robots','polyhedra-network','smart-layer-network','vapor-wallet','realio-network','swarm-markets','guru-network','holograph','lightlink']}, 'raul': {'large': ['solana','chainlink']}}
top_tokens = {'Rocio mid-large': ['bitcoin','ethereum','binancecoin','solana','chainlink','kaspa','fetch-ai','render-token','arbitrum','filecoin','bittensor','arweave','ondo-finance','celestia'], 'Rocio micro-small': ['coredaoorg','mantra-dao','aioz-network','golem','arkham','nosana','centrifuge','zksync','limewire-token','commune-ai','drunk-robots','polyhedra-network','smart-layer-network','vapor-wallet','realio-network','swarm-markets','guru-network','holograph','farcana','omni-network','supra','qubic-network','the-root-network','bvm']}
top_symbols = {}

# interval between data points in seconds
interval = 4*3600
#interval = 5*60

# location of data files for categories
directory = 'data/'

# default date format
date_format = '%Y-%m-%d %H:%M:%S'

# possible parameters for graphs
differentiation_orders = [0, 1, 2]
#emas = [0, 10, 20, 50, 200] # 0 is not averaged
emas = [0, 10, 50, 200] # 0 is not averaged

# time frames for percentage gains table
time_frames = [4, 8, 12, 24, 48, 72, 168, 336, 720]
time_frames_text = ['4 hrs', '8 hrs', '12 hrs', '1 day', '2 days', '3 days', '1 week', '2 weeks', '1 month']

# functions
###########
def read_csv(file_name):

    return csv_data

def read_api():

    return api_key

def LastNlines(fname, N):

    return out_line

def find_last_time_all():

    return last_time_all

def read_plot_data(this_directory,filename):

    return df

def name_mc_column(normalization):

    return colname

def interpolate_base_token(colname,df,df_base):

    return df_base_int[colname]

def interpolate_dataframe(colname,df,df_base):

    return df_base_int

def initialize_plot_data(mc_type,df_base,this_directory,filename,this_normalizations):

    return df

def prepare_plot_data(df,this_normalizations,this_orders):

    return df

def remove_normalization(category,in_normalizations):

    return this_normalizations

def define_graph_limits(min_mc,max_mc,min_last_mc,max_last_mc,mc_type,order):

    return min_mc, max_mc

def should_i_graph(normalization,order,ema):

    return False

def plot_type_graphs(mc_type,person,plot_data_base,in_normalizations):

    return 0

def plot_all_graphs():

    return 0

def define_this_categorys_interpolated_gains(mc_type,df,filename):

    return gains_str, gains_float

def define_this_categorys_gains(mc_type,df,filename):

    return gains_str, gains_float

def create_filename_list(mc_type,person):

    return filenames 

def create_table(mc_type,person,df,this_directory):

    return 0

def define_column_name(df,normalization,order,ema):

    return colname

def define_order_text(order):

    return order_text

def define_ema_text(ema):

    return ema_text

def add_underscores(text_type,in_text,add_text):

    return 1

def create_plot_text(text_type,mc_type,person,normalization,order,ema):
    
    return plot_text.strip()

def initialize_file(fname):
    
    return 0

def query_coingecko(url,headers):

    return data

def write_mcs(mc_type,name,mc):

    return 0

def read_mcs_above_limit(lower_limit):

    return new_tokens, tokens, mcs

def read_last_period_tokens(old_tokens, old_mcs):
    
    return old_tokens, old_mcs

def subtract_last_mcs(old_tokens,old_mcs,new_tokens):

    return normalized_range_diffs

def write_old_tokens(old_tokens,old_mcs):

    return 0

def process_caps(old_tokens,old_mcs):

    return old_tokens, old_mcs

def define_url(mc_type,query_list):

    return 1

def process_mcs(mc_type,query_list):

    return 0

def find_range(l, x):

    return l.index(i)

def read_cap_ranges():

    return cap_ranges

def sum_cap_ranges(cap_ranges):

    return cap_range_mcs

def create_token_list():

    return token_list

# main code
###########
if __name__ == '__main__':

    import datetime
    import requests
    import time
    import sys
    import os
    import pandas as pd
    from pandas import *
    import json
    import inflect
    import matplotlib
    import matplotlib.pyplot as plt
    from matplotlib import cm
    import math
    import numpy as np

    # read API key from file
    api_key = read_api()
    headers['x-cg-demo-api-key'] = api_key + "\t"

    # create token list containing all top tokens
    token_list = create_token_list()

    # initialize last period's market caps for range difference calculation
    old_tokens, old_mcs = [], []

    # set current time and last time
    last_time = find_last_time_all()

    # plot first time launched
    plot_all_graphs()

    # infinite loop
    while True:

        now = datetime.datetime.now()
#        print("now = ",now)

        # cycle if interval has not passed yet
        if now - datetime.timedelta(seconds=interval) < last_time and is_timer:
            print("interval has not passed yet at ",now,". sleeping for ",(datetime.timedelta(seconds=interval) - (now - last_time)).total_seconds(),"seconds until ",datetime.timedelta(seconds=interval) + last_time)
            time.sleep(60)
            continue

        if is_query:
            
            # process top tokens data
            query_list = token_list
            print("Querying CoinGecko for top tokens data")
            process_mcs('tokens',query_list)

            # process narrative data
            query_list = narratives
            print("Querying CoinGecko for narrative data")
            process_mcs('narratives',query_list)

            # process micro-large cap data
            print("Querying CoinGecko for cap ranges data")
            old_tokens, old_mcs = process_caps(old_tokens,old_mcs)

        print("New data has been retrieved")

        # create graphs
        plot_all_graphs()

        if is_once:
            sys.exit()

        last_time = now