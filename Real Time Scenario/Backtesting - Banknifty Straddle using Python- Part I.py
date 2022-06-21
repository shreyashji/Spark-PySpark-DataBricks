# Databricks notebook source
# MAGIC %md
# MAGIC 
# MAGIC * /FileStore/tables/backtesting/complete_nfo_data_2019_01_01.pkl

# COMMAND ----------

# MAGIC %md
# MAGIC * Bank Nifty short Straddle
# MAGIC * Selling 1 CE ATM & 1 PE ATM option at 9:20 and exit at 15:10 with a stoploss of 20% each

# COMMAND ----------

#importing all liabraries
import pandas as pd
impot y
import numpy as np
import datetime
import matplotlib.pyplot as plt
from glob import glob
from dateutil.relativedelta import relativedelta,TH

# COMMAND ----------

dbutils.fs.ls ("/FileStore/tables/backtesting/")

# COMMAND ----------

#fething all data
path=pd.DataFrame(glob("/FileStore/tables/backtesting/"),columns=['location'])
path['datadate']=path['location'].apply(lambda x: x.split('_')[-1].split('.')[0])
path['datadate']=path['datadate'].apply(lambda x: datetime.datetime.strptime(x,'%Y-%m-%d'))
path=path.sort_values('datadate')
path.reset_index(drop=True,inplace=True)

# COMMAND ----------

path

# COMMAND ----------

trade_log=pd.DataFrame(columns=['Entry_date_time', 'Future_price', 'ATM', 'Days_to_expiry',
          'CE_symbol','CE_entry_price','CE_exit_price','CE_exit_datetime','CE_pnl',
          'PE_symbol','PE_entry_price','PE_exit_price','PE_exit_datetime','PE_pnl', 'Total_pnl'])

# COMMAND ----------

trade_log

# COMMAND ----------

for index, row in path.iterrows():
    try:
        print(index)
        data=pd.read_pickle(row['location'])


# COMMAND ----------



# COMMAND ----------

for index, row in path.iterrows():
    try:
        print(index)
        data=pd.read_pickle(row['location'])
    
        entry_datetime=datetime.datetime.combine(row['datadate'].date(),datetime.time(9,20))
        exit_dateitme=datetime.datetime.combine(row['datadate'].date(),datetime.time(15,10))
        print(entry_datetime)
        print(exit_dateitme).show()
                
        # bnf future data & ATM
        data['expiry_type']=np.where((data['instrument_type']=='FUT'),(data['ticker'].apply(lambda x:x.split('-')[-1].split('.')[0])),"")
        current_month_exp= 'I' # I-current month expiry, II-next month expiry, III- far month expiry
        instrument='BANKNIFTY'
        future_data=data[(data['instrument_type']=='FUT') & (data['instrument_name']==instrument) & (data['expiry_type']==current_month_exp)]
        future_data.reset_index(drop=True,inplace=True)
        atm=future_data[future_data['datetime']==entry_datetime]['open'].iloc[0]
        base_price=100
        atm=base_price*round(atm/base_price)

        # CE data & PE data
        nearest_expiry=row['datadate'].date()+relativedelta(weekday=TH)
        ce_data=data[(data['instrument_type']=='CE') & (data['instrument_name']==instrument) & (data['strike_price']==atm) & ((data['expiry_date']==nearest_expiry)|(data['expiry_date']==nearest_expiry-datetime.timedelta(days=1))|(data['expiry_date']==nearest_expiry-datetime.timedelta(days=2)))]
        ce_data.reset_index(drop=True,inplace=True)
        pe_data=data[(data['instrument_type']=='PE') & (data['instrument_name']==instrument) & (data['strike_price']==atm) & ((data['expiry_date']==nearest_expiry)|(data['expiry_date']==nearest_expiry-datetime.timedelta(days=1))|(data['expiry_date']==nearest_expiry-datetime.timedelta(days=2)))]
        pe_data.reset_index(drop=True,inplace=True)

        #symbol & entry price
        ce_symbol=ce_data['ticker'].iloc[0]
        pe_symbol=pe_data['ticker'].iloc[0]
        future_price= future_data[future_data['datetime']==entry_datetime]['open'].iloc[0]
        ce_entry_price= ce_data[ce_data['datetime']==entry_datetime]['close'].iloc[0]
        pe_entry_price=pe_data[pe_data['datetime']==entry_datetime]['close'].iloc[0]

        #test data
        future_data=future_data[['datetime','close']].set_index(['datetime'])
        ce_data=ce_data[['datetime','close']].set_index(['datetime'])
        pe_data=pe_data[['datetime','close']].set_index(['datetime'])
        intraday_data=pd.concat([future_data,ce_data,pe_data],axis=1)
        intraday_data.columns=['future_close','ce_close','pe_close']
        intraday_data=intraday_data.ffill()
        intraday_data.reset_index(inplace=True)
        entry_datetime_index=intraday_data[intraday_data['datetime']==entry_datetime].index[0]
        exit_datetime_index=intraday_data[intraday_data['datetime']==exit_dateitme].index[0]
        intraday_data=intraday_data[entry_datetime_index:exit_datetime_index+1]
        intraday_data['ce_pnl']=0
        intraday_data['pe_pnl']=0
        intraday_data.reset_index(drop=True,inplace=True)

        #stoploss
        stop_loss= 20/100
        ce_stop_loss=ce_entry_price+ce_entry_price*stop_loss
        pe_stop_loss=pe_entry_price+pe_entry_price*stop_loss

        #backtest
        ce_stop_loss_counter=0
        pe_stop_loss_counter=0
        ce_exit_datetime=''
        pe_exit_datetime=''
        ce_exit_price=0
        pe_exit_price=0
        ce_pnl=0
        pe_pnl=0
        pnl=0

        for index, row in intraday_data.iterrows():
            ce_ltp=row['ce_close']
            pe_ltp=row['pe_close']
            #print(f"{row['datetime']}::{ce_ltp}::{pe_ltp}")
            #Exit crateria
            #1. CE & PE didn't hit SL & both reach exit time limit 15:10
            if (ce_stop_loss_counter==0) & (pe_stop_loss_counter==0) & (row['datetime']==exit_dateitme):
                ce_pnl=ce_entry_price-ce_ltp
                pe_pnl=pe_entry_price-pe_ltp
                ce_stop_loss_counter=1
                pe_stop_loss_counter=1
                ce_exit_datetime=row['datetime']
                pe_exit_datetime=row['datetime']
                ce_exit_price=ce_ltp
                pe_exit_price=pe_ltp
                intraday_data.loc[index,'ce_pnl']=ce_pnl
                intraday_data.loc[index,'pe_pnl']=pe_pnl
                print('CE & PE did not hit SL, both exit at 15:10')
                pnl=ce_pnl+pe_pnl
                break 
            #2. CE is now hit SL, none were hit SL till now
            elif (ce_ltp>=ce_stop_loss) & (ce_stop_loss_counter==0) & (pe_stop_loss_counter==0):
                ce_pnl=ce_entry_price-ce_stop_loss
                pe_pnl=pe_entry_price-pe_ltp
                ce_stop_loss_counter=1
                ce_exit_datetime=row['datetime']
                ce_exit_price=ce_stop_loss
                intraday_data.loc[index,'ce_pnl']=ce_pnl
                intraday_data.loc[index,'pe_pnl']=pe_pnl
                print('CE SL hit')
                pnl=ce_pnl+pe_pnl

            #3. PE is now hit SL, none were hit SL till now
            elif (pe_ltp>=pe_stop_loss) & (ce_stop_loss_counter==0) & (pe_stop_loss_counter==0):
                ce_pnl=ce_entry_price-ce_ltp
                pe_pnl=pe_entry_price-pe_stop_loss
                pe_stop_loss_counter=1
                pe_exit_datetime=row['datetime']
                pe_exit_price=pe_stop_loss
                intraday_data.loc[index,'ce_pnl']=ce_pnl
                intraday_data.loc[index,'pe_pnl']=pe_pnl
                print('PE SL hit')
                pnl=ce_pnl+pe_pnl
            #4. CE was hit SL, (PE is either hit SL or reach exit time limit 15:10)
            elif (ce_stop_loss_counter==1) & (pe_stop_loss_counter==0):
                if (pe_ltp>=pe_stop_loss) & (row['datetime']<exit_dateitme):
                    pe_pnl=pe_entry_price-pe_stop_loss
                    pe_stop_loss_counter=1
                    pe_exit_datetime=row['datetime']
                    pe_exit_price=pe_stop_loss
                    intraday_data.loc[index,'ce_pnl']=ce_pnl
                    intraday_data.loc[index,'pe_pnl']=pe_pnl
                    print('PE SL hit')
                    pnl=ce_pnl+pe_pnl
                    break
                elif (row['datetime']==exit_dateitme):
                    pe_pnl=pe_entry_price-pe_ltp
                    pe_stop_loss_counter=1
                    pe_exit_datetime=row['datetime']
                    pe_exit_price=pe_ltp
                    intraday_data.loc[index,'ce_pnl']=ce_pnl
                    intraday_data.loc[index,'pe_pnl']=pe_pnl
                    print('PE exit at 15:10')
                    pnl=ce_pnl+pe_pnl
                    break     
            #5. PE was hit SL, (CE is either hit SL or reach exit time limit 15:10)
            elif (pe_stop_loss_counter==1) & (ce_stop_loss_counter==0):
                if (ce_ltp>=ce_stop_loss) & (row['datetime']<exit_dateitme):
                    ce_pnl=ce_entry_price-ce_stop_loss
                    ce_stop_loss_counter=1
                    ce_exit_datetime=row['datetime']
                    ce_exit_price=ce_stop_loss
                    intraday_data.loc[index,'ce_pnl']=ce_pnl
                    intraday_data.loc[index,'pe_pnl']=pe_pnl
                    print('CE SL hit')
                    pnl=ce_pnl+pe_pnl
                    break
                elif (row['datetime']==exit_dateitme):
                    ce_pnl=ce_entry_price-ce_ltp
                    ce_stop_loss_counter=1
                    ce_exit_datetime=row['datetime']
                    ce_exit_price=ce_ltp
                    intraday_data.loc[index,'ce_pnl']=ce_pnl
                    intraday_data.loc[index,'pe_pnl']=pe_pnl
                    print('CE exit at 15:10')
                    pnl=ce_pnl+pe_pnl
                    break
            #6. update pnl in normal time
            elif ((ce_stop_loss_counter==0 & pe_stop_loss_counter==0) | (ce_stop_loss_counter==1 & pe_stop_loss_counter==0) | (ce_stop_loss_counter==0 & pe_stop_loss_counter==1) | (ce_stop_loss_counter==1 & pe_stop_loss_counter==1)) or (row['datetime']<exit_dateitme):
                ce_pnl=ce_entry_price-ce_ltp
                pe_pnl=pe_entry_price-pe_ltp
                intraday_data.loc[index,'ce_pnl']=ce_pnl
                intraday_data.loc[index,'pe_pnl']=pe_pnl
                pnl=ce_pnl+pe_pnl



        trade_log=trade_log.append({'Entry_date_time': entry_datetime,
                                    'Future_price': future_price,
                                    'ATM': atm, 
                                    'Days_to_expiry': (nearest_expiry-entry_datetime.date()).days,
                                    'CE_symbol': ce_symbol,
                                    'CE_entry_price':ce_entry_price,
                                    'CE_exit_price': ce_exit_price,
                                    'CE_exit_datetime': ce_exit_datetime,
                                    'CE_pnl': ce_pnl,
                                    'PE_symbol': pe_symbol,
                                    'PE_entry_price': pe_entry_price,
                                    'PE_exit_price': pe_exit_price,
                                    'PE_exit_datetime': pe_exit_datetime,
                                    'PE_pnl':pe_pnl, 
                                    'Total_pnl': pnl},ignore_index=True)
    except Exception as e:
        print(e)
        print(row['location'])

# COMMAND ----------



# COMMAND ----------

