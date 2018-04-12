# -*- coding: utf-8 -*-
"""
Created on Mon Nov 27 05:44:23 2017

@author: divya
"""

import DBConnection
import datetime as dt
import numpy as np
import pandas as pd
import bisect
import os
import openpyxl
import pandas.io.sql as psql

def writeToExcel(filename,df1,dirpath):
    
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)  
        
    filepath = dirpath+'/'+filename+'.xlsx'
    # create a writer 
    writer = pd.ExcelWriter(filepath, engine='openpyxl')

    try:
        # try to open an existing workbook
        writer.book = openpyxl.load_workbook(filename)

        # copy existing sheets
        writer.sheets = dict(
            (ws.title, ws) for ws in writer.book.worksheets)
    except IOError:
        # file does not exist , create it
        pass
    
    sheetname = filename
    # write out the new sheet
    df1.to_excel(writer, sheetname)
    # save the workbook
    writer.save()


def combine_fut_cbbc(sec_df,d_df):
    st = dt.datetime.now().time().strftime('%H:%M:%S')
    
    u_df = sec_df.append(d_df,ignore_index=True).sort_values('time').reset_index()

    tme = np.array(u_df.time)
    fpx = np.array(u_df.futpx)
    bpx = np.array(u_df.bidpx)
    apx = np.array(u_df.askpx)
    
    xlist = []
    for i in range(1,len(u_df)):
        if tme[i] == tme[i-1]:
            bpx[i-1] = bpx[i-1] + bpx[i]
            apx[i-1] = apx[i-1] + apx[i]
            fpx[i-1] = fpx[i-1] + fpx[i]
            xlist.append(i)
    tme = np.floor(tme/1000.)
    

## Replace 0's with previous 
    for ar  in range(1,len(u_df)):
        if fpx[ar] == 0:
            fpx[ar] = fpx[ar-1]
        if bpx[ar] == 0:
            bpx[ar] = bpx[ar-1]
        if apx[ar] == 0:
            apx[ar] = apx[ar-1]
    
    u_df.time = tme
    u_df.futpx = fpx
    u_df.bidpx = bpx
    u_df.askpx = apx
                              
    u_df = u_df.drop(xlist).reset_index()
    
    ## Remove the top 0  value rows from dataframe
    u_df = u_df[u_df['bidpx'] != 0]

    print('***********')
    et = dt.datetime.now().time().strftime('%H:%M:%S')
    tt = (dt.datetime.strptime(et,'%H:%M:%S') - dt.datetime.strptime(st,'%H:%M:%S'))
    print("Time to combine : ",tt, 'Length : ',len(u_df))
    
    return u_df
    
def split_fut_times(d_df):
    mtime = np.zeros(len(d_df)).astype(np.int64)
    mtime[0] = d_df.time[0]*1000    
    for k in range(1,len(d_df)):
        if d_df.time[k] == d_df.time[k-1]:
            mtime[k] = mtime[k-1]+1
        else:
            mtime[k] = d_df.time[k] * 1000
    d_df.time = mtime
    return()

def split_sec_times(sec_df):
    mtime = np.zeros(len(sec_df)).astype(np.int64)
    mtime[0] = sec_df.time[0]*1000
    for l in range(1,len(sec_df)):
        if sec_df.time[l] == sec_df.time[l-1]:
            mtime[l] = mtime[l-1]+1
        else:
            mtime[l] = sec_df.time[l] * 1000
    sec_df.time = mtime
    return()

## Function to fetch Securities Data
def fetchSecData(hangSeng_dbconn,ranDate,fTime,lTime,cbbc,ent):
    sec_sql_cbbc1 = ' SELECT \
                    "tm" as time, "bidpx", "askpx" \
                    FROM \
                    "sc_'+ranDate+'"  \
                    WHERE  \
                    "tm" BETWEEN '+ fTime+' AND '+ lTime +' AND "askpx" != -1 AND\
                    "scode" = '+cbbc+' ORDER BY "tm" '
#    print(sec_sql_cbbc1)
    ## Read the SQL Result into DF
    sec_df_cbbc1 = psql.read_sql(sec_sql_cbbc1, hangSeng_dbconn)
    return sec_df_cbbc1


def getTrdData(ranDate,hangSengMarket_dbconn,fTime,lTime):
    sql_st = dt.datetime.now().time().strftime('%H:%M:%S')
    trdpxSQL = 'SELECT "Time" as time,"Price" as futpx \
                FROM "trd_der_'+ranDate+'" \
                WHERE  "Time" BETWEEN '+fTime+' AND '+ lTime +' \
                ORDER BY "Time" , seq '
    trd_df = psql.read_sql(trdpxSQL, hangSengMarket_dbconn)
    sql_et = dt.datetime.now().time().strftime('%H:%M:%S')
    sql_tt = (dt.datetime.strptime(sql_et,'%H:%M:%S') - dt.datetime.strptime(sql_st,'%H:%M:%S'))
    print('Trd data fetched, Time : ',sql_tt,' rowcount:',len(trd_df))
    return trd_df
    
## Function to get the Entitlement ratio for the given cbbc code
def getEntitlementRatio(cbbc_code):
    entratio_sql = 'SELECT "BullBear","EntRatio" ,"StrikePrice","Issuer","SecurityShortName" from "CBBC_Codes" where "SecurityCode" = '+ str(cbbc_code)
    cur = hangSeng_dbconn.cursor()
    cur.execute(entratio_sql)
    res = cur.fetchone()
    return res



#start_time = '093800223'
#end_time = '093800551'

#start_time = '94040292'
#end_time = '94040680'

start_time = '093500000'
end_time = '144500000'


## Set the databatesase connection
hangSeng_dbconn = DBConnection.SetDB_HANGSENG()
hangSengMarket_dbconn = DBConnection.SetDB_HANGSENG_MARKET()

## Get  date from Database
#datelist = getAllDates(hangSeng_dbconn)
datelist = ['20170406']

fTime = start_time    
lTime = end_time
    

for i in datelist:
    dst = dt.datetime.now().time().strftime('%H:%M:%S')
    ranDate = i
    s_date = str(i).split("'")
    ranDate = s_date[0]
    print('Date:',ranDate,'Time:',fTime,'-',lTime)
    
    ## Fetch the Derivatives data
    d_df = getTrdData(ranDate,hangSengMarket_dbconn,fTime,lTime)
    split_fut_times(d_df)
    d_df['bidpx'] = np.zeros(len(d_df))
    d_df['askpx'] = np.zeros(len(d_df))

    ## Fetch the cbbc code list for the given date
    if ranDate == '20170403':
        cbbclist = [66086,66413] #
    elif ranDate == '20170405':
        cbbclist = [66413,65974]
    elif ranDate == '20170406':
        cbbclist = [65791,66271]
     
    calc_df = pd.DataFrame() 
    sec_df_all = pd.DataFrame() 
    
    for cbbc in cbbclist:
        ## Get the entitlement ratio,bull/bear,ent,strikeprice for the given cbbc code
        data = getEntitlementRatio(cbbc)
        bullbear = data[0]
        ent = data[1]
        strikeprice = data[2]
        issuer = data[3]
        shortname = data[4]

        ## Fetch the Securities data
        sec_df = fetchSecData(hangSeng_dbconn,ranDate,fTime,lTime,str(cbbc),ent)
        split_sec_times(sec_df)
        sec_df['futpx'] = np.zeros(len(sec_df))
        
        ## Combine Securities and Futures Data
        union_df =  combine_fut_cbbc(sec_df,d_df)
        
#        calc_df = calc_df.append(union_df)
#        sec_df_all = sec_df_all.append(sec_df)
#        print('Length of Sec DF  :',len(sec_df))
        dirpath = 'C:/Aurora/Divya/HangSeng/Strategy_output'
        filename = 'u_df_'+ranDate+'_'+str(cbbc)
        writeToExcel(filename,union_df,dirpath)
        
        filename = 'sec_df_'+ranDate+'_'+str(cbbc)
        writeToExcel(filename,sec_df,dirpath)
        
        
    
    
    ## write data to excel     
    
    filename = 'fut_df_'+ranDate
    writeToExcel(filename,d_df,dirpath)

            
        
                
    
    
    