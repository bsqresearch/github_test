# -*- coding: utf-8 -*-
"""
Created on Mon Sep 25 09:03:20 2017

@author: divya

Add new CBBC codes to DB
"""

from sqlalchemy import create_engine
import pandas.io.sql as psql
import DBConnection
import datetime
import pandas as pd
import numpy as np
import time
import os
from numba import jit


def updateOrderBook(row,j,otm,opx,oqt,osd,oid,opo):
#    print(j,row)
    ## If orderid is in orderBooK_df then drop
    if row[2] == 30:
        if row[6] in oid:
            ind =  np.where(oid == row[6])[0]
            for i in ind:
                otm[i] = row[1]
                oqt[i] = row[4]
                osd[i] = row[5]
                opo[i] = row[7]
        else:
            j = j + 1
            otm[j] = row[1]
            opx[j] = row[3]
            oqt[j] = row[4]
            osd[j] = row[5]
            oid[j] = row[6]
            opo[j] = row[7]
    elif row[2] == 31:
        if row[6] in oid:
            ind =  np.where(oid == row[6])[0]
            for i in ind:
                otm[i] = row[1]
                oqt[i] = row[4]
                osd[i] = row[5]
                opo[i] = row[7]        
    elif row[2] == 32 or row[2] == 50:
       if row[6] in oid:
            ind =  np.where(oid == row[6])[0]
            for i in ind:
                otm[i] = -1
                opx[i] = -1
                oqt[i] = -1
                osd[i] = -1
                oid[i] = -1
                opo[i] = -1
    
    otm,j = clean_NA(otm)    
    opx,j = clean_NA(opx)    
    oqt,j = clean_NA(oqt)    
    osd,j = clean_NA(osd)    
    oid,j = clean_NA(oid)    
    opo,j = clean_NA(opo)   
    
#    print(j)
    return(j,otm,opx,oqt,osd,oid,opo)

## Function to write data to Excel File
## parameters - dirctory path , filename , sheetname , dataframe 
def writeToExcel(dirpath,filename,sheetname1,df1):
    if not os.path.exists(dirpath):
        os.makedirs(dirpath)  
    filepath = dirpath+'/'+filename+'.xlsx'
    writer = pd.ExcelWriter(filepath, engine='xlsxwriter')
    df1.to_excel(writer,sheetname1)
    worksheet = writer.sheets[sheetname1]
    # Set the column width and format.
    worksheet.set_column('B:M', 12, None)
#    writer.save()
         
def clean_NA(arr):
    n = len(arr)
    hed = arr[arr != -1]
    m = len(hed)
    tel = np.empty(n-m).astype('int64')
    tel[:] = -1
    arr = np.concatenate((hed,tel), axis = 0)
    return (arr, m)
    
    
@jit   
def get_best_quote(j,otm,opx,oqt,osd,opo):
    bidpx, bidsz, bidtm, bidpo, askpx, asksz, asktm, askpo = -1,-1,-1,-1,-1,-1,-1,-1
    for i in range(j):
        if osd[i] == 0:
            if bidpx == -1:
                bidtm,bidpx, bidsz, bidpo = otm[i],opx[i],oqt[i],opo[i]
            else:
                if opx[i] > bidpx:
                    bidtm,bidpx, bidsz, bidpo = otm[i],opx[i],oqt[i],opo[i]
                elif opx[i] == bidpx:
                    if otm[i] > bidtm or opo[i] < bidpo:
                        bidtm,bidpx, bidsz, bidpo = otm[i],opx[i],oqt[i],opo[i]
        elif osd[i] == 1:
            if askpx == -1:
                asktm,askpx,asksz,askpo = otm[i],opx[i],oqt[i],opo[i]
            else:
                if opx[i] < askpx:
                    asktm,askpx,asksz,askpo = otm[i],opx[i],oqt[i],opo[i]
                elif opx[i] == askpx:
                    if otm[i] > asktm or opo[i] < askpo:
                        asktm,askpx,asksz,askpo = otm[i],opx[i],oqt[i],opo[i]
    return(bidpx,bidsz,bidtm,bidpo,askpx,asksz,asktm,askpo)
    
def count_blanks(j,osd):
    bcou = 0
    acou = 0
    for i in range(j):
#        print(j,i,osd[i])
        if osd[i] == 0:
            bcou = bcou + 1
        elif osd[i] == 1:
            acou = acou + 1
    return(bcou,acou)


## function to fetch the rows from database
def fetchRows(currentdate,sid):
    secSQL = 'SELECT  mc30."Date" as date,mc30."Time" as time, \
                mc30."MsgType" as msgtype ,mc30."Price" as price ,\
                mc30."Qty" as qty , mc30."Side" as side,\
                mc30."OrderID" as orderid ,mc30."OrdBookPos" as pos , \
                mc30."SecurityCode" as scode, mc30."Seq" as seq   \
        FROM  "C_'+currentdate+'_R_MC01" as mc01 \
        JOIN "C_'+currentdate+'_fob" as mc30  \
        ON mc01."SecurityCode" = mc30."SecurityCode" \
        WHERE  "SecurityShortName" like '"'__#HSI%'"' \
        AND  mc30."SecurityCode" = ' + str(sid) + ' \
        AND CAST(mc01."SecurityCode" AS TEXT) LIKE '"'6%'"' AND   mc30."MsgType" in (30,31,32,50,51) \
        ORDER By seq,date,time'
#    print(secSQL)
    st1 =  time.time()
    Cursor_hs_mk.execute(secSQL)
    msgRows = Cursor_hs_mk.fetchall()
#    msgRows_df = psql.read_sql(secSQL, hangSengMarket_dbconn)
    print('Time to fetch from DB',round(time.time()-st1,1))
    return msgRows
                
            
def getDates(hangSeng_dbconn):
#    getDateSql = 'SELECT date FROM "Dates_yyyymmdd" ORDER BY index;'
#    cur = hangSeng_dbconn.cursor()
#    cur.execute(getDateSql)
#    dates = list(cur.fetchall())
    dates = ['20170413']
    return dates

if __name__ == '__main__':
    ## Connect to Database
    hangSengMarket_dbconn = DBConnection.SetDB_HANGSENG_MARKET()
    hangSeng_dbconn = DBConnection.SetDB_HANGSENG()
    Cursor_hs = hangSeng_dbconn.cursor()
    Cursor_hs_mk = hangSengMarket_dbconn.cursor()
    
    ## Fetch the dates from DB
    dateList = getDates(hangSeng_dbconn)
#    dateList = ['20170411','20170412','20170413',]
    
    for i in dateList:
        currentdate = i
        print('Date : ',currentdate)
        start_time = time.time()
        st = datetime.datetime.now().time().strftime('%H:%M:%S')
        
        ## 1st set of codes
#        cbbclist = [66740,67005,66263,66983,66559,66816,66405,66867,64065,66709,
#                    66996,65938,66966,65974,67014,66020,66247,66279,65190,66245]
        
        cbbclist = [66405,66867,65880,67195,66703,67251,64083,67214,66708,66996,
                    65938,63590,66113,63678,66020,62833,66819,66247,67388,63704,
                    65907,67420,65114,66816,65933,66184,67377,67005,63858,66740,
                    65151,63593,61833,67101,64290,66643,65190,
                    65974,64065,66245,66263,66279,66559,66709,66983,66966,67014]
                    
                    
        
#        cbbclist = [66263]
        
    
        for sid in cbbclist :
            print('CBBC Code :',sid)
            
            ## Fetch the data for given date
            msgRows = fetchRows(currentdate,sid)
            n = len(msgRows)
            
            
            
            ## create orderbook dataframe
                    
            q = 2000
            otm = np.empty(q).astype('int64')
            opx = np.empty(q).astype('float64')
            oqt = np.empty(q).astype('int64')
            osd = np.empty(q).astype('int64')
            oid = np.empty(q).astype('int64')
            opo = np.empty(q).astype('int64')
            otm[:] = -1
            opx[:] = -1.
            oqt[:] = -1
            osd[:] = -1
            oid[:] = -1
            opo[:] = -1
    
            tme = np.empty(n).astype('int64')
            bpx = np.empty(n).astype('float64')
            bsz = np.empty(n).astype('int64')
            btm = np.empty(n).astype('int64')
            bpo = np.empty(n).astype('int64')
            apx = np.empty(n).astype('float64')
            asz = np.empty(n).astype('int64')
            atm = np.empty(n).astype('int64')
            apo = np.empty(n).astype('int64')
             
            
            
            ## Iterate over each row in resultset
            i = 0
            j = 0
            k = 0
            
            
            for row in msgRows:
                if(row[8] == sid):
    #                if i % 10000 == 0:print(i,j,k,row[1],row[2],row[6],row[7],'Time:', round(time.time()-start_time,1))
                    ## Update the order book
                
                    j,otm,opx,oqt,osd,oid,opo = updateOrderBook(row,j,otm,opx,oqt,osd,oid,opo)                
                 ## Construct and add the Best Quotes
                    if i == 0 and row[2] == 30:
                        tme[k] = row[1]
                        if row[5] == 0:
                            bpx[k],bsz[k],btm[k],bpo[k]= row[3], row[4], row[1], row[7]
                            apx[k],asz[k],atm[k],apo[k] = apx[k-1],asz[k-1],atm[k-1],apo[k-1]
                        else:
                            bpx[k],bsz[k],btm[k],bpo[k] = bpx[k-1],bsz[k-1],btm[k-1],bpo[k-1]
                            apx[k],asz[k],atm[k],apo[k]= row[3], row[4], row[1], row[7]
                    else:
                        ptm = tme[k]
                        if row[1] != ptm:
                        # If current row time is not equal to last tme add a row
                            k = k + 1
                            tme[k] = row[1]
                            if row[2] == 30:
                                if row[5] == 0: 
                                    if row[3] > bpx[k-1] or (row[3] == bpx[k-1] and (row[1] > btm[k-1] or row[7] < bpo[k-1])): 
                                        bpx[k],bsz[k],btm[k],bpo[k] = row[3], row[4], row[1], row[7]
                                        apx[k],asz[k],atm[k],apo[k] = apx[k-1],asz[k-1],atm[k-1],apo[k-1]
                                    else:
                                        bpx[k],bsz[k],btm[k],bpo[k] = bpx[k-1],bsz[k-1],btm[k-1],bpo[k-1]
                                        apx[k],asz[k],atm[k],apo[k] = apx[k-1],asz[k-1],atm[k-1],apo[k-1]
                                elif row[5] == 1:                        
                                    if apx[k-1] ==-1 or row[3] < apx[k-1] or (row[3] == apx[k-1] and (row[1] > atm[k-1] or row[7] < apo[k-1])): 
                                        bpx[k],bsz[k],btm[k],bpo[k] = bpx[k-1],bsz[k-1],btm[k-1],bpo[k-1]
                                        apx[k],asz[k],atm[k],apo[k]= row[3], row[4], row[1], row[7]
                                    else:
                                        bpx[k],bsz[k],btm[k],bpo[k] = bpx[k-1],bsz[k-1],btm[k-1],bpo[k-1]
                                        apx[k],asz[k],atm[k],apo[k] = apx[k-1],asz[k-1],atm[k-1],apo[k-1]
                            else:
                                bpx[k], bsz[k], btm[k], bpo[k], apx[k], asz[k],atm[k], apo[k] = get_best_quote(j,otm,opx,oqt,osd,opo)
                        elif row[1] == ptm:
                        # if current row time is equal to last tme update current row
                            if row[2] == 30:
                                if row[5] == 0: 
                                    if row[3] > bpx[k] or (row[3] == bpx[k] and (row[1] > btm[k] or row[7] < bpo[k])): 
                                        bpx[k],bsz[k],btm[k],bpo[k]= row[3], row[4], row[1], row[7]
                                elif row[5] == 1:                        
                                    if apx[k] ==-1 or row[3] < apx[k] or (row[3] == apx[k] and (row[1] > atm[k] or row[7] < apo[k])): 
                                        apx[k],asz[k],atm[k],apo[k]= row[3], row[4], row[1], row[7]
                            else:
                                bpx[k], bsz[k], btm[k], bpo[k], apx[k], asz[k],atm[k], apo[k] = get_best_quote(j,otm,opx,oqt,osd,opo)
                i = i + 1
    #            if i > 1300: break
            ordcols = ['otm','opx','oqt','osd','oid','opo']               
            orderbk = pd.DataFrame(columns=ordcols)
            orderbk['otm'] = otm
            orderbk['opx'] = opx
            orderbk['oqt'] = oqt
            orderbk['osd'] = osd
            orderbk['oid'] = oid
            orderbk['opo'] = opo
                
            tme = tme[:k+1]
            bpx = bpx[:k+1]
            bsz = bsz[:k+1]
            apx = apx[:k+1]
            asz = asz[:k+1]
            
            bestbook_df = pd.DataFrame(columns=['Time','SecurityCode','BidPx','BidSz','AskPx','AskSz'])
            bestbook_df['Time'] = tme
            bestbook_df['SecurityCode'] = sid
            bestbook_df['BidPx'] = bpx
            bestbook_df['BidSz'] = bsz
            bestbook_df['AskPx'] = apx
            bestbook_df['AskSz'] = asz
    #        del(oid,opo,opx,oqt,osd,otm,ptm,q,tme,bsz,btm,bpo,asz,atm,apo,i,j,k,n)
            
            ## Write to csv
            file = 'C:/Aurora/Divya/HangSeng/BestBook_L1/'+currentdate+'_'+str(sid)+'_09_1_new.csv'
            bestbook_df.to_csv(file, sep=',',header=False)
            
#            ## write to excel format
#            dirpath = 'C:/Aurora/Divya/HangSeng/DB_csvFiles'
#            filename = 'cbbc1_'+str(sid)+'_L1_09_1'
#            writeToExcel(dirpath,filename,filename,bestbook_df)
            
#            print('length',len(bestbook_df))
            
            et = datetime.datetime.now().time().strftime('%H:%M:%S')
            total_time = (datetime.datetime.strptime(et,'%H:%M:%S') - datetime.datetime.strptime(st,'%H:%M:%S'))
                        
            print("Time taken to process cbbc-"+str(sid)+" data ",total_time)
            print("------------------------------------------\n")
    
    hangSeng_dbconn.close()
    hangSengMarket_dbconn.close()