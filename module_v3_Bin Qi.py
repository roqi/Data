import requests, json, os, errno, calendar, sqlalchemy, pandas as pd, numpy as np, glob, pymysql
from sqlalchemy import create_engine
from pandas.tseries.holiday import USFederalHolidayCalendar
from urllib.request import urlretrieve
from datetime import timedelta, date, datetime
from dateutil.rrule import rrule, MONTHLY

##------------------------------------------------------------------------------------------------------
##auto-download files for a range of dates in two folders: dalmp and ftrslt, no repetitive files allowed.
def download_FTR(start_yr, start_mon, start_day, end_yr, end_mon, end_day):
    try:
        start_date = date(start_yr, start_mon, start_day)
        end_date = date(end_yr, end_mon, end_day)
    except:
        return 'Error: day is out of range for month'

    delta = timedelta(days=1)
    ##day ahead LMPs files stored in the new folder "dalmp"
    ##sample url: https://www.iso-ne.com/histRpts/da-lmp/WW_DALMP_ISO_20190524.csv
    while start_date <= end_date:
        url_LMP = "https://www.iso-ne.com/histRpts/da-lmp/WW_DALMP_ISO_" + start_date.strftime("%Y%m%d") + ".csv"
        try:
            fullfilename = os.path.join('dalmp', "WW_DALMP_ISO_" + start_date.strftime("%Y%m%d") + ".csv")
            pathmaker(fullfilename)
            urlretrieve(url_LMP, fullfilename)
        except:
            print("no such date's hourly day ahead LMPs disclosed: " + "WW_DALMP_ISO_" + start_date.strftime(
                "%Y%m%d") + ".csv")
            continue
        start_date += delta

    ###LT FTR Auction results (2 Rounds)
    # sample url: https://www.iso-ne.com/static-assets/documents/2018/12/ftrslt_lt22019.csv
    for i in range(start_yr, end_yr + 1):
        url = "https://www.iso-ne.com/static-assets/documents/"
        url_lt_r1 = url + str(i - 1) + "/11/ftrslt_lt1" + str(i) + ".csv"
        url_lt_r2 = url + str(i - 1) + "/12/ftrslt_lt2" + str(i) + ".csv"
        try:
            # round1
            fullfilename = os.path.join('ftrslt', "ftrslt_lt1" + str(i) + ".csv")
            pathmaker(fullfilename)
            urlretrieve(url_lt_r1, fullfilename)
        except:
            print("no such Annual Long-Term FTR Auction Result disclosed: " + "ftrslt_lt1" + str(i) + ".csv")
            break

        try:
            # round2
            fullfilename = os.path.join('ftrslt', "ftrslt_lt2" + str(i) + ".csv")
            urlretrieve(url_lt_r2, fullfilename)
        except:
            print("no such Annual Long-Term FTR Auction Result disclosed: " + "ftrslt_lt2" + str(i) + ".csv")
            break

    ##Monthly FTR Auction results
    # sample url: https://www.iso-ne.com/static-assets/documents/2019/04/ftrslt_may2019.csv
    start = date(start_yr, start_mon, 1)
    end = date(end_yr, end_mon, 1)
    month_iter = ((calendar.month_abbr[d.month].lower() + str(d.year)) for d in
                  rrule(MONTHLY, dtstart=start, until=end))
    for m in month_iter:
        if m[:3] == 'jan':
            url_mon = "https://www.iso-ne.com/static-assets/documents/" + str(int(m[-4:]) - 1) + "/" + str(
                12) + "/ftrslt_" + m + ".csv"
        else:
            url_mon = "https://www.iso-ne.com/static-assets/documents/" + str(int(m[-4:])) + "/" + "{:02d}".format(
                [x.lower() for x in list(calendar.month_abbr)].index(m[:3]) - 1) + "/ftrslt_" + m + ".csv"

        try:
            fullfilename = os.path.join('ftrslt', "ftrslt_" + m + ".csv")
            pathmaker(fullfilename)
            urlretrieve(url_mon, fullfilename)
        except:
            print("no such monthly FTR Auction Result disclosed: " + "ftrslt_" + m + ".csv")
            break
        
def pathmaker(filename):
    if not os.path.exists(os.path.dirname(filename)):
        try:
            os.makedirs(os.path.dirname(filename))
        except OSError as exc:  # Guard against race condition
            if exc.errno != errno.EEXIST:
                raise

#test case: 20190524
download_FTR(2019, 5, 24, 2019, 5, 30)

##-----------------------------------------------------------------------
#create database 'FTR'
name = 'FTR'
db = pymysql.connect("localhost","root","password" )##specify your password
cursor = db.cursor()
query = 'DROP DATABASE IF EXISTS ' + name + ';'
cursor.execute(query)
query = "CREATE DATABASE IF NOT EXISTS " + name + ';'
cursor.execute(query)
query = "USE " + name + ';'
cursor.execute(query)
db.commit()
db.close()



##----------------------------------------------------------------------------
#NERC Off-Peak holidays
class nercCalendar(AbstractHolidayCalendar):
    rules = [
        Holiday('NewYearsDay', month=1, day=1, observance=nearest_workday),
        USMemorialDay,
        Holiday('USIndependenceDay', month=7, day=4, observance=nearest_workday),
        USLaborDay,
        USThanksgivingDay,
        Holiday('Christmas', month=12, day=25, observance=nearest_workday)
    ]

def get_NERC_holidays(start_yr, end_yr):
    inst = nercCalendar()
    return inst.holidays(date(start_yr-1, 12, 31), date(end_yr, 12, 31))


#if __name__ == '__main__':
    #print(get_NERC_holidays(2019, 2019))


# pre-processing daily LMPs data, converting 'Hour Ending' to 'ONPEAK' and 'OFFPEAK' (Class Type)
engine = create_engine("mysql://root:password@localhost/FTR") # create_engine("mysql://USER:PASSWORD@HOST/DATABASE")
con = engine.connect()

#write daily LMPs files into database
dalmp_files = glob.glob('dalmp' + "/*.csv")
#get NERC holidays: specify the years investigated, here using 2019 as an example
holidays = get_NERC_holidays(2019, 2019)

for filename in dalmp_files:
    df = pd.read_csv(filename, skiprows=[i for i in range(0,4)]+[5], skipfooter = 1, index_col=None, header=0,engine='python')
    df = df.drop('H', axis=1)
    df['Date'] = pd.to_datetime(df['Date'], format='%m/%d/%Y').apply(lambda x: x.date())
    date_time = df['Date'].iloc[0]
    
    if date_time.weekday()<5 and date_time not in holidays:#weekday
        df['Class Type'] =np.where((df['Hour Ending'] <= 23) & (df['Hour Ending'] >= 8),"ONPEAK","OFFPEAK")
    else:
        df['Class Type'] = "OFFPEAK"
        
    schema_type = {'Date': sqlalchemy.types.DateTime(),\
                   'Hour Ending':  sqlalchemy.types.INTEGER(),\
                   'Location ID':  sqlalchemy.types.INTEGER(),\
                   'Location Name': sqlalchemy.types.NVARCHAR(length=255),\
                   'Location Type': sqlalchemy.types.NVARCHAR(length=255),\
                   'Locational Marginal Price': sqlalchemy.types.Float(precision=2, asdecimal=True),\
                   'Energy Component': sqlalchemy.types.Float(precision=2, asdecimal=True),\
                   'Congestion Component': sqlalchemy.types.Float(precision=2, asdecimal=True),\
                   'Marginal Loss Component': sqlalchemy.types.Float(precision=2, asdecimal=True),\
                   'Class Type': sqlalchemy.types.NVARCHAR(length=255)}
    
        
    df.to_sql(con=con, name='dalmp', if_exists='append', dtype = schema_type)


#write FTR Auction files into database
ftrslt_files = glob.glob('ftrslt' + "/*.csv")
for filename in ftrslt_files:
    df_2 = pd.read_csv(filename, skiprows=[i for i in range(0,2)], skipfooter = 1, index_col=None, header=0,engine='python')
    df_2 = df_2.iloc[:,4:]
    df_2['FTR Date'] = df_2['FTR Auction'].apply(lambda x: datetime.strptime(x[-4:], '%Y').strftime('%Y') if x[:2] == 'LT' else datetime.strptime(x, '%b-%Y').strftime('%Y-%m'))
    schema_type_2 = {'FTR Date': sqlalchemy.NVARCHAR(length=255),\
                     'FTR Auction': sqlalchemy.types.NVARCHAR(length=255),\
                     'Customer ID':  sqlalchemy.types.INTEGER(),\
                     'Customer Name': sqlalchemy.types.NVARCHAR(length=255),\
                     'Source Location Id':  sqlalchemy.types.INTEGER(),\
                     'Source Location Name': sqlalchemy.types.NVARCHAR(length=255),\
                     'Source Location Type': sqlalchemy.types.NVARCHAR(length=255),\
                     'Sink Location Id':  sqlalchemy.types.INTEGER(),\
                     'Sink Location Name': sqlalchemy.types.NVARCHAR(length=255),\
                     'Sink Location Type': sqlalchemy.types.NVARCHAR(length=255),\
                     'Buy/Sell': sqlalchemy.types.NVARCHAR(length=255),\
                     'Class Type': sqlalchemy.types.NVARCHAR(length=255),\
                     'Award FTR MW': sqlalchemy.types.Float(precision=2, asdecimal=True),\
                     'Award FTR Price': sqlalchemy.types.Float(precision=2, asdecimal=True)}
    df_2.to_sql(con=con, name='ftrslt', if_exists='append', dtype = schema_type_2)  


##----------------------------------------------------------------------------------------------
#SQL queries to calculate profit for each company on each day

#generate the daily ftr auction results by aggregating on date, Customer Name, Soruce Location Id, Sink Location Id, Buy/Sell, Class Type
query1 = """
drop table if exists ftr_agg_daily; 
create table ftr_agg_daily 
select fd.`FTR Date`, fd.`Customer Name`, fd.`Source Location Id`, fd.`Sink Location Id`, fd.`Buy/Sell`, fd.`Class Type`, sum(fd.`Award FTR MW`)/avg(fd.day_count) as `Award FTR MW`, sum(fd.`Award FTR Price`)/avg(fd.day_count) as `Award FTR Price` from (select *, case when length(`FTR Date`) > 4 then (day(last_day(CONCAT(`FTR Date`, '-01')))) else (DAYOFYEAR(CONCAT(`FTR Date`, '-12-31'))) END as day_count from ftrslt) as fd
group by fd.`FTR Date`, fd.`Customer Name`, fd.`Source Location Id`, fd.`Sink Location Id`, fd.`Buy/Sell`, fd.`Class Type`;
"""
con.execute(query1)


#generate the daily congestion component by aggregating on date，Location Id, Class Type
query2 = """
drop table if exists dalmp_agg_daily;
create table dalmp_agg_daily
select  `Date`, `Location ID`, `Class Type`, sum(`Congestion Component`) as `Congestion Component` from `dalmp`
group by `Date`, `Location ID`, `Class Type`
"""
con.execute(query2)

#calculate the total profit each day for each customer, classified by 'BUY' and 'SELL'
query3 = """
drop table if exists buy_sell_profit;
create table buy_sell_profit
select d1.`Date`, f.`Customer Name`, f.`Buy/Sell`, case when f.`Buy/Sell` = 'BUY' then ((d2.`Congestion Component` - d1.`Congestion Component`) * f.`Award FTR MW` - f.`Award FTR Price`) else (-(d2.`Congestion Component` - d1.`Congestion Component`) * f.`Award FTR MW` + f.`Award FTR Price`) END as `Profit` from ftr_agg_daily as f
join dalmp_agg_daily as d1 on f.`Source Location Id` = d1.`Location Id` and f.`Class Type` = d1.`Class Type` and (length(f.`FTR Date`) > 4 and f.`FTR Date` =  DATE_FORMAT(d1.`Date`, "%%Y-%%m") or length(f.`FTR Date`) = 4 and f.`FTR Date` = DATE_FORMAT(d1.`Date`, "%%Y"))
join dalmp_agg_daily as d2 on f.`Sink Location Id` = d2.`Location Id` and f.`Class Type` = d2.`Class Type` and (length(f.`FTR Date`) > 4 and f.`FTR Date` =  DATE_FORMAT(d2.`Date`, "%%Y-%%m") or length(f.`FTR Date`) = 4 and f.`FTR Date` = DATE_FORMAT(d2.`Date`, "%%Y")) and d1.Date = d2.Date;
"""
con.execute(query3)

#calculate the final daily PNL for each customer
query4 = """
drop table if exists profits;
create table profits
select `Date`, `Customer Name`, sum(`Profit`) as profit from buy_sell_profit
group by `Date`, `Customer Name`;
"""
con.execute(query4)

con.close()

def search_profit(year, month, day, customer_name):
    customer_name = str(customer_name)
    
    date_day = date(year, month, day).strftime('%Y-%m-%d')
    name = 'FTR'
    db = pymysql.connect("localhost","root","PASSWORD")#specify your password
    cursor = db.cursor()
    query = "USE " + name + ';'
    cursor.execute(query)
    
    query = 'SELECT * FROM profits where `Customer Name` = "' + customer_name + '" and `Date` = "'+  date_day  + '";'
    cursor.execute(query)
    table = cursor.fetchall()
    return table
   
#sample: search_profit(2019, 5, 27, 'Black Bear Hydro Partners_ LLC')
