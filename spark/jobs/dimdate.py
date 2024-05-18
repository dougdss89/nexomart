#importing packages and vars

from pyspark.sql.session import SparkSession
import pyspark.sql.functions as f

import os
from dotenv import load_dotenv

#load environment variables
env = load_dotenv(dotenv_path='/mnt/d/linux/project/vars/.env')

#env vars for postgresql connection
mssqlsource = os.getenv('MSSQLSOURCE')
mssqluser = os.getenv('MSSQL_SPARK_USER')
mssqlpwd = os.getenv('MSSQL_SPARK_PWD')
mssqldriver = os.getenv('MSSQL_DRIVER')
sqlserverjar = os.getenv('SQLSERVER_JAR')
mssql_dwdest = os.getenv('MSSQL_CONN')

#create session 

def createsession():
    
    spark = ((SparkSession
                .builder
                .appName('dimcountry')
                .config('spark.jar', f'{sqlserverjar}')
                .config('spark.driver.extraClassPath', f'{sqlserverjar}')
                .config('spark.executor.extraClassPath', f'{sqlserverjar}')
                .getOrCreate()
                
            ))
    
    return spark

def load_dimdate(spark = createsession(), conn= mssqlsource, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    temp_dimdate = ((
        
                    spark
                        .read
                        .format('jdbc')
                        .option('url', f'{conn}')
                        .option('driver', f'{driver}')
                        .option('user', f'{user}')
                        .option('password', f'{pwd}')
                        .option('query', 
                                        """ 
                                            select distinct

                                                    
                                                    cast(cast(replace(orderdate,'-','') as varchar(10)) as int) as orderdatekey,
                                                    orderdate,
                                                    year(orderdate) as orderyear,
                                                    'FY' + cast(year(orderdate) as varchar(5)) as fiscalyear,
                                                    datename(mm,orderdate) as monthname,
                                                    substring(cast(datename(m, orderdate) as varchar(10)),1,3) as monthabrv,
                                                    datepart(m,orderdate) as monthnum,

                                                    case
                                                        when datepart(m, orderdate) between 1 and 3 then 'Q1'
                                                        when datepart(m, orderdate) between 4 and 6 then 'Q2'
                                                        when datepart(m, orderdate) between 6 and 9 then 'Q3'
                                                        when datepart(m, orderdate) between 9 and 12 then 'Q4'
                                                    end as quartermonth,

                                                    datename(w, orderdate) as weekname,
                                                    substring(cast(datename(w, orderdate) as varchar(10)),1,3) as weeknameabrv,

                                                    case 
                                                        when cast(datename(w, orderdate) as varchar(10)) = 'Saturday' or cast(datename(w, orderdate) as varchar(10)) = 'Sunday' then 'Weekend'
                                                        else 'Weekday'
                                                    end as isweekday,

                                                    datename(ww, orderdate) as weeknuminyear,
                                                    datepart(d, orderdate) as daynum,

                                                    case 
                                                        when datepart(d, orderdate) = 1 then 'Yes'
                                                        else 'No'
                                                    end as ismonthbegin,

                                                    case
                                                        when cast(datename(w, orderdate) as varchar(10)) = 'monday' then 'Yes'
                                                        else 'No'
                                                    end as isweekbegin,

                                                    upper(cast(year(orderdate) as varchar(5)) + '_' + substring(cast(datename(m, orderdate) as varchar(3)),1,3)) as yearmonth,
                                                    cast(datepart(YYYY, orderdate) as varchar(5)) + '' +  cast(month(orderdate) as varchar(5)) as yearmonthnum,
                                                    eomonth(orderdate) as lastdayofmonth,
                                                    
                                                    case
                                                        when cast(orderdate as date) = cast(eomonth(orderdate) as date) then 'Yes'
                                                        --when datepart(m, orderdate) = 2 and datepart(d, orderdate) between 28 and 29 then 'Yes'
                                                    else 'No'
                                                    end as islastdayofmonth,

                                                    case	
                                                        when year(orderdate) % 4 = 0 or year(orderdate) % 400 = 0 and year(orderdate) % 100 <> 0  then 'Yes'
                                                    else 'No'
                                                    end as leapyear
                                                    
                                                from sales.Orders

                                        """).load()))
    return temp_dimdate

def writedimdate(data = load_dimdate(), conn = mssql_dwdest, driver = mssqldriver, user = mssqluser, pwd = mssqlpwd):
    
    ((data
        .write
            .format('jdbc')
            .option('url', conn)
            .option('driver', driver)
            .option('user', user)
            .option('password', pwd)
            .option('dbtable', 'dimension.dimdate')
        .save(mode= 'overwrite')
    ))
    
if __name__ == '__main__':
    
    createsession()
    load_dimdate()
    writedimdate()