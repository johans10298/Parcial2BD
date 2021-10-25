import json 
import boto3
import requests
import datetime
import requests
from bs4 import BeautifulSoup
import pandas as pd
import wget
s3= boto3.client('s3')
def handler(event,context):
    date = datetime.datetime.now()
    year= date.year
    month= date.month
    day = date.day
    ts= int(datetime.datetime(year, month,day,00,00,00).timestamp())
    print(ts)
    url = "https://query1.finance.yahoo.com/v7/finance/download/AVHOQ?period1="+str(1634860800)+"&period2="+str(1635033600)+"&interval=1d&events=history&includeAdjustedClose=true"
    print(url)
    wget.download(url, '/tmp/avianca.csv')  
    url1 = "https://query1.finance.yahoo.com/v7/finance/download/EC?period1="+str(1634860800)+"&period2="+str(1635033600)+"&interval=1d&events=history&includeAdjustedClose=true"
    wget.download(url1, '/tmp/ecopetrol.csv') 
    url2 = "https://query1.finance.yahoo.com/v7/finance/download/AVAL?period1="+str(1634860800)+"&period2="+str(1635033600)+"&interval=1d&events=history&includeAdjustedClose=true"
    wget.download(url2, '/tmp/Aval.csv') 
    url3 = "https://query1.finance.yahoo.com/v7/finance/download/CMTOY?period1="+str(1634860800)+"&period2="+str(1635033600)+"&interval=1d&events=history&includeAdjustedClose=true"
    wget.download(url3, '/tmp/Argos.csv')  

    url4 = "stocks/company=avianca/year="+str(year)+"/month="+str(month)+"/day="+str(day)+"/avianca.csv"
    url5 = "stocks/company=Ecopetrol/year="+str(year)+"/month="+str(month)+"/day="+str(day)+"/Ecopetrol.csv"
    url6 = "stocks/company=Aval/year="+str(year)+"/month="+str(month)+"/day="+str(day)+"/Aval.csv"
    url7 = "stocks/company=Argos/year="+str(year)+"/month="+str(month)+"/day="+str(day)+"/Argos.csv"
    s3.upload_file("/tmp/avianca.csv","parcial-2c",url4)
    s3.upload_file("/tmp/ecopetrol.csv","parcial-2c",url5)
    s3.upload_file("/tmp/Aval.csv","parcial-2c",url6)
    s3.upload_file("/tmp/Argos.csv","parcial-2c",url7)
    return {'statusCode' : 200}


def scrap(event, context):
    bucket_name = 'parcial-2c'
    client = boto3.client('athena')
    config = {
        'OutputLocation': 's3://' + bucket_name +'stocks/company=avianca/year=2021/month=10/',
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
    # Query Execution Parameters
    sql = 'MSCK REPAIR TABLE avianca'
    context = {'Database': 'parcial1'}
    client.start_query_execution(QueryString = sql, 
                                 QueryExecutionContext = context,
                                 ResultConfiguration = config)
   



    config = {
        'OutputLocation': 's3://' + bucket_name +'/stocks/company=Ecopetrol/year=2021/month=10/',
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
    # Query Execution Parameters
    sql = 'MSCK REPAIR TABLE Ecopetrol'
    context = {'Database': 'parcial1'}

    client.start_query_execution(QueryString = sql, 
                                 QueryExecutionContext = context,
                                 ResultConfiguration = config)

    config = {
        'OutputLocation': 's3://' + bucket_name +'/stocks/company=Aval/year=2021/month=10/',
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
    # Query Execution Parameters
    sql = 'MSCK REPAIR TABLE Aval'
    context = {'Database': 'parcial1'}

    client.start_query_execution(QueryString = sql, 
                                 QueryExecutionContext = context,
                                 ResultConfiguration = config)


    config = {
        'OutputLocation': 's3://' + bucket_name +'/stocks/company=Argos/year=2021/month=10/',
        'EncryptionConfiguration': {'EncryptionOption': 'SSE_S3'}
    }
    # Query Execution Parameters
    sql = 'MSCK REPAIR TABLE Argos'
    context = {'Database': 'parcial1'}

    client.start_query_execution(QueryString = sql, 
                                 QueryExecutionContext = context,
                                 ResultConfiguration = config)