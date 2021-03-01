# -*- coding: utf-8 -*-
"""The file contains ETL-steps."""

# imports
import sys
import re
import os
import shutil
import logging
import time
import boto3
import pandas as pd
import gzip
import zipfile
import requests
import configparser
import psycopg2
from datetime import datetime, date
from urllib.request import urlopen, Request
import pandas_datareader.data as web
from io import BytesIO, StringIO
from infrastructure import create_tables, drop_tables
from redshift_queries import create_staging_table_queries, copy_table
from redshift_queries import upsert_table_queries

path_settings = 'aws.cfg'
path_tmp = 'tmp'

path_resales = 'https://www.bondora.com/marketing/media/ResaleArchive.zip'
path_loans = 'https://www.bondora.com/marketing/media/LoanData.zip'

# set start date as a minimal date from the resales dataframe
from_date = '2010-01-01'


def set_logger(file_name=None):
    """
    Set logger to write logs in a file or in stdout.

    Parameters
    ----------
    file_name : string
        Name of the file to write logs.

    Returns
    -------
    Logger object.

    """
    logger = logging.getLogger(__name__)
    formatter = logging.Formatter(
        ('%(asctime)s %(threadName)-8s %(name)-8s '
         '%(funcName)-8s %(levelname)-6s %(message)s'),
        datefmt='%d.%m.%Y %H:%M:%S')
    handler = logging.StreamHandler(sys.stdout)
    error = ''
    if isinstance(file_name, str):
        try:
            handler = logging.FileHandler(file_name)
        except FileNotFoundError as e:
            error = e
    elif file_name is not None:
        error = 'Parameter "{}" is not a string'.format(file_name)

    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(logging.DEBUG)

    if error:
        logger.error(error)

    return logger


# create logger
logger = set_logger()


def read_zip(file_name):
    """
    Read zipped csv-file into pandas dataframe.

    Parameters
    ----------
    file_name : string
        The path to zip-archive.

    Returns
    -------
    Pandas dataframe containing data from archived csv-file.

    """
    with zipfile.ZipFile(file_name) as zip_file:
        try:
            df = pd.read_csv(zip_file.open(zip_file.infolist()[0].filename))
            return df
        except Exception as e:
            logger.error(e)


def download_transactions(url):
    """
    Download transactions file from internet.

    Parameters
    ----------
    url : string
        URL of file with transactions.

    Returns
    -------
    file : object of BytesIO class
        Binary stream containing downloaded file.

    """
    headers = {'User-Agent': ('Mozilla/5.0 (X11; Linux x86_64) '
                              'AppleWebKit/537.11 (KHTML, like Gecko) '
                              'Chrome/23.0.1271.64 Safari/537.11'),
               'Accept': ('text/html,application/xhtml+xml,'
                          'application/xml;q=0.9,*/*;q=0.8'),
               'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
               'Accept-Encoding': 'none',
               'Accept-Language': 'en-US,en;q=0.8',
               'Connection': 'keep-alive'}

    try:
        request = Request(url, None, headers)
        response = urlopen(request)
        file = BytesIO(response.read())
        return file

    except Exception as e:
        logger.error(e)


def transform_resales(df):
    """
    Transform resales dataframe.

    Parameters
    ----------
    df : pandas dataframe
        Pandas dataframe to transform.

    Returns
    -------
    df : pandas dataframe
        Transformed dataframe.

    """
    # select important columns and
    # rows corresponding to successful transactions ('Result' = 'Successful')
    columns = ['loan_id', 'PrincipalAtEnd',
               'DiscountRate', 'StartDate', 'EndDate']
    df = df.loc[df['Result'] == 'Successful', columns]

    # remove '{' and '}' from 'loan_id' column
    df['loan_id'] = df['loan_id'].apply(lambda x: re.sub(r'[\{\}]', '', x))

    # convert column type from string to datetime format
    df['StartDate'] = pd.to_datetime(df['StartDate'])
    df['EndDate'] = pd.to_datetime(df['EndDate'])

    # calculate time (in seconds) how long a loan was offered for sale
    df['OfferTime'] = (df['EndDate'] - df['StartDate']).astype(
        'timedelta64[s]')

    return df


def get_loans(path):
    """
    Read loans data from a zipped file.

    Parameters
    ----------
    path : string
        File path on local machine or URL of file with resales.

    Returns
    -------
    df : pandas dataframe
        Pandas dataframe containing loans data.

    """
    headers = {'User-Agent': ('Mozilla/5.0 (X11; Linux x86_64) '
                              'AppleWebKit/537.11 (KHTML, like Gecko) '
                              'Chrome/23.0.1271.64 Safari/537.11'),
               'Accept': ('text/html,application/xhtml+xml,'
                          'application/xml;q=0.9,*/*;q=0.8'),
               'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
               'Accept-Encoding': 'none',
               'Accept-Language': 'en-US,en;q=0.8',
               'Connection': 'keep-alive'}

    try:
        request = Request(path, None, headers)
        response = urlopen(request)
        df = read_zip(BytesIO(response.read()))
        df.name = 'Loans'
        return df
    except Exception as e:
        logger.error(e)


def split_loans(df):
    """
    Split loans dataframe into two with loans and borrowers data.

    Parameters
    ----------
    df : pandas dataframe
        Pandas dataframe to transform.

    Returns
    -------
    df_loans : pandas dataframe
        Transformed dataframe with loans data.
    df_borrowers : pandas dataframe
        Transformed dataframe with borrowers data.

    """
    # select important columns and split dataframe
    columns_loans = ['LoanId', 'UserName', 'Amount', 'Interest',
                     'LoanDuration', 'MonthlyPayment', 'UseOfLoan',
                     'VerificationType', 'LoanDate', 'DefaultDate',
                     'DebtOccuredOn', 'LastPaymentOn']
    columns_borrowers = ['UserName', 'LoanApplicationStartedDate', 'Age',
                         'Gender', 'Country', 'Education', 'MaritalStatus',
                         'NrOfDependants', 'EmploymentStatus',
                         'OccupationArea', 'HomeOwnershipType', 'IncomeTotal',
                         'LiabilitiesTotal', 'DebtToIncome', 'FreeCash']
    df_loans = df.loc[:, columns_loans]
    df_borrowers = df.loc[:, columns_borrowers]

    # convert column type from string to datetime format
    df_loans['LoanDate'] = pd.to_datetime(df_loans['LoanDate'])
    df_loans['DefaultDate'] = pd.to_datetime(df_loans['DefaultDate'])
    df_loans['DebtOccuredOn'] = pd.to_datetime(df_loans['DebtOccuredOn'])
    df_loans['LastPaymentOn'] = pd.to_datetime(df_loans['LastPaymentOn'])
    df_borrowers['LoanApplicationStartedDate'] = pd.to_datetime(df_borrowers['LoanApplicationStartedDate'])

    # fill NaN with -1
    df_loans['VerificationType'] = df_loans['VerificationType'].fillna(-1)
    df_loans['UseOfLoan'] = df_loans['UseOfLoan'].fillna(-1)
    df_borrowers['Gender'] = df_borrowers['Gender'].fillna(-1)
    df_borrowers['Education'] = df_borrowers['Education'].fillna(-1)
    df_borrowers['MaritalStatus'] = df_borrowers['MaritalStatus'].fillna(-1)
    df_borrowers['NrOfDependants'] = df_borrowers['NrOfDependants'].fillna(-1)
    df_borrowers['EmploymentStatus'] = df_borrowers['EmploymentStatus'].fillna(-1)
    df_borrowers['OccupationArea'] = df_borrowers['OccupationArea'].fillna(-1)
    df_borrowers['HomeOwnershipType'] = df_borrowers['HomeOwnershipType'].fillna(-1)

    # replace wrong numbers with -1 and convert column type to integer
    df_loans['VerificationType'] = df_loans['VerificationType'].apply(
        lambda x: -1 if x <= 0.0 else int(x))
    df_borrowers['Gender'] = df_borrowers['Gender'].apply(
        lambda x: -1 if x == 2.0 else int(x))
    df_borrowers['Education'] = df_borrowers['Education'].apply(
        lambda x: -1 if x <= 0.0 else int(x))
    df_borrowers['MaritalStatus'] = df_borrowers['MaritalStatus'].apply(
        lambda x: -1 if x <= 0.0 else int(x))
    df_borrowers['NrOfDependants'] = df_borrowers['NrOfDependants'].apply(
        lambda x: -1 if x == '10Plus' else int(x))
    df_borrowers['EmploymentStatus'] = df_borrowers['EmploymentStatus'].apply(
        lambda x: -1 if x <= 0.0 else int(x))
    df_borrowers['OccupationArea'] = df_borrowers['OccupationArea'].apply(
        lambda x: -1 if x <= 0.0 else int(x))
    df_borrowers['HomeOwnershipType'] = df_borrowers['HomeOwnershipType'].apply(
        lambda x: -1 if x < 0.0 else int(x))

    # drop duplicated users not taking into account columns
    # 'LoanApplicationStartedDate' and 'Age'
    columns_borrowers.remove('LoanApplicationStartedDate')
    columns_borrowers.remove('Age')
    df_borrowers.drop_duplicates(subset=columns_borrowers,
                                 keep='first', inplace=True)

    return df_loans, df_borrowers


def get_euro_stoxx_50(data_source='yahoo', start_date='2010-01-01', end_date=''):
    """
    Read EURO STOXX 50 historical data from internet.

    Parameters
    ----------
    data_source : string
        Data provider.
    start_date : string
        First date of the data.
    end_date : string
        Last date of the data.

    Returns
    -------
    df : pandas dataframe
        Pandas dataframe containing historical EURO STOXX 50 data.

    """
    ticker = '^STOXX50E'

    # if there is no end_date, set it as today
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')

    try:
        # read historical data of EURO STOXX 50
        df = web.DataReader(ticker, data_source, start_date, end_date)

        # select only date and close price columns
        df.reset_index(drop=False, inplace=True)
        df = df.loc[:, ['Date', 'Close']]
        df['Date'] = df['Date'].dt.date
        return df

    except Exception as e:
        logger.error(e)


def get_omx_tallinn(start_date='2010-01-01', end_date=''):
    """
    Read OMX Tallinn historical data from internet.

    Parameters
    ----------
    start_date : string
        First date of the data.
    end_date : string
        Last date of the data.

    Returns
    -------
    df : pandas dataframe
        Pandas dataframe containing historical OMX Tallinn data.

    """
    url = ('https://nasdaqbaltic.com/statistics/en/charts/download?'
           'filter=1&indexes%5B0%5D=OMXTGI&start={}&end={}&_=1604482897599')

    headers = {'User-Agent': ('Mozilla/5.0 (X11; Linux x86_64) '
                              'AppleWebKit/537.11 (KHTML, like Gecko) '
                              'Chrome/23.0.1271.64 Safari/537.11'),
               'Accept': ('text/html,application/xhtml+xml,'
                          'application/xml;q=0.9,*/*;q=0.8'),
               'Accept-Charset': 'ISO-8859-1,utf-8;q=0.7,*;q=0.3',
               'Accept-Encoding': 'none',
               'Accept-Language': 'en-US,en;q=0.8',
               'Connection': 'keep-alive'}

    # if there is no end_date, set it as today
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')

    try:
        #  read historical data of OMX Tallinn
        request = Request(url.format(start_date, end_date), None, headers)
        response = urlopen(request)
        df = pd.read_excel(BytesIO(response.read()))

        # select only date and price columns
        df = df.loc[1:, ['Date', 'Value']]
        df['Date'] = pd.to_datetime(df['Date']).dt.date
        return df

    except Exception as e:
        logger.error(e)


def read_market(start_date='2010-01-01', end_date=''):
    """
    Read historical data of OMX Tallinn and EURO STOXX 50 from internet
    and combine in one dataframe.

    Parameters
    ----------
    start_date : string
        First date of the data.
    end_date : string
        Last date of the data.

    Returns
    -------
    df : pandas dataframe
        Pandas dataframe containing historical OMX Tallinn and
        EURO STOXX 50 data.

    """
    # if there is no end_date, set it as today
    if not end_date:
        end_date = datetime.now().strftime('%Y-%m-%d')

    # get EURO STOXX 50 data
    df_estoxx = get_euro_stoxx_50(start_date=start_date, end_date=end_date)

    # get OMX Tallinn data
    df_omx = get_omx_tallinn(start_date=start_date, end_date=end_date)

    # merge dataframes to one
    df_joined = None
    try:
        df_joined = pd.merge(df_estoxx, df_omx, on='Date', how='outer')
        df_joined.sort_values(by='Date', axis=0, ascending=True, inplace=True)
        df_joined.reset_index(drop=True, inplace=True)

    except Exception as e:
        logger.error(e)

    # rename columns
    try:
        df_joined = df_joined.rename(
            columns={'Close': 'ESTOXX', 'Value': 'OMX'})
    except Exception as e:
        logger.error(e)

    # calculate daily percentage change of the index
    try:
        df_joined['dESTOXX'] = 100 * (df_joined['ESTOXX'] - df_joined['ESTOXX'].shift(1))/df_joined['ESTOXX'].shift(1)
        df_joined['dOMX'] = 100 * (df_joined['OMX'] - df_joined['OMX'].shift(1))/df_joined['OMX'].shift(1)
        df_joined = df_joined.loc[:, ['Date', 'OMX', 'dOMX', 'ESTOXX', 'dESTOXX']]

        return df_joined

    except Exception as e:
        logger.error(e)


def quarter_to_month(str_date):
    """
    Transform string representing year and quarter in date.

    Parameters
    ----------
    str_date : string
        String representing year (YYYY) and quarter (qq) as YYYYQq.

    Returns
    -------
    date : datetime.date
        Transformed date.

    """
    format_string = '%YQ%m'
    try:
        # convert string to date
        d = datetime.strptime(str_date, format_string)
        day = d.day
        month = d.month * 3 - 2
        year = d.year
        return date(year, month, day)
    except Exception as e:
        logger.error(e)

    return d


def read_eurostat(version='2.1', format_data='json',
                  language='en', since='2009M02', region='EE'):
    """
    Read macroeconomic data (house price index, harmonised unemployment
    rates (%), and consumers) from internet and combine in one dataframe.

    Parameters
    ----------
    version : string
        API version.
    format_data : string
        Data format to retrieve.
    language : string
        Language of the data.
    since : string
        Start date to retrieve the data.
    region : string
        Country to retrieve the data.

    Returns
    -------
    df : pandas dataframe
        Pandas dataframe containing macroeconomic data.

    """
    url_base = f'http://ec.europa.eu/eurostat/wdds/rest/data/v{version}/{format_data}/{language}/'
    url_houses = f'ei_hppi_q?precision=1&indic=TOTAL&unit=I15_NSA&sinceTimePeriod={since}&geo={region}'
    url_unemployment = f'ei_lmhr_m?precision=1&indic=LM-UN-T-TOT&unit=PC_ACT&s_adj=SA&sinceTimePeriod={since}&geo={region}'
    url_consumers = f'ei_bsco_m?precision=1&indic=BS-MP-PR&unit=BAL&s_adj=SA&sinceTimePeriod={since}&geo={region}'

    urls = [url_base + url_houses,
            url_base + url_unemployment,
            url_base + url_consumers
            ]
    labels = ['HousePriceIndex', 'HarmonizedUnemploymentRate', 'Consumers']
    labeled_data = {}
    df_joined = None
    for i, url in enumerate(urls):
        df = pd.DataFrame()
        try:
            # read dataset
            r = requests.get(url)
            r.raise_for_status()

            # if no errors, convert response to json
            j = r.json()

            # get data dictionary
            data_dict = j['value']

            # get label dictionary
            label_dict = j['dimension']['time']['category']['index']

            # combine label und data dictionaries in dataframe
            if 'M' in list(label_dict)[0]:
                # monthly data
                if j['size'][-1] == len(label_dict):
                    labeled_data = {datetime.strptime(i, '%YM%m').date(): data_dict.get(str(j)) for i, j in label_dict.items()}
                    df = pd.DataFrame(labeled_data.items(), columns=['Date', labels[i]])
            else:
                # quarterly data
                if j['size'][-1] == len(label_dict):
                    labeled_data = {quarter_to_month(i): data_dict.get(str(j)) for i, j in label_dict.items()}
                    df = pd.DataFrame(labeled_data.items(), columns=['Date', labels[i]])
                    # extrapolate to months
                    df['Date'] = pd.to_datetime(df['Date']).dt.to_period('M')
                    df = df.set_index('Date').resample('M').interpolate()
                    df.index = pd.to_datetime(df.index.to_timestamp())
                    df.reset_index(drop=False, inplace=True)
                    df['Date'] = df['Date'].dt.date

        except requests.exceptions.HTTPError as e:
            print(e)
        except Exception as e:
            print(e)

        # merge dataframes to one
        if df_joined is None:
            df_joined = df.copy()
        else:
            df_joined = pd.merge(df_joined, df, on='Date', how='outer')

    return df_joined


def show_df_info(df, name):
    """
    Print info (size, number of duplicates, number of absent values) about the dataframe.

    Parameters
    ----------
    df : pandas dataframe
        Dataframe to show info.
    name: string
        Name of the dataframe.

    Returns
    -------
    df : pandas dataframe
        Pandas dataframe containing deduplicated data.

    """
    star_length = 60
    try:
        # print shape
        rows = df.shape[0]
        columns = df.shape[1]
        print(star_length * '*')
        print('Dataframe "{}" has {} rows and {} columns.'.format(
            name, rows, columns))
    except Exception as e:
        logger.error(e)

    try:
        # print number of duplicates
        duplicates = df.loc[df.duplicated(keep='first'), :].shape[0]
        print(star_length * '*')
        if duplicates:
            print('{} duplicates are detected and will be deleted.'.format(
                duplicates))
            # delete duplicates
            df = df.drop_duplicates()
        else:
            print('There are no duplicates in the dataframe.')
    except Exception as e:
        logger.error(e)

    try:
        # print number of absent values
        print(star_length * '*')
        for column in df.columns:
            null_number = (df[column].isnull().sum())
            if null_number > 0:
                print('Column "{}" has {} absent values'.format(
                    column, null_number))
        if df.isna().values.sum() == 0:
            print('There are no absent values in the dataframe.')
        print(star_length * '*' + '\n')
    except Exception as e:
        logger.error(e)

    return df


def upload_to_s3(df, file_name=None):
    """
    Upload pandas dataframe or zip-file into S3 bucket as csv-file or gz-file.

    Parameters
    ----------
    df : pandas dataframe or URL of zip-file
        Dataframe or URL of zip-file to upload into S3 bucket.
    file_name : string
        The name of a csv-file in S3 bucket.

    Returns
    -------
    int
        -1 - error, 1 - success.

    """
    # read configuration
    try:
        print('Reading configuration from {}...'.format(path_settings))
        start = time.time()
        config = configparser.ConfigParser()
        config.read_file(open(path_settings))

        DWH_REGION = config.get("DWH", "DWH_REGION")
        KEY = config.get('AWS', 'KEY')
        SECRET = config.get('AWS', 'SECRET')
        S3_BUCKET = config.get("S3", "BUCKET")
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.\n')
        logger.error(e)
        return -1

    # create client for s3
    try:
        print('\nCreating S3 client...')
        start = time.time()
        s3 = boto3.resource('s3',
                            region_name=DWH_REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.\n')
        logger.error(e)
        return -1

    # upload dataframe to S3
    try:
        if file_name:
            print('\nUploading file "{}" to S3 bucket...'.format(file_name))
            start = time.time()
            csv_buffer = StringIO()
            df.to_csv(csv_buffer, index=False, header=False)
            s3.Object(S3_BUCKET, file_name).put(Body=csv_buffer.getvalue())
            dt = time.time() - start
            print('The stage completed successfully in {:0.1f} s.\n'.format(dt))
        else:
            # create temporarily folder
            if not os.path.exists(path_tmp):
                os.makedirs(path_tmp)

            # download zip file from internet
            print('\nDownloading ZIP file to temporary folder...')
            start = time.time()
            with open(os.path.join(path_tmp, 'transactions.zip'), 'wb') as wf:
                wf.write(download_transactions(df).read())
            dt = time.time() - start
            print('The stage completed successfully in {:0.1f} s.'.format(dt))

            # unzip file to the temporarily file
            print('\nUnzipping ZIP file...')
            start = time.time()
            with zipfile.ZipFile(os.path.join(path_tmp, 'transactions.zip'), 'r') as zip_file:
                with zip_file.open(zip_file.infolist()[0].filename) as zf:
                    with open(os.path.join(path_tmp, 'unzipped.tmp'), 'wb') as wf:
                        wf.write(zf.read())
            dt = time.time() - start
            print('The stage completed successfully in {:0.1f} s.'.format(dt))

            # compress the temporarily file as gzip file
            print('\nCompressing file as GZIP...')
            start = time.time()
            with open(os.path.join(path_tmp, 'unzipped.tmp'), 'rb') as rf:
                with gzip.open(os.path.join(path_tmp, 'out.gz'), 'wb') as zf:
                    zf.write(rf.read())
            dt = time.time() - start
            print('The stage completed successfully in {:0.1f} s.'.format(dt))

            # upload gzip file to S3 bucket
            print('\nUploading GZIP file to S3 bucket...')
            start = time.time()
            with open(os.path.join(path_tmp, 'out.gz'), 'rb') as zf:
                s3.Object(S3_BUCKET, 'transactions.gz').put(Body=zf)
            dt = time.time() - start
            print('The stage completed successfully in {:0.1f} s.\n'.format(dt))

            # delete temporarily folder and files
            shutil.rmtree(path_tmp)

        return 1

    except Exception as e:
        print('The last stage failed. See logs for details.\n')
        logger.error(e)
        return -1


def copy_to_staging_table():
    """
    Copy content of all files located in S3 bucket to Redshift staging tables.

    Returns
    -------
    int
        -1 - error, 1 - success.

    """
    # read configuration
    try:
        print('Reading configuration from {}...'.format(path_settings))
        start = time.time()
        config = configparser.ConfigParser()
        config.read_file(open(path_settings))
        KEY = config.get('AWS', 'KEY')
        SECRET = config.get('AWS', 'SECRET')
        DWH_REGION = config.get("DWH", "DWH_REGION")
        DWH_ENDPOINT = config.get("DWH", "DWH_ENDPOINT")
        DWH_DB = config.get("DWH", "DWH_DB")
        DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
        DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
        DWH_PORT = config.get("DWH", "DWH_PORT")
        IAM_ROLE_ARN = config.get("IAM_ROLE", "ARN")
        S3_BUCKET = config.get("S3", "BUCKET")
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.')
        logger.error(e)
        return -1

    # create client for S3
    try:
        print('\nCreating S3 client...')
        start = time.time()
        s3 = boto3.resource('s3',
                            region_name=DWH_REGION,
                            aws_access_key_id=KEY,
                            aws_secret_access_key=SECRET
                            )
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.\n')
        logger.error(e)
        return -1

    # get a list of files in S3-bucket
    try:
        print('\nGetting list of files in S3 bucket...')
        start = time.time()
        bucket = s3.Bucket(S3_BUCKET)
        files_list = [file.key for file in bucket.objects.all()]
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.\n')
        logger.error(e)
        return -1

    # connect to database
    try:
        print('\nConnecting to database...')
        start = time.time()
        conn = psycopg2.connect(('host={} dbname={} user={} password={} '
                                 'port={}').format(DWH_ENDPOINT, DWH_DB,
                                                   DWH_DB_USER,
                                                   DWH_DB_PASSWORD, DWH_PORT))
        cur = conn.cursor()
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))

        print('\nCopying data to staging tables...')
        start = time.time()
        for file_name in files_list:
            location = 's3://{}/{}'.format(S3_BUCKET, file_name)
            header = 0
            file_format = file_name.split('.')[1].upper()
            if file_format == 'GZ':
                file_format = 'GZIP'
                header = 1
            if cur:
                cur.execute(copy_table.format(
                    table_name='staging_'+file_name.split('.')[0].lower(),
                    location=location,
                    iam_role=IAM_ROLE_ARN,
                    region=DWH_REGION,
                    file_format=file_format,
                    header=header
                    ))
                conn.commit()
            print(('Copy to "staging_{}" completed successfully in {:0.1f} '
                   's.\n').format(
                       file_name.split('.')[0].lower(), time.time() - start))
        cur.close()
        conn.close()
        return 1

    except Exception as e:
        print('\nFailed to copy data.\n')
        logger.error(e)
        return -1

    except Exception as e:
        print('\nFailed to copy data.\n')
        logger.error(e)
        return -1


def insert_into_table(queries):
    """
    Read configuration from the file, connect to the database,
    and insert content of staging tables to tables.

    Parameters
    ----------
    queries  : list
        List of queries to execute for table creation.

    Returns
    -------
    int
        -1 - error, 1 - success.

    """
    # read configuration
    try:
        print('Reading configuration from {}...'.format(path_settings))
        start = time.time()
        config = configparser.ConfigParser()
        config.read_file(open(path_settings))
        DWH_ENDPOINT = config.get("DWH", "DWH_ENDPOINT")
        DWH_DB = config.get("DWH", "DWH_DB")
        DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
        DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
        DWH_PORT = config.get("DWH", "DWH_PORT")
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.')
        logger.error(e)
        return -1

    # connect to database
    try:
        print('\nConnecting to database...')
        start = time.time()
        conn = psycopg2.connect(('host={} dbname={} user={} password={} '
                                 'port={}').format(DWH_ENDPOINT, DWH_DB,
                                                   DWH_DB_USER,
                                                   DWH_DB_PASSWORD, DWH_PORT))
        cur = conn.cursor()
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))

        print('\nInserting data from staging tables...')
        start = time.time()
        if cur:
            for query in queries:
                cur.execute(query[0])
                conn.commit()
                cur.execute(query[1])
                conn.commit()
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.\n'.format(dt))
        cur.close()
        conn.close()
        return 1

    except Exception as e:
        print('\nFailed to insert data.\n')
        logger.error(e)
        return -1


def main():
    """
    Run ETL.

    Returns
    -------
    None.

    """
    # get loans data from Bondora
    df_loans = get_loans(path_loans)

    # split loans data into loans and borrowers data
    df_loans, df_borrowers = split_loans(df_loans)

    # get historical market data
    df_market = read_market(start_date=from_date)

    # get macroeconomic indicators
    df_mstat = read_eurostat(since='{}M{}'.format(from_date[0:4],
                                                  from_date[5:7]))

    # take care about missing values and duplicates
    df_loans = show_df_info(df_loans, 'Loans')
    df_borrowers = show_df_info(df_borrowers, 'Borrowers')
    df_market = show_df_info(df_market, 'Market')
    df_mstat = show_df_info(df_mstat, 'Statistics')

    # download zip file, convert it in gzip format, and upload it to S3 bucket
    upload_to_s3(path_resales)

    # upload dataframes to S3 bucket
    upload_to_s3(df_loans, 'loans.csv')
    upload_to_s3(df_borrowers, 'borrowers.csv')
    upload_to_s3(df_market, 'market.csv')
    upload_to_s3(df_mstat, 'statistics.csv')

    # create staging tables
    create_tables(create_staging_table_queries)

    # copy into staging tables
    copy_to_staging_table()

    # insert from staging tables to tables
    insert_into_table(upsert_table_queries)

    # drop staging tables
    drop_tables(staging=True)


if __name__ == "__main__":
    main()
