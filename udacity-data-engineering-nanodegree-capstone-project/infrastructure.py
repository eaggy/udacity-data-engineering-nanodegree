# -*- coding: utf-8 -*-
"""The file contains functions to create AWS infrastructure."""

# imports
import sys
import logging
import time
import boto3
import psycopg2
import json
import configparser
from redshift_queries import get_tables, drop_table, create_table_queries

path_settings = 'aws.cfg'


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


def create_bucket():
    """
    Read configuration from the file, connect to AWS, and create a bucket.

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

    # create bucket
    try:
        print('\nCreating S3 bucket...')
        start = time.time()
        location = {'LocationConstraint': DWH_REGION}
        s3.create_bucket(Bucket=S3_BUCKET,
                         CreateBucketConfiguration=location)
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.\n'.format(dt))
        return 1
    except Exception as e:
        print('The last stage failed. See logs for details.\n')
        logger.error(e)
        return -1


def delete_bucket():
    """
    Read configuration from the file, connect to AWS, and delete a bucket.

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

    # delete bucket
    try:
        print('\nDeleting S3 bucket...')
        start = time.time()
        bucket = s3.Bucket(S3_BUCKET)
        bucket.objects.all().delete()
        bucket.delete()
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.\n'.format(dt))
        return 1
    except Exception as e:
        print('The last stage failed. See logs for details.\n')
        logger.error(e)
        return -1


def check_cluster():
    """
    Read configuration, connect to AWS, and check Redshift cluster status.

    Returns
    -------
    int
        -1 - error, 0 - cluster not found, 1 - success.
    str
        The status of the cluster.

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
        DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.')
        logger.error(e)
        return -1, ''

    # create client for redshift
    try:
        print('\nCreating Redshift client...')
        start = time.time()
        redshift = boto3.client('redshift',
                                region_name=DWH_REGION,
                                aws_access_key_id=KEY,
                                aws_secret_access_key=SECRET
                                )
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.')
        logger.error(e)
        return -1, ''

    # check cluster status
    try:
        print('\nChecking cluster status...')
        start = time.time()
        clusterProps = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        print(('The cluster status is "{}". '
               'The stage completed successfully in {:0.1f} s.\n').format(
                   clusterProps['ClusterStatus'], time.time() - start))
        return 1, clusterProps['ClusterStatus']

    except Exception as e:
        if 'Cluster {} not found'.format(DWH_CLUSTER_IDENTIFIER.lower()) in str(e):
            print(('The cluster {} does not exist. '
                   'The stage completed successfully in {:0.1f} s.\n').format(
                       DWH_CLUSTER_IDENTIFIER.lower(), time.time() - start))
            return 0, ''
        else:
            print('The last stage failed. See logs for details.\n')
            return -1, ''


def create_cluster():
    """
    Read configuration from the file, connect to AWS,
    create a new Redshift cluster, and check its status.

    Returns
    -------
    int
        -1 - error, 0 - timeout, 1 - success.
    str
        The status of the cluster.

    """
    # read configuration
    try:
        print('Reading configuration from {}...'.format(path_settings))
        start = time.time()
        config = configparser.ConfigParser()
        config.read_file(open(path_settings))

        KEY = config.get('AWS', 'KEY')
        SECRET = config.get('AWS', 'SECRET')

        DWH_CLUSTER_TYPE = config.get("DWH", "DWH_CLUSTER_TYPE")
        DWH_NUM_NODES = config.get("DWH", "DWH_NUM_NODES")
        DWH_NODE_TYPE = config.get("DWH", "DWH_NODE_TYPE")
        DWH_REGION = config.get("DWH", "DWH_REGION")

        DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
        DWH_DB = config.get("DWH", "DWH_DB")
        DWH_DB_USER = config.get("DWH", "DWH_DB_USER")
        DWH_DB_PASSWORD = config.get("DWH", "DWH_DB_PASSWORD")
        DWH_PORT = config.get("DWH", "DWH_PORT")

        DWH_IAM_ROLE_NAME = config.get("DWH", "DWH_IAM_ROLE_NAME")
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.')
        logger.error(e)
        return -1, ''

    # create iam client
    try:
        print('\nCreating IAM client...')
        start = time.time()
        iam = boto3.client('iam',
                           region_name=DWH_REGION,
                           aws_access_key_id=KEY,
                           aws_secret_access_key=SECRET
                           )
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.')
        logger.error(e)
        return -1, ''

    # create iam role
    try:
        print('\nCreating IAM role...')
        start = time.time()
        iam.create_role(Path='/',
                        RoleName=DWH_IAM_ROLE_NAME,
                        Description=('Allows Redshift clusters to call '
                                     'AWS services on your behalf.'),
                        AssumeRolePolicyDocument=json.dumps(
                            {'Statement':
                             [{'Action': 'sts:AssumeRole',
                               'Effect': 'Allow',
                               'Principal': {'Service':
                                             'redshift.amazonaws.com'}}],
                             'Version': '2012-10-17'})
                        )
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        if 'Role with name {} already exists'.format(DWH_IAM_ROLE_NAME) in str(e):
            print(('The role {} already exists. The stage completed '
                   'successfully in {:0.1f} s.').format(
                      DWH_IAM_ROLE_NAME, time.time() - start))
        else:
            print('The last stage failed. See logs for details.')
            logger.error(e)
            return -1, ''

    # attach policy
    try:
        print('\nAttaching policy...')
        start = time.time()
        iam.attach_role_policy(
            RoleName=DWH_IAM_ROLE_NAME,
            PolicyArn="arn:aws:iam::aws:policy/AmazonS3ReadOnlyAccess"
            )['ResponseMetadata']['HTTPStatusCode']

        # get iam role arn
        roleArn = iam.get_role(RoleName=DWH_IAM_ROLE_NAME)['Role']['Arn']
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.')
        logger.error(e)
        return -1, ''

    # create client for redshift
    try:
        print('\nCreating Redshift client...')
        start = time.time()
        redshift = boto3.client('redshift',
                                region_name=DWH_REGION,
                                aws_access_key_id=KEY,
                                aws_secret_access_key=SECRET
                                )
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.')
        logger.error(e)
        return -1, ''

    # create redshift cluster
    try:
        print('\nCreating Redshift cluster...')
        start = time.time()
        redshift.create_cluster(ClusterType=DWH_CLUSTER_TYPE,
                                NodeType=DWH_NODE_TYPE,
                                NumberOfNodes=int(DWH_NUM_NODES),
                                DBName=DWH_DB,
                                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
                                MasterUsername=DWH_DB_USER,
                                MasterUserPassword=DWH_DB_PASSWORD,
                                IamRoles=[roleArn]
                                )

        # check cluster status
        status = ''
        counter = 0
        while counter <= 600:
            time.sleep(15)
            status = redshift.describe_clusters(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']
            if status == 'available':
                break
            counter += 15
            print(('\rRedshift cluster '
                  'creation in-progress... ({} seconds left)').format(counter),
                  end='', flush=True)

        if counter <= 600:
            print(('\rThe cluster status is "{}". '
                   'The stage completed successfully in {:0.1f} '
                   's.').format(status, time.time() - start), flush=True)

            # check connection to database
            try:
                print('\nConnecting to database...')
                start = time.time()
                DWH_ENDPOINT = redshift.describe_clusters(
                    ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['Endpoint']['Address']
                conn = psycopg2.connect(('host={} dbname={} user={} '
                                        'password={} port={}').format(
                                            DWH_ENDPOINT, DWH_DB, DWH_DB_USER,
                                            DWH_DB_PASSWORD, DWH_PORT))
                cur = conn.cursor()
                cur.close()
                conn.close()
                print(('The stage completed successfully in '
                       '{:0.1f} s.\n').format(time.time() - start))

                # write DWH endpoint and roleArn in configuration file
                config.set("DWH", "DWH_ENDPOINT", DWH_ENDPOINT)
                config.set("IAM_ROLE", "ARN", roleArn)
                with open(path_settings, 'w') as f:
                    config.write(f)

                return 1, status

            except Exception as e:
                print('\nFailed to connect to the database.\n')
                logger.error(e)
                return -1, status

        else:
            print(('\rThe cluster status is "{}". The stage completed with '
                   'timeout in {:0.1f} s.\n').format(status,
                                                     time.time() - start),
                  flush=True)
            return 0, status
    except Exception as e:
        print('The last stage failed. See logs for details.\n')
        logger.error(e)
        return -1, ''


def delete_cluster():
    """
    Read configuration, connect to AWS, and delete Redshift cluster.

    Returns
    -------
    int
        -1 - error, 0 - cluster not found / timeout, 1 - success.
    str
        The status of the cluster.

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
        DWH_CLUSTER_IDENTIFIER = config.get("DWH", "DWH_CLUSTER_IDENTIFIER")
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.')
        logger.error(e)
        return -1, ''

    # create client for redshift
    try:
        print('\nCreating Redshift client...')
        start = time.time()
        redshift = boto3.client('redshift',
                                region_name=DWH_REGION,
                                aws_access_key_id=KEY,
                                aws_secret_access_key=SECRET
                                )
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))
    except Exception as e:
        print('The last stage failed. See logs for details.')
        logger.error(e)
        return -1, ''

    # check cluster status
    try:
        print('\nChecking cluster status...')
        start = time.time()
        clusterProps = redshift.describe_clusters(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]
        print(('The cluster status is "{}". The stage completed successfully '
              'in {:0.1f} s.').format(
                  clusterProps['ClusterStatus'], time.time() - start))

    except Exception as e:
        if 'Cluster {} not found'.format(DWH_CLUSTER_IDENTIFIER.lower()) in str(e):
            print(('The cluster {} does not exist. The stage completed '
                  'successfully in {:0.1f} s.\n').format(
                      DWH_CLUSTER_IDENTIFIER.lower(), time.time() - start))
            return 0, 'not found'
        else:
            print('The last stage failed. See logs for details.\n')
            return -1, ''

    # delete cluster
    try:
        print('\nDeleting cluster ...')
        start = time.time()
        redshift.delete_cluster(
            ClusterIdentifier=DWH_CLUSTER_IDENTIFIER,
            SkipFinalClusterSnapshot=True)

        # check cluster status
        status = ''
        counter = 0
        while counter <= 600:
            time.sleep(15)
            status = redshift.describe_clusters(
                ClusterIdentifier=DWH_CLUSTER_IDENTIFIER)['Clusters'][0]['ClusterStatus']
            counter += 15
            print(('\rRedshift cluster deletion in-progress... '
                  '({} seconds left)').format(counter), end='', flush=True)

        if counter > 600:
            print(('\rThe cluster status is "{}". The stage completed with '
                   'timeout in {:0.1f} s.\n').format(
                       status, time.time() - start), flush=True)
            return 0, status
    except Exception as e:
        if 'Cluster {} not found'.format(DWH_CLUSTER_IDENTIFIER.lower()) in str(e):
            print(('\nThe cluster "{}" was deleted. The stage completed '
                   'successfully in {:0.1f} s.\n').format(
                       DWH_CLUSTER_IDENTIFIER, time.time() - start))

            # remove DWH endpoint from configuration file
            config.set("DWH", "DWH_ENDPOINT", '')
            with open(path_settings, 'w') as f:
                config.write(f)
            return 1, 'deleted'
        else:
            print('\nThe last stage failed. See logs for details.\n')
            logger.error(e)
            return -1, ''


def drop_tables(staging=False):
    """
    Read configuration from the file, connect to the database, and drop tables.

    Parameters
    ----------
    staging : bool
        Drop only staging tables, if true.

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
        print('The last stage failed. See logs for details.\n')
        logger.error(e)
        return -1

    # connect to database
    try:
        print('\nConnecting to database...')
        start = time.time()
        conn = psycopg2.connect(('host={} dbname={} user={} password={} '
                                'port={}').format(
                                    DWH_ENDPOINT, DWH_DB, DWH_DB_USER,
                                    DWH_DB_PASSWORD, DWH_PORT))
        cur = conn.cursor()
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))

        print('\nGet tables list...')
        start = time.time()
        if cur:
            cur.execute(get_tables)
            tables = cur.fetchall()
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))

        # drop tables
        if staging:
            print('\nDrop staging tables...')
        else:
            print('\nDrop tables...')
        start = time.time()
        if cur:
            for table in tables:
                if staging:
                    if table[0].startswith('staging_'):
                        cur.execute(drop_table.format(table_name=table[0]))
                        conn.commit()
                else:
                    cur.execute(drop_table.format(table_name=table[0]))
                    conn.commit()
        dt = time.time() - start
        print('The stage completed successfully in '
              '{:0.1f} s.\n'.format(time.time() - start))

        cur.close()
        conn.close()
        return 1

    except Exception as e:
        print('\nFailed to drop tables.\n')
        logger.error(e)
        return -1


def create_tables(queries):
    """
    Read configuration, connect to the database, and create required tables.

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
                                                  DWH_DB_USER, DWH_DB_PASSWORD,
                                                  DWH_PORT))
        cur = conn.cursor()
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.'.format(dt))

        print('\nCreating tables...')
        start = time.time()
        if cur:
            for query in queries:
                cur.execute(query)
                conn.commit()
        dt = time.time() - start
        print('The stage completed successfully in {:0.1f} s.\n'.format(dt))
        cur.close()
        conn.close()
        return 1

    except Exception as e:
        print('\nFailed to create tables.\n')
        logger.error(e)
        return -1


def main():
    """
    Create infrastructure on AWS.

    Returns
    -------
    None.

    """
    # check cluster status and creat it, if not existing
    status = check_cluster()
    if status[0] == 0:
        status = create_cluster()

    # drop existing tables and create new tables
    if status[1] == 'available':
        drop_tables()
        create_tables(create_table_queries)

    # delete cluster; uncomment the next line to delete Redshift cluster
    # delete_cluster()

    # create bucket
    create_bucket()

    # delete bucket; uncomment the next line to delete S3 bucket
    # delete_bucket()


if __name__ == "__main__":
    main()
