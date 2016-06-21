from __future__ import print_function
import pytest
import sys
from operator import add
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf, SQLContext, Row
import docker, os, subprocess, json, riak, time
import pyspark_riak
import timeout_decorator
import datetime
import tzlocal
import pytz

#### Instructions to run #####
'''
These assume OSX:

1:Install docker-toolbox, docker-machine, and docker
2: Add a route to the docker machine with sudo route add 172.17.0.0/16 192.168.99.100
3: Install required python libraries sudo pip install pytest, findspark, docker-py, timeout_decorator, datetime, riak
4: Will need to manually install pyspark_riak library
5: Create Spark-Riak Connector uber jar 1.5.1 and paste the path below in os.environ['SPARK_CLASSPATH]
6: Pull down riak-ts docker image and install
7: To run the test, cd to the test file (this file) and run py.test pyspark_tests.py -s

'''

#### Notes ####
'''
Saving ints to riak ts preserves the value of the timestamp.
Querying ints using riak client is, in this case, simple, just query the int range

Saving datetimes to riak ts, the datetimes will be treated as local time, converted then to gmt time.
You can query with riak client by int only, so in this case you must convert your local datetime to utc int.
If you do ts_get, you can use local datetime to query. The query will be converted automatically to utc before query.

Reading datetime from ts using spark timestamp option will convert datetime back to local datetime.
'''



#### TODO ####

#sudo route add 172.17.0.0/16 192.168.99.100, osx only
#docker, pull riak-ts image
#pytest
#cd to the test file and run, py.test pyspark_tests.py -s
#pass in uber jar as os.environ['SPARK_CLASSPATH']

os.environ['SPARK_CLASSPATH'] = '/Users/basho/.m2/repository/com/basho/riak/spark-riak-connector/1.5.1-SNAPSHOT/spark-riak-connector-1.5.1-SNAPSHOT-uber.jar'

###### FIXTURES #######

@pytest.fixture(scope="session")
def docker_cli(request):
    home = os.environ['HOME']
    cert_path = home+'/.docker/machine/certs/cert.pem'
    key_path = home+'/.docker/machine/certs/key.pem'
    ca_path = home+'/.docker/machine/certs/ca.pem'
    tls_config = docker.tls.TLSConfig(client_cert=(cert_path, key_path), verify=ca_path)
    cli = docker.Client(base_url=u'tcp://192.168.99.100:2376', tls=tls_config)

    #for line in cli.pull('basho/riak-ts', stream=True):
    #    print(json.dumps(json.loads(line), indent=4))

    try:
        cli.stop('dev1')
        cli.remove_container('dev1')
    except Exception as e:
        pass

    riak_container = cli.create_container(image='basho/riak-ts', name='dev1', ports=[8087, 8098], \
                                          host_config=cli.create_host_config(port_bindings={8087: 8087, 8098: 8098}))
    cli.start('dev1')
    task = cli.exec_create('dev1', 'riak-admin wait-for-service riak_kv riak@172.17.0.2')
    result = cli.exec_start(task['Id'])
    print(result)

    task = cli.exec_create('dev1', 'riak-admin test')
    result = cli.exec_start(task['Id'])
    print(result)

    task = cli.exec_create('dev1', 'riak ping')
    result = cli.exec_start(task['Id'])
    print(result)
    #request.addfinalizer(lambda: cli.remove_container('dev1'))
    #request.addfinalizer(lambda: cli.stop('dev1'))
    return cli
@pytest.mark.usefixtures("docker_cli")


@pytest.fixture(scope="session")
def spark_context(request, docker_cli):
    host, pb_port, hostAndPort = get_host_and_port(docker_cli)
    conf = (SparkConf().setMaster("local[*]").setAppName("pytest-pyspark-local-testing"))
    conf.set('spark.riak.connection.host', hostAndPort)
    sc = SparkContext(conf=conf)
    sc.setLogLevel("ERROR")
    pyspark_riak.riak_context(sc)
    request.addfinalizer(lambda: sc.stop())
    return sc
@pytest.mark.usefixtures("spark_context")


@pytest.fixture(scope="session")
def sql_context(request, spark_context):
    sqlContext = SQLContext(spark_context)
    return sqlContext
@pytest.mark.usefixtures("sql_context")


@pytest.fixture(scope="session")
def riak_client(request, docker_cli):
    host, pb_port, hostAndPort = get_host_and_port(docker_cli)
    client = riak.RiakClient(host=host, pb_port=pb_port)
    request.addfinalizer(lambda: client.close())
    return client
@pytest.mark.usefixtures("riak_client")

###### FUNCTIONS #######

def get_host_and_port(docker_cli):
    container = [x for x in docker_cli.containers() if x['Names'][0] == u'/dev1']
    host= container[0]['NetworkSettings']['Networks']['bridge']['IPAddress']
    pb_port = container[0]['Ports'][1]['PublicPort']
    hostAndPort = ":".join([str(host), str(pb_port)])
    return host, pb_port, hostAndPort

def retry_func_with_timeout(func, times, timeout, signal, args, use_condition, condition_func, condition_val, test_func, test_args):

    @timeout_decorator.timeout(timeout, use_signals=signal)
    def temp_func(run_func, run_args):
        return run_func(*run_args)

    success = False
    i = 0
    while i < times:

        try:
            result = temp_func(func,args)

            if use_condition:
                if condition_func(result, condition_val, test_func, test_args) == True:
                    return True, result
            else:
                return True, result

        except Exception as e:
            print(func)
            #print(e)

        i = i + 1
        time.sleep(1)
            
    return False, None

def setup_table(client):

    riak_ts_table_name = 'spark-riak-%d' % int(time.time())
    riak_ts_table = client.table(riak_ts_table_name)

    create_sql = """CREATE TABLE %(table_name)s (
    field1 varchar not null,
    field2 varchar not null,
    datetime timestamp not null,
    data sint64,
    PRIMARY KEY ((field1, field2, quantum(datetime, 24, h)), field1, field2, datetime))
    """ % ({'table_name': riak_ts_table_name})

    return riak_ts_table_name, create_sql, riak_ts_table

def setup_kv_obj(client, bucket_name, key, content_type, data):

    bucket = client.bucket(bucket_name)
    obj = riak.RiakObject(client, bucket, key)
    obj.content_type = content_type
    obj.data = data
    return obj

def setup_ts_obj(ts_table, data):
    return ts_table.new(data)

def general_condition(result, val, func, args):
    if result == val:
        return True
    else:
        return False

def key_get_condition(result, val, func, args):

    if result.data == val:
        return True
    else:
        return False

def ts_get_condition(result, val, func, args):

    if result.rows == val:
        return True
    else:
        return False

def ts_query_condition(result, val, func, args):

    if sorted(result.rows, key=lambda x: x[2]) == sorted(val, key=lambda x: x[2]):
        return True
    else:
        return False

def riak_start_condition(result, val, func, args):
    if val in result:
        return True
    else:
        return False

def df_read_verify_condition(result, val, func, args):

    #print(sorted(result.collect(), key=lambda x: x[2]))
    #print(sorted(val.collect(), key=lambda x: x[2]))

    if sorted(result.collect(), key=lambda x: x[2]) == sorted(val.collect(), key=lambda x: x[2]):
        return True
    else:
        return False

def unix_time_millis(dt):
    td = dt - datetime.datetime.utcfromtimestamp(0)
    return int(td.total_seconds() * 1000.0)

def make_data_long(start_date, N, M):

    data = []
    one_second = datetime.timedelta(seconds=1)
    one_day = datetime.timedelta(days=1)

    for i in range(M):
        for j in range(N):

            data.append(['field1_val', 'field2_val', unix_time_millis(start_date + i*one_day + j*one_second), i+j])


    end_date = start_date + (M-1)*one_day + (N-1)*one_second
    return data, start_date, end_date


def make_data_timestamp(start_date, N, M):

    timestamp_data = []
    long_data = []

    one_second = datetime.timedelta(seconds=1)
    one_day = datetime.timedelta(days=1)

    for i in range(M):
        for j in range(N):

            cur_local_timestamp = start_date + i*one_day + j*one_second
            timestamp_data.append(['field1_val', 'field2_val', cur_local_timestamp, i+j])
            long_data.append(['field1_val', 'field2_val', unix_time_millis(convert_local_dt_to_gmt_dt(cur_local_timestamp)), i+j])
    
    start_timestamp = timestamp_data[0][2]
    end_timestamp = timestamp_data[-1][2]
    start_long = long_data[0][2]
    end_long = long_data[-1][2]

    return timestamp_data, start_timestamp, end_timestamp, long_data, start_long, end_long 

def convert_local_dt_to_gmt_dt(dt):

    local_tz = tzlocal.get_localzone()
    local_dt = local_tz.localize(dt)
    gmt_dt_with_tzinfo = pytz.utc.normalize(local_dt)

    year = gmt_dt_with_tzinfo.year
    month = gmt_dt_with_tzinfo.month
    day = gmt_dt_with_tzinfo.day
    hour = gmt_dt_with_tzinfo.hour
    minute = gmt_dt_with_tzinfo.minute
    second = gmt_dt_with_tzinfo.second

    gmt_dt = datetime.datetime(year, month, day, hour, minute, second)

    return gmt_dt


###### TESTS #######

def _test_connection(spark_context, docker_cli, riak_client, sql_context):

    assert retry_func_with_timeout(func=riak_client.ping, 
                                   times=10, 
                                   timeout=1, 
                                   signal=True, 
                                   args=[], 
                                   use_condition=True, 
                                   condition_func=general_condition, 
                                   condition_val=True,
                                   test_func=None,
                                   test_args=None 
                                   )[0] == True

    obj = setup_kv_obj(riak_client, 'temp_bucket', 'temp_key', 'text/plain', 'temp_data')

    assert retry_func_with_timeout(func=obj.store, 
                                   times=20, 
                                   timeout=2, 
                                   signal=True, 
                                   args=[], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    assert retry_func_with_timeout(func=riak_client.bucket('temp_bucket').get, 
                                   times=10, 
                                   timeout=2, 
                                   signal=True, 
                                   args=['temp_key'], 
                                   use_condition=True, 
                                   condition_func=key_get_condition, 
                                   condition_val='temp_data',
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

    assert retry_func_with_timeout(func=riak_ts_table.query,
                                   times=10, 
                                   timeout=2, 
                                   signal=True, 
                                   args=[create_sql], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None 
                                   )[0] == True

    ts_obj = setup_ts_obj(riak_ts_table, [['field1_val', 'field2_val', unix_time_millis(datetime.datetime(2015, 1, 1, 12, 0, 0)), 0]])

    assert retry_func_with_timeout(func=ts_obj.store, 
                                   times=20, 
                                   timeout=2, 
                                   signal=True, 
                                   args=[], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    assert retry_func_with_timeout(func=riak_client.ts_get, 
                                   times=10, 
                                   timeout=2, 
                                   signal=True, 
                                   args=[riak_ts_table_name, ['field1_val', 'field2_val', unix_time_millis(datetime.datetime(2015, 1, 1, 12, 0, 0))]], 
                                   use_condition=True, 
                                   condition_func=ts_get_condition, 
                                   condition_val=[['field1_val', 'field2_val', unix_time_millis(datetime.datetime(2015, 1, 1, 12, 0, 0)), 0]],
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

###### Riak TS Test #######

def _test_spark_df_ts_write_use_long(N, M, spark_context, docker_cli, riak_client, sql_context):

    riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

    assert retry_func_with_timeout(func=riak_ts_table.query,
                                   times=10, 
                                   timeout=2, 
                                   signal=True, 
                                   args=[create_sql], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True


    seed_date = datetime.datetime(2015, 1, 1, 12, 0, 0)
    test_data, start, end = make_data_long(seed_date, N, M)
    test_rdd = spark_context.parallelize(test_data)
    test_df = test_rdd.toDF(['field1', 'field2', 'datetime', 'data'])

    assert retry_func_with_timeout(func=test_df.write.format('org.apache.spark.sql.riak').mode('Append').save,
                                   times=10, 
                                   timeout=30, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    fmt =   """
                select * from {table_name}
                where datetime >= {start_date}
                AND datetime <=  {end_date}
                AND field1 = '{field1}'
                AND field2 = '{field2}'          
            """
    query = fmt.format(table_name=riak_ts_table_name, start_date=unix_time_millis(start), end_date=unix_time_millis(end), field1='field1_val', field2='field2_val')

    assert retry_func_with_timeout(func=riak_ts_table.query, 
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=[query], 
                                   use_condition=True, 
                                   condition_func=ts_query_condition, 
                                   condition_val=test_rdd.collect(),
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def _test_spark_df_ts_write_use_timestamp(N, M, spark_context, docker_cli, riak_client, sql_context):

    riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

    assert retry_func_with_timeout(func=riak_ts_table.query,
                                   times=10, 
                                   timeout=2, 
                                   signal=True, 
                                   args=[create_sql], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    seed_date = datetime.datetime(2015, 1, 1, 12, 0, 0)
    timestamp_data, start_timestamp, end_timestamp, long_data, start_long, end_long, = make_data_timestamp(seed_date, N, M)
    test_rdd = spark_context.parallelize(timestamp_data)
    test_df = test_rdd.toDF(['field1', 'field2', 'datetime', 'data'])
    verify_rdd = spark_context.parallelize(long_data)

    assert retry_func_with_timeout(func=test_df.write.format('org.apache.spark.sql.riak').mode('Append').save,
                                   times=10, 
                                   timeout=30, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    fmt =   """
                select * from {table_name}
                where datetime >= {start_date}
                AND datetime <=  {end_date}
                AND field1 = '{field1}'
                AND field2 = '{field2}'          
            """
    query = fmt.format(table_name=riak_ts_table_name, start_date=start_long, end_date=end_long, field1='field1_val', field2='field2_val')

    assert retry_func_with_timeout(func=riak_ts_table.query, 
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=[query], 
                                   use_condition=True, 
                                   condition_func=ts_query_condition, 
                                   condition_val=verify_rdd.collect(),
                                   test_func=None,
                                   test_args=None
                                   )[0] == True



def _test_spark_df_ts_read_use_long(N, M, spark_context, docker_cli, riak_client, sql_context):

    riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

    assert retry_func_with_timeout(func=riak_ts_table.query,
                                   times=10, 
                                   timeout=2, 
                                   signal=True, 
                                   args=[create_sql], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    seed_date = datetime.datetime(2015, 1, 1, 12, 0, 0)
    test_data, start, end = make_data_long(seed_date, N, M)
    test_rdd = spark_context.parallelize(test_data)
    test_df = test_rdd.toDF(['field1', 'field2', 'datetime', 'data'])

    assert retry_func_with_timeout(func=test_df.write.format('org.apache.spark.sql.riak').mode('Append').save,
                                   times=10, 
                                   timeout=30, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    temp_filter = """datetime >= %(start_date)s
                    AND datetime <=  %(end_date)s
                    AND field1 = '%(field1)s'
                    AND field2 = '%(field2)s'
                """ % ({'start_date': unix_time_millis(start), 'end_date': unix_time_millis(end), 'field1': 'field1_val', 'field2': 'field2_val'})

    assert retry_func_with_timeout(func=sql_context.read.format("org.apache.spark.sql.riak").option("spark.riakts.bindings.timestamp", "useLong").load(riak_ts_table_name).filter, 
                                   times=20, 
                                   timeout=2, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_verify_condition, 
                                   condition_val=test_df,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True


def _test_spark_df_ts_read_use_timestamp(N, M, spark_context, docker_cli, riak_client, sql_context):

    riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

    assert retry_func_with_timeout(func=riak_ts_table.query,
                                   times=10, 
                                   timeout=2, 
                                   signal=True, 
                                   args=[create_sql], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    seed_date = datetime.datetime(2015, 1, 1, 12, 0, 0)
    timestamp_data, start_timestamp, end_timestamp, long_data, start_long, end_long, = make_data_timestamp(seed_date, N, M)
    test_rdd = spark_context.parallelize(timestamp_data)
    test_df = test_rdd.toDF(['field1', 'field2', 'datetime', 'data'])
    verify_rdd = spark_context.parallelize(long_data)

    assert retry_func_with_timeout(func=test_df.write.format('org.apache.spark.sql.riak').mode('Append').save,
                                   times=10, 
                                   timeout=30, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    temp_filter = """datetime >= CAST('%(start_date)s' AS TIMESTAMP)
                    AND datetime <=  CAST('%(end_date)s' AS TIMESTAMP)
                    AND field1 = '%(field1)s'
                    AND field2 = '%(field2)s'
                """ % ({'start_date': start_timestamp, 'end_date': end_timestamp, 'field1': 'field1_val', 'field2': 'field2_val'})

    assert retry_func_with_timeout(func=sql_context.read.format("org.apache.spark.sql.riak").option("spark.riakts.bindings.timestamp", "useTimestamp").load(riak_ts_table_name).filter, 
                                   times=20, 
                                   timeout=2, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_verify_condition, 
                                   condition_val=test_df,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def _test_spark_df_ts_range_query_use_long(N, M, S,spark_context, docker_cli, riak_client, sql_context):

    riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

    assert retry_func_with_timeout(func=riak_ts_table.query,
                                   times=10, 
                                   timeout=2, 
                                   signal=True, 
                                   args=[create_sql], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    seed_date = datetime.datetime(2015, 1, 1, 12, 0, 0)
    test_data, start, end = make_data_long(seed_date, N, M)
    test_rdd = spark_context.parallelize(test_data)
    test_df = test_rdd.toDF(['field1', 'field2', 'datetime', 'data'])


    test_func = test_df.write.format('org.apache.spark.sql.riak').mode('Append').save

    assert retry_func_with_timeout(func=test_func,
                                   times=3, 
                                   timeout=3, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    temp_filter = """datetime >= %(start_date)s
                    AND datetime <=  %(end_date)s
                    AND field1 = '%(field1)s'
                    AND field2 = '%(field2)s'
                """ % ({'start_date': unix_time_millis(start), 'end_date': unix_time_millis(end), 'field1': 'field1_val', 'field2': 'field2_val'})


    print(temp_filter)
    print(start)
    print(end)
    test_func = sql_context.read.format("org.apache.spark.sql.riak") \
                .option("spark.riakts.bindings.timestamp", "useLong") \
                .load(riak_ts_table_name).filter


    assert retry_func_with_timeout(func=test_func, 
                                   times=1, 
                                   timeout=3, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_verify_condition, 
                                   condition_val=test_df,
                                   test_func=None,
                                   test_args=None
                                   )[0] == False


    test_func = sql_context.read.format("org.apache.spark.sql.riak") \
                .option("spark.riakts.bindings.timestamp", "useLong") \
                .option("spark.riak.input.split.count", str(S)) \
                .option("spark.riak.partitioning.ts-range-field-name", "datetime") \
                .load(riak_ts_table_name).filter

    assert retry_func_with_timeout(func=test_func, 
                                   times=5, 
                                   timeout=5, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_verify_condition, 
                                   condition_val=test_df,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def test_spark_riak_connector(spark_context, docker_cli, riak_client, sql_context):

    _test_connection(spark_context, docker_cli, riak_client, sql_context)
    #_test_spark_df_ts_write_use_timestamp(1, 100, spark_context, docker_cli, riak_client, sql_context)
    #_test_spark_df_ts_write_use_long(1, 100, spark_context, docker_cli, riak_client, sql_context)
    #_test_spark_df_ts_read_use_timestamp(1, 100, spark_context, docker_cli, riak_client, sql_context)
    #_test_spark_df_ts_read_use_long(1, 100, spark_context, docker_cli, riak_client, sql_context)
    _test_spark_df_ts_range_query_use_long(1, 500, 1, spark_context, docker_cli, riak_client, sql_context)


'''
###### Riak KV Tests ######

def test_spark_df_kv_write_read_query_all_one_entry(spark_context, docker_cli, riak_client, sql_context):

    key1 = 'key1'
    field1_key = 'field1_key'
    field1_val = 'field1_val'
    entry1 = {key1 : {field1_key : field1_val}}

    source_data = [entry1]
    source_rdd = spark_context.parallelize(source_data)

    source_rdd.saveToRiak("test-python-bucket-1", "default")

    rdd = spark_context.riakBucket("test-python-bucket-1", "default").queryAll()
    data = rdd.collect()[0]

    assert data == (key1, {field1_key : field1_val})


def test_spark_df_kv_write_read_query_bucket_keys_one_entry(spark_context, docker_cli, riak_client, sql_context):

    key1 = 'key1'
    field1_key = 'field1_key'
    field1_val = 'field1_val'
    entry1 = {key1 : {field1_key : field1_val}}

    source_data = [entry1]
    source_rdd = spark_context.parallelize(source_data)

    source_rdd.saveToRiak("test-python-bucket-2", "default")

    rdd = spark_context.riakBucket("test-python-bucket-2", "default").queryBucketKeys('key1')
    data = rdd.collect()

    assert data == [(key1, {field1_key : field1_val})]

    rdd = spark_context.riakBucket("test-python-bucket-2", "default").queryBucketKeys('key2')
    data = rdd.collect()

    assert data == []


def test_spark_df_kv_read_query2iKeys_one_entry(spark_context, docker_cli, riak_client, sql_context):

    bucket = riak_client.bucket_type('default').bucket('test-python-bucket-5')
    obj = riak.RiakObject(riak_client, bucket, 'key1')
    obj.content_type = 'text/plain'
    obj.data = 'test_data'
    obj.add_index('test_index_1_bin', 'index_val_1')
    obj.store()

    rdd = spark_context.riakBucket("test-python-bucket-5", "default").query2iKeys('test_index_1',"index_val_1")
    data = rdd.collect()

    assert data == [('key1', 'test_data')]

    rdd = spark_context.riakBucket("test-python-bucket-5", "default").query2iKeys('test_index_1',"index_val_2")
    data = rdd.collect()

    assert data == []


def test_spark_df_kv_read_query2iRange_one_entry(spark_context, docker_cli, riak_client, sql_context):

    bucket = riak_client.bucket_type('default').bucket('test-python-bucket-6')
    obj = riak.RiakObject(riak_client, bucket, 'key1')
    obj.content_type = 'text/plain'
    obj.data = 'test_data'
    obj.add_index('test_index_1_int', 1)
    obj.store()

    rdd = spark_context.riakBucket("test-python-bucket-6", "default").query2iRange('test_index_1',0,2)
    data = rdd.collect()

    assert data == [('key1', 'test_data')]  

    rdd = spark_context.riakBucket("test-python-bucket-6", "default").query2iRange('test_index_1',0,1)
    data = rdd.collect()

    assert data == [('key1', 'test_data')] 

    rdd = spark_context.riakBucket("test-python-bucket-6", "default").query2iRange('test_index_1',1,2)
    data = rdd.collect()

    assert data == [('key1', 'test_data')] 

    rdd = spark_context.riakBucket("test-python-bucket-6", "default").query2iRange('test_index_1',1,1)
    data = rdd.collect()

    assert data == [('key1', 'test_data')] 

    rdd = spark_context.riakBucket("test-python-bucket-6", "default").query2iRange('test_index_1',2,4)
    data = rdd.collect()

    assert data == []   

    

def test_spark_df_kv_write_read_partition_by_2i_range_one_entry(spark_context, docker_cli, riak_client, sql_context):

    bucket = riak_client.bucket_type('default').bucket('test-python-bucket-7')
    obj = riak.RiakObject(riak_client, bucket, 'key1')
    obj.content_type = 'text/plain'
    obj.data = 'test_data'
    obj.add_index('test_index_1_int', 1)
    obj.store()

    rdd = spark_context.riakBucket("test-python-bucket-7", "default").partitionBy2iRanges("test_index_1", (0, 2))
    data = rdd.collect()

    assert data == [('key1', 'test_data')]
    assert rdd.getNumPartitions() == 1

    rdd = spark_context.riakBucket("test-python-bucket-7", "default").partitionBy2iRanges("test_index_1", (0, 1))
    data = rdd.collect()

    assert data == [('key1', 'test_data')]
    assert rdd.getNumPartitions() == 1

    rdd = spark_context.riakBucket("test-python-bucket-7", "default").partitionBy2iRanges("test_index_1", (1, 2))
    data = rdd.collect()

    assert data == [('key1', 'test_data')]
    assert rdd.getNumPartitions() == 1

    rdd = spark_context.riakBucket("test-python-bucket-7", "default").partitionBy2iRanges("test_index_1", (1, 1))
    data = rdd.collect()

    assert data == [('key1', 'test_data')]
    assert rdd.getNumPartitions() == 1

    rdd = spark_context.riakBucket("test-python-bucket-7", "default").partitionBy2iRanges("test_index_1", (0, 2), (3, 4))
    data = rdd.collect()

    assert data == [('key1', 'test_data')]
    assert rdd.getNumPartitions() == 2

    rdd = spark_context.riakBucket("test-python-bucket-7", "default").partitionBy2iRanges("test_index_1", (3, 4))
    data = rdd.collect()

    assert data == []
    assert rdd.getNumPartitions() == 1

    rdd = spark_context.riakBucket("test-python-bucket-7", "default").partitionBy2iRanges("test_index_1", (3, 4), (5, 6))
    data = rdd.collect()

    assert data == []
    assert rdd.getNumPartitions() == 2


def test_spark_df_kv_write_read_query_all_1000_entries(spark_context, docker_cli, riak_client, sql_context):

    source_data = []
    for i in range(100):
        source_data.append({str('key'+str(i)) : {'field_key' : 'field_val'}})

    source_rdd = spark_context.parallelize(source_data)

    source_rdd.saveToRiak("test-python-bucket-3", "default")

    rdd = spark_context.riakBucket("test-python-bucket-3", "default").queryAll()
    data = rdd.collect()
    data = sorted(data, key = lambda x: x[0])

    assert len(data) == 100

    data_list = [(str('key'+str(i)), {'field_key' : 'field_val'}) for i in range(100)]
    data_list = sorted(data_list, key = lambda x: x[0])

    for i in range(100):
        assert data[i] == data_list[i]

def test_spark_df_kv_write_read_query_bucket_keys_3_entries(spark_context, docker_cli, riak_client, sql_context):

    key1 = 'key1'
    field1_key = 'field1_key'
    field1_val = 'field1_val'
    entry1 = {key1 : {field1_key : field1_val}}

    key2 = 'key2'
    field2_key = 'field2_key'
    field2_val = 'field2_val'
    entry2 = {key2 : {field2_key : field2_val}}

    key3 = 'key3'
    field3_key = 'field3_key'
    field3_val = 'field3_val'
    entry3 = {key3 : {field3_key : field3_val}}

    source_data = [entry1, entry2, entry3]
    source_rdd = spark_context.parallelize(source_data)

    source_rdd.saveToRiak("test-python-bucket-4", "default")

    rdd = spark_context.riakBucket("test-python-bucket-4", "default").queryBucketKeys('key1')
    data = rdd.collect()
    data = sorted(data, key=lambda x: x[0])

    assert data[0] == (key1, {field1_key : field1_val})

    rdd = spark_context.riakBucket("test-python-bucket-4", "default").queryBucketKeys('key2')
    data = rdd.collect()
    data = sorted(data, key=lambda x: x[0])

    assert data[0] == (key2, {field2_key : field2_val})

    rdd = spark_context.riakBucket("test-python-bucket-4", "default").queryBucketKeys('key3')
    data = rdd.collect()
    data = sorted(data, key=lambda x: x[0])

    assert data[0] == (key3, {field3_key : field3_val})

    rdd = spark_context.riakBucket("test-python-bucket-4", "default").queryBucketKeys('key1', 'key2')
    data = rdd.collect()
    data = sorted(data, key=lambda x: x[0])

    assert data[0] == (key1, {field1_key : field1_val})
    assert data[1] == (key2, {field2_key : field2_val})

    rdd = spark_context.riakBucket("test-python-bucket-4", "default").queryBucketKeys('key1', 'key3')
    data = rdd.collect()
    data = sorted(data, key=lambda x: x[0])

    assert data[0] == (key1, {field1_key : field1_val})
    assert data[1] == (key3, {field3_key : field3_val})

    rdd = spark_context.riakBucket("test-python-bucket-4", "default").queryBucketKeys('key2', 'key3')
    data = rdd.collect()
    data = sorted(data, key=lambda x: x[0])

    assert data[0] == (key2, {field2_key : field2_val})
    assert data[1] == (key3, {field3_key : field3_val})

    rdd = spark_context.riakBucket("test-python-bucket-4", "default").queryBucketKeys('key1','key2', 'key3')
    data = rdd.collect()
    data = sorted(data, key=lambda x: x[0])

    assert data[0] == (key1, {field1_key : field1_val})
    assert data[1] == (key2, {field2_key : field2_val})
    assert data[2] == (key3, {field3_key : field3_val})

'''


