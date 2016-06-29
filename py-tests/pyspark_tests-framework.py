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
import math

#### Instructions to run #####
'''
These assume OSX:

1:Install docker-toolbox, docker-machine, and docker
2: Add a route to the docker machine with sudo route add 172.17.0.0/16 192.168.99.100
3: Install required python libraries sudo pip install pytest, findspark, docker-py, timeout_decorator, datetime, riak, tzlocal, pytz
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

#os.environ['SPARK_CLASSPATH'] = '/Users/basho/.m2/repository/com/basho/riak/spark-riak-connector/1.5.1-SNAPSHOT/spark-riak-connector-1.5.1-SNAPSHOT-uber.jar'
#os.environ['SPARK_CLASSPATH'] = '/vagrant/spark-riak-connector-1.5.1-SNAPSHOT-uber.jar'
###### FIXTURES #######

@pytest.fixture(scope="session")
def docker_cli(request):
    conf = SparkConf().setMaster("local[*]").setAppName("pytest-pyspark-py4j")
    sc = SparkContext(conf=conf)
    docker_cli = sc._gateway.jvm.com.basho.riak.test.cluster.DockerRiakCluster(1, 1)
    docker_cli.start()
    sc.stop()
    request.addfinalizer(lambda: docker_cli.stop())
    return docker_cli
@pytest.mark.usefixtures("docker_cli")

@pytest.fixture(scope="session")
def spark_context(request, docker_cli):
    conf = SparkConf().setMaster("local[*]").setAppName("pytest-pyspark-local-testing")
    host_and_port = get_host_and_port(docker_cli)
    conf.set('spark.riak.connection.host', host_and_port)
    spark_context = SparkContext(conf=conf)
    spark_context.setLogLevel("ERROR")
    pyspark_riak.riak_context(spark_context)
    request.addfinalizer(lambda: spark_context.stop())
    return spark_context
@pytest.mark.usefixtures("spark_context")

@pytest.fixture(scope="session")
def sql_context(request, spark_context):
    sqlContext = SQLContext(spark_context)
    return sqlContext
@pytest.mark.usefixtures("sql_context")


@pytest.fixture(scope="session")
def riak_client(request, docker_cli):
    nodes = get_nodes(docker_cli)
    client = riak.RiakClient(nodes=nodes)
    request.addfinalizer(lambda: client.close())
    return client
@pytest.mark.usefixtures("riak_client")

###### FUNCTIONS #######

def get_nodes(docker_cli):
    print(docker_cli.getIps())
    pb_port = 8087
    http_port = 8098
    nodes = [{'host': ip, 'pb_port': pb_port, 'http_port': http_port} for ip in docker_cli.getIps()]
    return nodes

def get_host_and_port(docker_cli):
    pb_port = 8087
    nodes = [":".join([ip, str(pb_port)]) for ip in docker_cli.getIps()]
    host_and_port= ",".join(nodes)
    return host_and_port

def retry_func_with_timeout(func, times, timeout, signal, args, use_condition, condition_func, condition_val, test_func, test_args):

    if timeout > 0:
        @timeout_decorator.timeout(timeout, use_signals=signal)
        def temp_func(run_func, run_args):
            return run_func(*run_args)
    else:
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
            #print(func)
            print(e)
            pass

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

##### CONDITIONS #######

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

    #print(result.rdd.getNumPartitions())
    #print(val.rdd.getNumPartitions())
    if sorted(result.collect(), key=lambda x: x[2]) == sorted(val.collect(), key=lambda x: x[2]):
        return True
    else:
        return False

def df_read_input_split_count_condition(result, val, func, args):

    test_df = val[0]
    N = val[1]
    M = val[2]
    S = val[3]

    print('N='+str(N))
    print('M='+str(M))
    print('S='+str(S))

    print(len(sorted(result.collect(), key=lambda x: x[2])))
    print(len(sorted(test_df.collect(), key=lambda x: x[2])))

    print(test_df.rdd.getNumPartitions())
    print(result.rdd.getNumPartitions())

    if sorted(result.collect(), key=lambda x: x[2]) == sorted(test_df.collect(), key=lambda x: x[2]) and result.rdd.getNumPartitions() == S:
        return True
    else:
        return False

def kv_multiget_condition(result, val, func, args):
    print(result)
    test_data = [{x.key: x.data} for x in result]

    if sorted(test_data) == sorted(val):
        return True
    else:
        return False

def kv_query_condition(result, val, func, args):

    print(sorted(result.collect(), key=lambda x: x[0]))
    print(sorted(val, key=lambda x: x[0]))

    if sorted(result.collect(), key=lambda x: x[0]) == sorted(val, key=lambda x: x[0]):
        return True
    else:
        return False

def kv_partition_condition(result, val, func, args):


    print(sorted(result.collect(), key=lambda x: x[0]))
    print(sorted(val[0], key=lambda x: x[0]))

    if sorted(result.collect(), key=lambda x: x[0]) == sorted(val[0], key=lambda x: x[0]) and result.getNumPartitions() == val[1]:
        return True
    else:
        return False

###### TESTS #######

def _test_connection(spark_context, docker_cli, riak_client, sql_context):

    assert retry_func_with_timeout(func=riak_client.ping, 
                                   times=5, 
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
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    assert retry_func_with_timeout(func=riak_client.bucket('temp_bucket').get, 
                                   times=5, 
                                   timeout=10, 
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
                                   times=5, 
                                   timeout=10, 
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
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    # assert retry_func_with_timeout(func=riak_client.ts_get, 
    #                                times=5, 
    #                                timeout=10, 
    #                                signal=True, 
    #                                args=[riak_ts_table_name, ['field1_val', 'field2_val', unix_time_millis(datetime.datetime(2015, 1, 1, 12, 0, 0))]], 
    #                                use_condition=True, 
    #                                condition_func=ts_get_condition, 
    #                                condition_val=[['field1_val', 'field2_val', unix_time_millis(datetime.datetime(2015, 1, 1, 12, 0, 0)), 0]],
    #                                test_func=None,
    #                                test_args=None
    #                                )[0] == True

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
    timestamp_data, start_timestamp, end_timestamp, long_data, start_long, end_long = make_data_timestamp(seed_date, N, M)
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
                                   times=5, 
                                   timeout=10, 
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
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True
    time.sleep(3)


    temp_filter = """datetime >= %(start_date)s
                    AND datetime <=  %(end_date)s
                    AND field1 = '%(field1)s'
                    AND field2 = '%(field2)s'
                """ % ({'start_date': unix_time_millis(start), 'end_date': unix_time_millis(end), 'field1': 'field1_val', 'field2': 'field2_val'})

    print(retry_func_with_timeout(func=sql_context.read.format("org.apache.spark.sql.riak").option("spark.riakts.bindings.timestamp", "useLong").load(riak_ts_table_name).filter, 
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_verify_condition, 
                                   condition_val=test_df,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True)

def _test_spark_df_ts_read_use_long_ts_quantum(N, M, spark_context, docker_cli, riak_client, sql_context):

    riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

    assert retry_func_with_timeout(func=riak_ts_table.query,
                                   times=5, 
                                   timeout=10, 
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
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True
    time.sleep(3)


    temp_filter = """datetime >= %(start_date)s
                    AND datetime <=  %(end_date)s
                    AND field1 = '%(field1)s'
                    AND field2 = '%(field2)s'
                """ % ({'start_date': unix_time_millis(start), 'end_date': unix_time_millis(end), 'field1': 'field1_val', 'field2': 'field2_val'})

    assert retry_func_with_timeout(func=sql_context.read.format("org.apache.spark.sql.riak").option("spark.riakts.bindings.timestamp", "useLong").option("spark.riak.partitioning.ts-quantum", "24h").load(riak_ts_table_name).filter, 
                                   times=5, 
                                   timeout=10, 
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
                                   times=3, 
                                   timeout=10, 
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


    assert retry_func_with_timeout(func=test_df.write.format('org.apache.spark.sql.riak').mode('Append').save,
                                   times=3, 
                                   timeout=30, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=True, 
                                   condition_func=general_condition, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True
    time.sleep(3)

    temp_filter = """datetime >= CAST('%(start_date)s' AS TIMESTAMP)
                    AND datetime <=  CAST('%(end_date)s' AS TIMESTAMP)
                    AND field1 = '%(field1)s'
                    AND field2 = '%(field2)s'
                """ % ({'start_date': start_timestamp, 'end_date': end_timestamp, 'field1': 'field1_val', 'field2': 'field2_val'})

    assert retry_func_with_timeout(func=sql_context.read.format("org.apache.spark.sql.riak").option("spark.riakts.bindings.timestamp", "useTimestamp").load(riak_ts_table_name).filter, 
                                   times=3, 
                                   timeout=30, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_verify_condition, 
                                   condition_val=test_df,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def _test_spark_df_ts_read_use_timestamp_ts_quantum(N, M, spark_context, docker_cli, riak_client, sql_context):

    riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

    assert retry_func_with_timeout(func=riak_ts_table.query,
                                   times=3, 
                                   timeout=10, 
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


    assert retry_func_with_timeout(func=test_df.write.format('org.apache.spark.sql.riak').mode('Append').save,
                                   times=3, 
                                   timeout=30, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=True, 
                                   condition_func=general_condition, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True
    time.sleep(3)

    print(start_timestamp)
    print(end_timestamp)
    temp_filter = """datetime >= CAST('%(start_date)s' AS TIMESTAMP)
                    AND datetime <=  CAST('%(end_date)s' AS TIMESTAMP)
                    AND field1 = '%(field1)s'
                    AND field2 = '%(field2)s'
                """ % ({'start_date': start_timestamp, 'end_date': end_timestamp, 'field1': 'field1_val', 'field2': 'field2_val'})

    assert retry_func_with_timeout(func=sql_context.read.format("org.apache.spark.sql.riak").option("spark.riakts.bindings.timestamp", "useTimestamp").option("spark.riak.partitioning.ts-quantum", "24h").load(riak_ts_table_name).filter, 
                                   times=3, 
                                   timeout=30, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_verify_condition, 
                                   condition_val=test_df,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def _test_spark_df_ts_range_query_input_split_count_use_long(N, M, S,spark_context, docker_cli, riak_client, sql_context):

    riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

    assert retry_func_with_timeout(func=riak_ts_table.query,
                                   times=5, 
                                   timeout=10, 
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


    test_func_a = test_df.write.format('org.apache.spark.sql.riak').mode('Append').save

    assert retry_func_with_timeout(func=test_func_a,
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True
    time.sleep(3)

    temp_filter = """datetime >= %(start_date)s
                    AND datetime <=  %(end_date)s
                    AND field1 = '%(field1)s'
                    AND field2 = '%(field2)s'
                """ % ({'start_date': unix_time_millis(start), 'end_date': unix_time_millis(end), 'field1': 'field1_val', 'field2': 'field2_val'})


    print(temp_filter)
    print(start)
    print(end)
    
    test_func_b = sql_context.read.format("org.apache.spark.sql.riak") \
                .option("spark.riakts.bindings.timestamp", "useLong") \
                .load(riak_ts_table_name).filter


    assert retry_func_with_timeout(func=test_func_b, 
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_verify_condition, 
                                   condition_val=test_df,
                                   test_func=None,
                                   test_args=None
                                   )[0] == False   
    

    test_func_c = sql_context.read.format("org.apache.spark.sql.riak") \
                .option("spark.riakts.bindings.timestamp", "useLong") \
                .option("spark.riak.input.split.count", str(S)) \
                .option("spark.riak.partitioning.ts-range-field-name", "datetime") \
                .load(riak_ts_table_name).filter

    assert retry_func_with_timeout(func=test_func_c, 
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_input_split_count_condition, 
                                   condition_val=[test_df, N,M,S],
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def _test_spark_df_ts_range_query_input_split_count_use_long_ts_quantum(N, M, S,spark_context, docker_cli, riak_client, sql_context):

    riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

    assert retry_func_with_timeout(func=riak_ts_table.query,
                                   times=5, 
                                   timeout=10, 
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


    test_func_a = test_df.write.format('org.apache.spark.sql.riak').mode('Append').save

    assert retry_func_with_timeout(func=test_func_a,
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True
    time.sleep(3)

    temp_filter = """datetime >= %(start_date)s
                    AND datetime <=  %(end_date)s
                    AND field1 = '%(field1)s'
                    AND field2 = '%(field2)s'
                """ % ({'start_date': unix_time_millis(start), 'end_date': unix_time_millis(end), 'field1': 'field1_val', 'field2': 'field2_val'})


    print(temp_filter)
    print(start)
    print(end)
    
    test_func_b = sql_context.read.format("org.apache.spark.sql.riak") \
                .option("spark.riakts.bindings.timestamp", "useLong") \
                .load(riak_ts_table_name).filter


    assert retry_func_with_timeout(func=test_func_b, 
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_verify_condition, 
                                   condition_val=test_df,
                                   test_func=None,
                                   test_args=None
                                   )[0] == False   
    '''
    test_func_c = sql_context.read.format("org.apache.spark.sql.riak") \
                .option("spark.riakts.bindings.timestamp", "useLong") \
                .option("spark.riak.partitioning.ts-quantum", "1s") \
                .option("spark.riak.input.split.count", str(S)) \
                .option("spark.riak.partitioning.ts-range-field-name", "datetime") \
                .load(riak_ts_table_name).filter

    assert retry_func_with_timeout(func=test_func_c, 
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_input_split_count_condition, 
                                   condition_val=[test_df, N,M,S],
                                   test_func=None,
                                   test_args=None
                                   )[0] == False
    '''
    test_func_d = sql_context.read.format("org.apache.spark.sql.riak") \
                .option("spark.riakts.bindings.timestamp", "useLong") \
                .option("spark.riak.partitioning.ts-quantum", "24h") \
                .option("spark.riak.input.split.count", str(S)) \
                .option("spark.riak.partitioning.ts-range-field-name", "datetime") \
                .load(riak_ts_table_name).filter

    assert retry_func_with_timeout(func=test_func_d, 
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_input_split_count_condition, 
                                   condition_val=[test_df, N,M,S],
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def _test_spark_df_ts_range_query_input_split_count_use_timestamp(N, M, S,spark_context, docker_cli, riak_client, sql_context):

    riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

    assert retry_func_with_timeout(func=riak_ts_table.query,
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[create_sql], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    seed_date = datetime.datetime(2015, 1, 1, 12, 0, 0)
    timestamp_data, start_timestamp, end_timestamp, long_data, start_long, end_long = make_data_timestamp(seed_date, N, M)
    test_rdd = spark_context.parallelize(timestamp_data)
    test_df = test_rdd.toDF(['field1', 'field2', 'datetime', 'data'])


    test_func_a = test_df.write.format('org.apache.spark.sql.riak').mode('Append').save

    assert retry_func_with_timeout(func=test_func_a,
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True
    time.sleep(3)

    temp_filter = """datetime >= CAST('%(start_date)s' AS TIMESTAMP)
                    AND datetime <=  CAST('%(end_date)s' AS TIMESTAMP)
                    AND field1 = '%(field1)s'
                    AND field2 = '%(field2)s'
                """ % ({'start_date': start_timestamp, 'end_date': end_timestamp, 'field1': 'field1_val', 'field2': 'field2_val'})


    print(temp_filter)
    print(start_timestamp)
    print(end_timestamp)
    
    test_func_b = sql_context.read.format("org.apache.spark.sql.riak") \
                .option("spark.riakts.bindings.timestamp", "useTimestamp") \
                .load(riak_ts_table_name).filter


    assert retry_func_with_timeout(func=test_func_b, 
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_verify_condition, 
                                   condition_val=test_df,
                                   test_func=None,
                                   test_args=None
                                   )[0] == False   
    

    test_func_c = sql_context.read.format("org.apache.spark.sql.riak") \
                .option("spark.riakts.bindings.timestamp", "useTimestamp") \
                .option("spark.riak.input.split.count", str(S)) \
                .option("spark.riak.partitioning.ts-range-field-name", "datetime") \
                .load(riak_ts_table_name).filter

    assert retry_func_with_timeout(func=test_func_c, 
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_input_split_count_condition, 
                                   condition_val=[test_df, N,M,S],
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def _test_spark_df_ts_range_query_input_split_count_use_timestamp_ts_quantum(N, M, S,spark_context, docker_cli, riak_client, sql_context):

    riak_ts_table_name, create_sql, riak_ts_table = setup_table(riak_client)

    assert retry_func_with_timeout(func=riak_ts_table.query,
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[create_sql], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    seed_date = datetime.datetime(2015, 1, 1, 12, 0, 0)
    timestamp_data, start_timestamp, end_timestamp, long_data, start_long, end_long = make_data_timestamp(seed_date, N, M)
    test_rdd = spark_context.parallelize(timestamp_data)
    test_df = test_rdd.toDF(['field1', 'field2', 'datetime', 'data'])


    test_func_a = test_df.write.format('org.apache.spark.sql.riak').mode('Append').save

    assert retry_func_with_timeout(func=test_func_a,
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[riak_ts_table_name], 
                                   use_condition=False, 
                                   condition_func=None, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True
    #time.sleep(3)

    temp_filter = """datetime >= CAST('%(start_date)s' AS TIMESTAMP)
                    AND datetime <=  CAST('%(end_date)s' AS TIMESTAMP)
                    AND field1 = '%(field1)s'
                    AND field2 = '%(field2)s'
                """ % ({'start_date': start_timestamp, 'end_date': end_timestamp, 'field1': 'field1_val', 'field2': 'field2_val'})


    #print(temp_filter)
    #print(start_timestamp)
    #print(end_timestamp)
    

    '''
    test_func_b = sql_context.read.format("org.apache.spark.sql.riak") \
                .option("spark.riakts.bindings.timestamp", "useTimestamp") \
                .load(riak_ts_table_name).filter


    assert retry_func_with_timeout(func=test_func_b, 
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_verify_condition, 
                                   condition_val=test_df,
                                   test_func=None,
                                   test_args=None
                                   )[0] == False   

    '''
    

    test_func_c = sql_context.read.format("org.apache.spark.sql.riak") \
                .option("spark.riakts.bindings.timestamp", "useTimestamp") \
                .option("spark.riak.partitioning.ts-quantum", "24h") \
                .option("spark.riak.input.split.count", str(S)) \
                .option("spark.riak.partitioning.ts-range-field-name", "datetime") \
                .load(riak_ts_table_name).filter

    assert retry_func_with_timeout(func=test_func_c, 
                                   times=5, 
                                   timeout=10, 
                                   signal=True, 
                                   args=[temp_filter], 
                                   use_condition=True, 
                                   condition_func=df_read_input_split_count_condition, 
                                   condition_val=[test_df, N,M,S],
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

###### Riak KV Tests ###### 

def _test_spark_rdd_write_kv(N, spark_context, docker_cli, riak_client, sql_context):

    source_data = []
    keys = []
    for i in range(N):
        keys.append(str('key'+str(i)))
        source_data.append({str('key'+str(i)) : {u'data' : i}})

    source_rdd = spark_context.parallelize(source_data)

    assert retry_func_with_timeout(func=source_rdd.saveToRiak,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=["test-python-bucket-3", "default"], 
                                   use_condition=True, 
                                   condition_func=general_condition, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    assert retry_func_with_timeout(func=riak_client.bucket("test-python-bucket-3").multiget,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=[keys], 
                                   use_condition=True, 
                                   condition_func=kv_multiget_condition, 
                                   condition_val=source_data,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def _test_spark_rdd_kv_read_query_all(N, spark_context, docker_cli, riak_client, sql_context):

    source_data = []
    test_data = []
    keys = []

    for i in range(N):
        keys.append(str('key'+str(i)))
        source_data.append({str('key'+str(i)) : {u'data' : i}})
        test_data.append( (str('key'+str(i)),{u'data' : i}) )

    source_rdd = spark_context.parallelize(source_data)

    assert retry_func_with_timeout(func=source_rdd.saveToRiak,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=["test-python-bucket-4", "default"], 
                                   use_condition=True, 
                                   condition_func=general_condition, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    assert retry_func_with_timeout(func=spark_context.riakBucket("test-python-bucket-4", "default").queryAll,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=[], 
                                   use_condition=True, 
                                   condition_func=kv_query_condition, 
                                   condition_val=test_data,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def _test_spark_rdd_kv_read_query_bucket_keys(N, spark_context, docker_cli, riak_client, sql_context):

    source_data = []
    test_data = []
    keys = []
    bad_keys = []

    for i in range(N):
        keys.append(str('key'+str(i)))
        source_data.append({str('key'+str(i)) : {u'data' : i}})
        test_data.append( (str('key'+str(i)),{u'data' : i}))
        bad_keys.append(str(i))


    source_rdd = spark_context.parallelize(source_data)

    assert retry_func_with_timeout(func=source_rdd.saveToRiak,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=["test-python-bucket-5", "default"], 
                                   use_condition=True, 
                                   condition_func=general_condition, 
                                   condition_val=None,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    assert retry_func_with_timeout(func=spark_context.riakBucket("test-python-bucket-5", "default").queryBucketKeys,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=keys, 
                                   use_condition=True, 
                                   condition_func=kv_query_condition, 
                                   condition_val=test_data,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    assert retry_func_with_timeout(func=spark_context.riakBucket("test-python-bucket-5", "default").queryBucketKeys,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=bad_keys, 
                                   use_condition=True, 
                                   condition_func=kv_query_condition, 
                                   condition_val=[],
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def _test_spark_rdd_kv_read_query_2i_keys(N, spark_context, docker_cli, riak_client, sql_context):

    bucket = riak_client.bucket_type('default').bucket('test-python-bucket-5')
    test_data = []
    string2i = ['string_index']
    integer2i = ['integer_index']

    for i in range(N):

        obj = riak.RiakObject(riak_client, bucket, str('key'+str(i)))
        obj.content_type = 'application/json'
        obj.data = {u'data' : i}
        obj.add_index('string_index_bin', 'string_val_'+str(i))
        obj.add_index('integer_index_int', i)

        assert retry_func_with_timeout(func=obj.store, 
                                           times=5, 
                                           timeout=2, 
                                           signal=True, 
                                           args=[], 
                                           use_condition=False, 
                                           condition_func=None, 
                                           condition_val=None,
                                           test_func=None,
                                           test_args=None
                                           )[0] == True

        test_data.append((str('key'+str(i)),{u'data' : i}))

        string2i.append('string_val_'+str(i))
        integer2i.append(i)

    assert retry_func_with_timeout(func=spark_context.riakBucket("test-python-bucket-5", "default").query2iKeys,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=string2i,
                                   use_condition=True, 
                                   condition_func=kv_query_condition, 
                                   condition_val=test_data,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    assert retry_func_with_timeout(func=spark_context.riakBucket("test-python-bucket-5", "default").query2iKeys,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=integer2i,
                                   use_condition=True, 
                                   condition_func=kv_query_condition, 
                                   condition_val=test_data,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    assert retry_func_with_timeout(func=spark_context.riakBucket("test-python-bucket-5", "default").query2iKeys,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=integer2i,
                                   use_condition=True, 
                                   condition_func=kv_query_condition, 
                                   condition_val=test_data,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def _test_spark_rdd_kv_read_query2iRange(N, spark_context, docker_cli, riak_client, sql_context):

    bucket = riak_client.bucket_type('default').bucket('test-python-bucket-9')
    test_data = []
    integer2i = []

    for i in range(N):

        obj = riak.RiakObject(riak_client, bucket, str('key'+str(i)))
        obj.content_type = 'application/json'
        obj.data = {u'data' : i}
        obj.add_index('integer_index_int', i)

        assert retry_func_with_timeout(func=obj.store, 
                                           times=5, 
                                           timeout=2, 
                                           signal=True, 
                                           args=[], 
                                           use_condition=False, 
                                           condition_func=None, 
                                           condition_val=None,
                                           test_func=None,
                                           test_args=None
                                           )[0] == True

        test_data.append((str('key'+str(i)),{u'data' : i}))
        integer2i.append(i)

    assert retry_func_with_timeout(func=spark_context.riakBucket("test-python-bucket-9", "default").query2iRange,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=['integer_index', integer2i[0], integer2i[-1]],
                                   use_condition=True, 
                                   condition_func=kv_query_condition, 
                                   condition_val=test_data,
                                   test_func=None,
                                   test_args=None
                                   )[0] == True


    assert retry_func_with_timeout(func=spark_context.riakBucket("test-python-bucket-9", "default").query2iRange,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=['integer_index', integer2i[0], integer2i[ int(math.floor((len(integer2i)-1)/2))] ],
                                   use_condition=True, 
                                   condition_func=kv_query_condition, 
                                   condition_val=test_data[:int(math.floor((len(integer2i)-1)/2))+1],
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    assert retry_func_with_timeout(func=spark_context.riakBucket("test-python-bucket-9", "default").query2iRange,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=[ 'integer_index', integer2i[ int(math.floor((len(integer2i)-1)/2))], integer2i[-1] ],
                                   use_condition=True, 
                                   condition_func=kv_query_condition, 
                                   condition_val=test_data[int(math.floor((len(integer2i)-1)/2)):],
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    assert retry_func_with_timeout(func=spark_context.riakBucket("test-python-bucket-9", "default").query2iRange,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=[ 'integer_index', N, 2*N ],
                                   use_condition=True, 
                                   condition_func=kv_query_condition, 
                                   condition_val=[],
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

def _test_spark_rdd_kv_read_partition_by_2i_range(N, spark_context, docker_cli, riak_client, sql_context):


    bucket = riak_client.bucket_type('default').bucket('test-python-bucket-11')
    test_data = []
    integer2i = []
    partitions = ['integer_index']
    bad_partitions = ['integer_index']

    for i in range(N):

        obj = riak.RiakObject(riak_client, bucket, str('key'+str(i)))
        obj.content_type = 'application/json'
        obj.data = {u'data' : i}
        obj.add_index('integer_index_int', i)

        assert retry_func_with_timeout(func=obj.store, 
                                           times=5, 
                                           timeout=2, 
                                           signal=True, 
                                           args=[], 
                                           use_condition=False, 
                                           condition_func=None, 
                                           condition_val=None,
                                           test_func=None,
                                           test_args=None
                                           )[0] == True

        test_data.append((str('key'+str(i)),{u'data' : i}))
        integer2i.append(i)
        partitions.append((i,i))
        bad_partitions.append((N+i,N+i))


    assert retry_func_with_timeout(func=spark_context.riakBucket("test-python-bucket-11", "default").partitionBy2iRanges,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=partitions,
                                   use_condition=True, 
                                   condition_func=kv_partition_condition, 
                                   condition_val=[test_data, N],
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

    assert retry_func_with_timeout(func=spark_context.riakBucket("test-python-bucket-11", "default").partitionBy2iRanges,
                                   times=10, 
                                   timeout=3, 
                                   signal=True, 
                                   args=bad_partitions,
                                   use_condition=True, 
                                   condition_func=kv_partition_condition, 
                                   condition_val=[[], N],
                                   test_func=None,
                                   test_args=None
                                   )[0] == True

###### Run Tests ######

def test_spark_riak_connector_kv(spark_context, docker_cli, riak_client, sql_context):

    _test_connection(spark_context, docker_cli, riak_client, sql_context)
    #_test_spark_rdd_write_kv(10, spark_context, docker_cli, riak_client, sql_context)
    # _test_spark_rdd_kv_read_query_all(5, spark_context, docker_cli, riak_client, sql_context)
    # _test_spark_rdd_kv_read_query_bucket_keys(10, spark_context, docker_cli, riak_client, sql_context)
    # _test_spark_rdd_kv_read_query_2i_keys(100, spark_context, docker_cli, riak_client, sql_context)
    # _test_spark_rdd_kv_read_query2iRange(50, spark_context, docker_cli, riak_client, sql_context)
    # _test_spark_rdd_kv_read_partition_by_2i_range(50, spark_context, docker_cli, riak_client, sql_context)

def test_spark_riak_connector_ts(spark_context, docker_cli, riak_client, sql_context):

    _test_connection(spark_context, docker_cli, riak_client, sql_context)
    _test_spark_df_ts_write_use_timestamp(10, 5, spark_context, docker_cli, riak_client, sql_context)
    _test_spark_df_ts_write_use_long(10, 5, spark_context, docker_cli, riak_client, sql_context)
    _test_spark_df_ts_read_use_timestamp(1000, 5, spark_context, docker_cli, riak_client, sql_context)
    _test_spark_df_ts_read_use_timestamp_ts_quantum(1000, 5, spark_context, docker_cli, riak_client, sql_context)
    _test_spark_df_ts_read_use_long(1000, 5, spark_context, docker_cli, riak_client, sql_context)
    _test_spark_df_ts_read_use_long_ts_quantum(1000, 5, spark_context, docker_cli, riak_client, sql_context)
    _test_spark_df_ts_range_query_input_split_count_use_long(100, 500, 1, spark_context, docker_cli, riak_client, sql_context)
    _test_spark_df_ts_range_query_input_split_count_use_long_ts_quantum(100, 500, 1, spark_context, docker_cli, riak_client, sql_context)
    _test_spark_df_ts_range_query_input_split_count_use_timestamp(1, 500, 1, spark_context, docker_cli, riak_client, sql_context)
    _test_spark_df_ts_range_query_input_split_count_use_timestamp_ts_quantum(100, 500, 1, spark_context, docker_cli, riak_client, sql_context)






