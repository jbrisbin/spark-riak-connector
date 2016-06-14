from __future__ import print_function
import pytest
import sys
from operator import add
import findspark
findspark.init()
from pyspark import SparkContext, SparkConf, SQLContext, Row
import docker, os, subprocess, json, riak, time
import pyspark_riak

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

    #cli.pull('basho/riak-ts', stream=True)

    try:
        cli.stop('dev1')
        cli.remove_container('dev1')
    except Exception as e:
        pass

    riak_container = cli.create_container(image='basho/riak-ts', name='dev1', ports=[8087, 8098], \
                                          host_config=cli.create_host_config(port_bindings={8087: 8087, 8098: 8098}))
    cli.start('dev1')

    #request.addfinalizer(lambda: cli.remove_container('dev1'))
    #request.addfinalizer(lambda: cli.stop('dev1'))
    return cli
@pytest.mark.usefixtures("docker_cli")


@pytest.fixture(scope="session")
def spark_context(request, docker_cli):

    host, pb_port, hostAndPort = get_host_and_port(docker_cli)
    conf = (SparkConf().setMaster("local[*]").setAppName("pytest-pyspark-local-testing"))
    conf.set('spark.riak.connection.host', hostAndPort)
    conf.set("spark.riakts.bindings.timestamp", "useLong") 
    conf.set("spark.riak.connections.inactivity.timeout", 10000) 
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
    #host = '192.168.99.100'
    #pb_port = '8087'
    hostAndPort = ":".join([str(host), str(pb_port)])
    return host, pb_port, hostAndPort

def wait_until_ping(client,times=10):

    pinged = False
    i = 0
    while not pinged and i < times:
        print('Pinging')
        try:
            pinged = client.ping()
        except:
            pass
        i = i + 1
        time.sleep(1)

    if pinged:
        print('Pinged')
    return pinged


def create_table(client):

    riak_ts_table_name = 'spark-riak-%d' % int(time.time())
    riak_ts_table = client.table(riak_ts_table_name)

    create_sql = """CREATE TABLE %(table_name)s (
    field1 varchar not null,
    field2 varchar not null,
    datetime timestamp not null,
    data sint64,
    PRIMARY KEY ((field1, field2, quantum(datetime, 24, h)), field1, field2, datetime))
    """ % ({'table_name': riak_ts_table_name})

    result = riak_ts_table.query(create_sql)
    return riak_ts_table, riak_ts_table_name

###### TESTS #######

def test_connection(spark_context, docker_cli, riak_client, sql_context):

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

def test_spark_df_ts_write_read_one_entry(spark_context, docker_cli, riak_client, sql_context):

    #assert wait_until_ping(riak_client) == True
    #assert riak_client.is_alive() == True

    host, pb_port, hostAndPort = get_host_and_port(docker_cli)
    temp_table, temp_table_name = create_table(riak_client)

    field1_val = 'field1_val'
    field2_val = 'field2_val'
    datetime = 1293840000000
    data = 0

    test_row_in = [field1_val, field2_val, datetime, data]
    test_rdd = spark_context.parallelize([test_row_in]).cache()
    test_df = test_rdd.toDF(['field1', 'field2', 'datetime', 'data'])

    #assert wait_until_ping(riak_client) == True
    #assert riak_client.is_alive() == True

    #time.sleep(3)

    #assert wait_until_ping(riak_client) == True
    #assert riak_client.is_alive() == True

    #time.sleep(3)

    test_df.write.format('org.apache.spark.sql.riak').mode('Append').save(temp_table_name)

    #assert wait_until_ping(riak_client) == True 
    #assert riak_client.is_alive() == True

    read_df = sql_context.read \
                .format("org.apache.spark.sql.riak") \
                .load(temp_table_name)\
                .filter("""datetime >= %(start_date)s
                    AND datetime <=  %(end_date)s
                    AND field1 = '%(field1)s'
                    AND field2 = '%(field2)s'
                """ % ({'start_date': datetime-1, 'end_date': datetime+1, 'field1': field1_val, 'field2': field2_val}))

    test_row_out = read_df.collect()[0]
    
    assert test_row_out['field1'] == field1_val
    assert test_row_out['field2'] == field2_val
    assert test_row_out['datetime'] == datetime
    assert test_row_out['data'] == data


def test_spark_df_kv_write_read_query_all_one_entry(spark_context, docker_cli, riak_client, sql_context):

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

    key1 = 'key1'
    field1_key = 'field1_key'
    field1_val = 'field1_val'
    entry1 = {key1 : {field1_key : field1_val}}

    source_data = [entry1]
    source_rdd = spark_context.parallelize(source_data)

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

    time.sleep(3)

    source_rdd.saveToRiak("test-python-bucket-1", "default")

    rdd = spark_context.riakBucket("test-python-bucket-1", "default").queryAll()
    data = rdd.collect()[0]

    assert data == (key1, {field1_key : field1_val})


def test_spark_df_kv_write_read_query_bucket_keys_one_entry(spark_context, docker_cli, riak_client, sql_context):

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

    key1 = 'key1'
    field1_key = 'field1_key'
    field1_val = 'field1_val'
    entry1 = {key1 : {field1_key : field1_val}}

    source_data = [entry1]
    source_rdd = spark_context.parallelize(source_data)

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

    source_rdd.saveToRiak("test-python-bucket-2", "default")

    rdd = spark_context.riakBucket("test-python-bucket-2", "default").queryBucketKeys('key1')
    data = rdd.collect()

    assert data == [(key1, {field1_key : field1_val})]

    rdd = spark_context.riakBucket("test-python-bucket-2", "default").queryBucketKeys('key2')
    data = rdd.collect()

    assert data == []


def test_spark_df_kv_read_query2iKeys_one_entry(spark_context, docker_cli, riak_client, sql_context):

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

    bucket = riak_client.bucket_type('default').bucket('test-python-bucket-5')
    obj = riak.RiakObject(riak_client, bucket, 'key1')
    obj.content_type = 'text/plain'
    obj.data = 'test_data'
    obj.add_index('test_index_1_bin', 'index_val_1')

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

    obj.store()

    rdd = spark_context.riakBucket("test-python-bucket-5", "default").query2iKeys('test_index_1',"index_val_1")
    data = rdd.collect()

    assert data == [('key1', 'test_data')]

    rdd = spark_context.riakBucket("test-python-bucket-5", "default").query2iKeys('test_index_1',"index_val_2")
    data = rdd.collect()

    assert data == []


def test_spark_df_kv_read_query2iRange_one_entry(spark_context, docker_cli, riak_client, sql_context):

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

    bucket = riak_client.bucket_type('default').bucket('test-python-bucket-6')
    obj = riak.RiakObject(riak_client, bucket, 'key1')
    obj.content_type = 'text/plain'
    obj.data = 'test_data'
    obj.add_index('test_index_1_int', 1)

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

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
    
    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

    bucket = riak_client.bucket_type('default').bucket('test-python-bucket-7')
    obj = riak.RiakObject(riak_client, bucket, 'key1')
    obj.content_type = 'text/plain'
    obj.data = 'test_data'
    obj.add_index('test_index_1_int', 1)

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

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


def test_spark_df_kv_write_read_query_all_3_entries(spark_context, docker_cli, riak_client, sql_context):

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

    source_data = []
    for i in range(10):
        source_data.append({str('key'+str(i)) : {'field_key' : 'field_val'}})

    source_rdd = spark_context.parallelize(source_data)

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

    source_rdd.saveToRiak("test-python-bucket-3", "default")

    rdd = spark_context.riakBucket("test-python-bucket-3", "default").queryAll()
    data = rdd.collect()
    data = sorted(data, key=lambda x: x[0])

    assert len(data) == 10

    for i in range(10):
        assert data[i] == (str('key'+str(i)), {'field_key' : 'field_val'})

def test_spark_df_kv_write_read_query_bucket_keys_3_entries(spark_context, docker_cli, riak_client, sql_context):

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

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

    assert wait_until_ping(riak_client) == True
    assert riak_client.is_alive() == True

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




