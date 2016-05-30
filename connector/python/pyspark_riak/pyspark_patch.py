import pyspark
import types
from .riak_rdd import saveToRiak, riakBucket

def riak_context():
    pyspark.context.SparkContext.riakBucket = riakBucket
    pyspark.rdd.RDD.saveToRiak = saveToRiak

def riak_context(context):
    context.riakBucket = types.MethodType(riakBucket, context)
    pyspark.rdd.RDD.saveToRiak = saveToRiak
