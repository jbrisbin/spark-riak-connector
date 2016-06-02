import pyspark
import types
from .riak_rdd import saveToRiak, riakBucket

def riak_context(context=None):
    if context==None:
        pyspark.context.SparkContext.riakBucket = riakBucket
        pyspark.rdd.RDD.saveToRiak = saveToRiak
    else:
        context.riakBucket = types.MethodType(riakBucket, context)
        pyspark.rdd.RDD.saveToRiak = saveToRiak
