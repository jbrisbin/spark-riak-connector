package com.basho.spark.connector.japi;
import com.basho.riak.client.core.query.Location;
import com.basho.riak.client.core.query.Namespace;
import com.basho.riak.client.core.query.RiakObject;
import com.basho.spark.connector.japi.rdd.RiakJavaPairRDD;
import com.basho.spark.connector.japi.rdd.RiakJavaRDD;
import com.basho.spark.connector.rdd.RiakRDD;
import com.basho.spark.connector.rdd.RiakRDD$;
import com.basho.spark.connector.util.RiakObjectConversionUtil;
import org.apache.spark.SparkContext;

import scala.Function2;
import scala.Tuple2;
import scala.reflect.ClassTag;
import scala.runtime.AbstractFunction2;


import java.io.Serializable;

import static com.basho.spark.connector.util.JavaUtil.getClassTag;

public class SparkContextJavaFunctions {
    protected static class ConversionFunction<T> extends AbstractFunction2<Location, RiakObject, T> implements Serializable {
        private final ClassTag<T> classTag;

        public ConversionFunction(ClassTag<T> classTag) {
            this.classTag = classTag;
        }

        @Override
        public T apply(Location l, RiakObject r) {
            return RiakObjectConversionUtil.from(l, r, classTag);
        }

        public static <T> ConversionFunction create(ClassTag<T> classTag){
            return new ConversionFunction(classTag);
        }
    };

    public final SparkContext sparkContext;

    SparkContextJavaFunctions(SparkContext sparkContext) {
        this.sparkContext = sparkContext;
    }

    public <T> RiakJavaRDD<T> toJavaRDD(RiakRDD<T> rdd, Class<T> targetClass) {
        return new RiakJavaRDD<>(rdd, getClassTag(targetClass));
    }

    public <T> RiakJavaRDD<T> riakBucket(Namespace ns, Class<T> valueClass) {
        return this.riakBucket(ns.getBucketNameAsString(), ns.getBucketTypeAsString(), valueClass);
    }

    public <T> RiakJavaRDD<T> riakBucket(String bucketName, String bucketType, Class<T> valueClass) {
        final ClassTag<T> classTag = getClassTag(valueClass);
        final String bucketTypeStr = bucketType == null || bucketType.isEmpty() ? "default" : bucketType;
        final RiakRDD<T> rdd = RiakRDD$.MODULE$.apply(sparkContext, bucketTypeStr, bucketName,
                ConversionFunction.create(classTag), classTag);
        return new RiakJavaRDD(rdd, classTag);
    }

    public <K, V> RiakJavaPairRDD<K, V> riakBucket(Namespace ns, Function2<Location, RiakObject, Tuple2<K, V>> convert, Class<K> keyClass, Class<V> valueClass) {
        return this.riakBucket(ns.getBucketNameAsString(), convert, ns.getBucketNameAsString(), keyClass, valueClass);
    }

    public <K, V> RiakJavaPairRDD<K, V> riakBucket(String bucketName, Function2<Location, RiakObject, Tuple2<K, V>> convert, String bucketType, Class<K> keyClass, Class<V> valueClass) {
        final ClassTag<K> kClassTag = getClassTag(keyClass);
        final ClassTag<V> vClassTag = getClassTag(valueClass);
        final String bucketTypeStr = bucketType == null || bucketType.isEmpty() ? "default" : bucketType;
        final RiakRDD<Tuple2<K, V>> rdd = RiakRDD$.MODULE$.apply(sparkContext, bucketTypeStr, bucketName, convert, kClassTag, vClassTag);
        return new RiakJavaPairRDD(rdd, kClassTag, kClassTag);
    }
}