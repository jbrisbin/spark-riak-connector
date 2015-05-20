package com.basho.spark.connector.rdd;

import com.basho.spark.connector.japi.SparkJavaUtil;
import com.basho.spark.connector.japi.rdd.RiakJavaRDD;
import org.junit.Test;

import java.util.List;

import static org.junit.Assert.assertEquals;

public class JavaRDDReadTest extends AbstractJavaSparkTest {
    private String CREATION_INDEX = "creationNo";

    @Override
    public String jsonData() {
        return "[" +
                " { key: 'key-1', indexes: {creationNo: 1, category: 'neighbor'}, value: {user_id: 'u1', timestamp: '2014-11-24T13:14:04.823Z'}}" +
                ",{ key: 'key-2', indexes: {creationNo: 2, category: 'visitor'}, value:  {user_id: 'u1', timestamp: '2014-11-24T13:15:04.824Z'}}" +
                ",{ key: 'key-3', indexes: {creationNo: 3, category: 'neighbor'}, value: {user_id: 'u1', timestamp: '2014-11-24T13:18:04'}}" +
                ",{ key: 'key-4', indexes: {creationNo: 4, category: 'stranger'}, value: {user_id: 'u2', timestamp: '2014-11-24T13:14:04Z'}}" +
                ",{ key: 'key-5', indexes: {creationNo: 5, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T13:16:04.823Z'}}" +
                ",{ key: 'key-6', indexes: {creationNo: 6, category: 'stranger'}, value: {user_id: 'u3', timestamp: '2014-11-24T13:21:04.825Z'}}" +
        "]";
    }

    @Test
    public void readJSONASString(){
        RiakJavaRDD<String> rdd = SparkJavaUtil.javaFunctions(jsc).riakBucket(DEFAULT_NAMESPACE(), String.class).
                query2iRange(CREATION_INDEX, 1l, 4l);

        rdd.count();
        List<String> results = rdd.collect();
        assertEquals(4, results.size());

        assertEquals("{\n" +
            "  \"user_id\" : \"u1\",\n" +
            "  \"timestamp\" : \"2014-11-24T13:14:04.823Z\"\n" +
            "}", results.get(0));
    }
}