package com.lovecws.mumu.kite;

import com.sun.org.apache.regexp.internal.RE;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: kite 配置
 * @date 2018-01-10 11:15:
 */
public class KiteConfiguration {

    public static final String DEFAULT_HADOOP_ADDRESS = "hdfs://192.168.11.25:9000";

    public static final String DEFAULT_PATH = "/mumu/kite";

    public static final String DEFAULT_NAMESPACE = "/kitenamespace/";

    public static final String DEFAULT_ZOOKEEPER_ADDRESS="192.168.11.25:2181/";

    //dataset:hdfs:/<path>/<namespace>/<dataset-name>
    public static String HDFS() {
        return "dataset:" + DEFAULT_HADOOP_ADDRESS + DEFAULT_PATH + DEFAULT_NAMESPACE;
    }

    //dataset:file:/<path>/<namespace>/<dataset-name>
    public static String LOCAL() {
        return "dataset:file://" + DEFAULT_PATH + DEFAULT_NAMESPACE;
    }

    //dataset:hive:<namespace>/<dataset>
    public static String HIVE() {
        return "dataset:hive://" + DEFAULT_NAMESPACE;
    }

    //dataset:hbase:<zookeeper>/<dataset-name>
    public static String HBASE() {
        return "dataset:hbase://" + DEFAULT_ZOOKEEPER_ADDRESS;
    }

}
