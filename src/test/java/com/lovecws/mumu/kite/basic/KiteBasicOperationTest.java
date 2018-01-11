package com.lovecws.mumu.kite.basic;

import com.lovecws.mumu.kite.KiteConfiguration;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: kite基本使用测试
 * @date 2018-01-10 17:40:
 */
public class KiteBasicOperationTest {

    @Test
    public void create() {
        new KiteBasicOperation().create(KiteConfiguration.HDFS() + "parquetdataset", "parquet");
    }

    @Test
    public void delete() {
        new KiteBasicOperation().delete(KiteConfiguration.HDFS() + "firstdataset");
    }

    @Test
    public void update() {
        new KiteBasicOperation().update(KiteConfiguration.HDFS() + "firstdataset");
    }

    @Test
    public void load() {
        new KiteBasicOperation().load(KiteConfiguration.HDFS() + "firstdataset");
    }

    @Test
    public void view() {
        new KiteBasicOperation().view("view:" + KiteConfiguration.DEFAULT_HADOOP_ADDRESS + KiteConfiguration.DEFAULT_PATH + KiteConfiguration.DEFAULT_NAMESPACE + "firstdataset");
    }

    @Test
    public void exists() {
        new KiteBasicOperation().exists(KiteConfiguration.HDFS() + "firstdataset");
    }

    @Test
    public void list() {
        new KiteBasicOperation().list("repo:" + KiteConfiguration.DEFAULT_HADOOP_ADDRESS + KiteConfiguration.DEFAULT_PATH + KiteConfiguration.DEFAULT_NAMESPACE);
    }

    @Test
    public void addRecord() {
        new KiteBasicOperation().addRecord(KiteConfiguration.HDFS() + "firstdataset", 10000);
    }
}
