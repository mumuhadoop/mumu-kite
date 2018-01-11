package com.lovecws.mumu.kite.util;

import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: parquet文件写入和读取
 * @date 2018-01-11 12:07:
 */
public class ParquetUtilTest {

    @Test
    public void write(){
        new ParquetUtil().write("e://mumu/kite/parquet/user.parquet");
    }

    @Test
    public void read(){
        new ParquetUtil().read("e://mumu/kite/parquet/user.parquet");
    }
}
