package com.lovecws.mumu.kite.partition;

import com.lovecws.mumu.kite.KiteConfiguration;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 本地文件测试
 * @date 2018-01-10 17:41:
 */
public class KiteFileDatasetOperationTest {

    @Test
    public void createFromCLass() {
        new KitePartitionOperation().createFromCLass(KiteConfiguration.LOCAL() + "basicdataset");
    }

    @Test
    public void createPartition() {
        new KitePartitionOperation().createPartition(KiteConfiguration.LOCAL() + "partitiondataset2");
    }

    @Test
    public void createDatePartition() {
        new KitePartitionOperation().createDatePartition(KiteConfiguration.LOCAL() + "datepartitiondataset");
    }
}
