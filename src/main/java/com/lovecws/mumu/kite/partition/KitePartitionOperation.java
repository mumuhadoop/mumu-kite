package com.lovecws.mumu.kite.partition;

import com.lovecws.mumu.kite.entity.BaseUserEntity;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericRecordBuilder;
import org.apache.log4j.Logger;
import org.kitesdk.data.*;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: dataset分区
 * @date 2018-01-10 17:37:
 */
public class KitePartitionOperation {

    private static final Logger log = Logger.getLogger(KitePartitionOperation.class);

    public void createFromCLass(String dataset) {
        DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
                .schema(BaseUserEntity.class)
                .format(Formats.AVRO)
                .compressionType(CompressionType.Snappy)
                .build();
        View<GenericRecord> genericRecordView = Datasets.create(dataset, descriptor);
        log.info(genericRecordView);

        DatasetWriter<GenericRecord> datasetWriter = genericRecordView.getDataset().newWriter();
        for (int i = 1; i <= 100; i++) {
            GenericRecord genericRecord = new GenericData.Record(descriptor.getSchema());
            genericRecord.put("username", "lovercws" + i);
            genericRecord.put("userpassword", "123456");
            genericRecord.put("age", i);
            datasetWriter.write(genericRecord);
        }

        GenericRecordBuilder genericRecordBuilder = new GenericRecordBuilder(descriptor.getSchema());
        for (int i = 100; i <= 200; i++) {
            GenericData.Record genericRecord = genericRecordBuilder
                    .set("username", "lovercws" + i)
                    .set("userpassword", "123456")
                    .set("", i)
                    .build();
            datasetWriter.write(genericRecord);
        }
        datasetWriter.close();
    }

    public void createBaseUserEntity(String dataset) {
        DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
                .schema(BaseUserEntity.class)
                .format(Formats.AVRO)
                .build();
        View<BaseUserEntity> genericRecordView = Datasets.create(dataset, descriptor, BaseUserEntity.class);
        log.info(genericRecordView);

        DatasetWriter<BaseUserEntity> datasetWriter = genericRecordView.getDataset().newWriter();
        for (int i = 1; i <= 100; i++) {
            BaseUserEntity baseUserEntity = new BaseUserEntity();
            baseUserEntity.setUsername("lovecws" + i);
            baseUserEntity.setUserpassword("123456");
            baseUserEntity.setSex(i % 2 == 0 ? "f" : "m");
            baseUserEntity.setAge(i);
            datasetWriter.write(baseUserEntity);
        }
        datasetWriter.close();
    }

    public void createPartition(String dataset) {
        DatasetDescriptor descriptor = new DatasetDescriptor.Builder()
                .schema(BaseUserEntity.class)
                .format(Formats.AVRO)
                .partitionStrategy(new PartitionStrategy.Builder().identity("sex").build())
                .build();
        View<GenericRecord> genericRecordView = Datasets.create(dataset, descriptor);
        log.info(genericRecordView);

        DatasetWriter<GenericRecord> datasetWriter = genericRecordView.getDataset().newWriter();
        for (int i = 1; i <= 100000; i++) {
            GenericRecord genericRecord = new GenericData.Record(descriptor.getSchema());
            genericRecord.put("username", "lovercws" + i);
            genericRecord.put("userpassword", "123456");
            genericRecord.put("age", i);
            genericRecord.put("sex", i % 2 == 0 ? "f" : "m");
            datasetWriter.write(genericRecord);
        }
        datasetWriter.close();
    }
}
