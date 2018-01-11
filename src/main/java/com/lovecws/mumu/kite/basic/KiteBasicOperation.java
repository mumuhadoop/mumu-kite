package com.lovecws.mumu.kite.basic;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericRecord;
import org.apache.log4j.Logger;
import org.kitesdk.data.*;

import java.net.URI;
import java.util.Collection;
import java.util.Iterator;
import java.util.List;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: kite基本操作
 * @date 2018-01-10 15:09:
 */
public class KiteBasicOperation {

    private static final Logger log = Logger.getLogger(KiteBasicOperation.class);

    public void create(String dataset, String format) {
        Schema.Parser parser = new Schema.Parser();
        Schema schema = parser.parse("{\n" +
                "    \"type\": \"record\",\n" +
                "    \"name\": \"StringPair\",\n" +
                "    \"doc\": \"A pair of strings.\",\n" +
                "    \"namespace\":\"com.lovecws.mumu.avro.entity\",\n" +
                "    \"fields\": [\n" +
                "        {\"name\": \"left\", \"type\": \"string\"},\n" +
                "        {\"name\": \"right\", \"type\": \"string\"}\n" +
                "    ]\n" +
                "}");

        Schema csvSchema = SchemaBuilder.record("Rating")
                .fields()
                .name("userId").type().intType().noDefault()
                .name("movieId").type().intType().noDefault()
                .name("rating").type().intType().noDefault()
                .name("timeInSeconds").type().intType().noDefault()
                .endRecord();

        //DatasetDescriptor descriptor = new DatasetDescriptor.Builder().schema(Hello.class).build();
        DatasetDescriptor datasetDescriptor = new DatasetDescriptor(schema, null, Formats.fromString(format), null, null, null);
        View<GenericRecord> genericRecordView = Datasets.create(dataset, datasetDescriptor);
        log.info(genericRecordView);

        DatasetWriter<GenericRecord> genericRecordDatasetWriter = genericRecordView.newWriter();
        for (int i = 1; i <= 10; i++) {
            GenericData.Record record = new GenericData.Record(genericRecordView.getSchema());
            record.put("left", "貂蝉" + i);
            record.put("right", "西施" + i);
            log.info(record);
            genericRecordDatasetWriter.write(record);
        }
        genericRecordDatasetWriter.close();
    }

    public void delete(String dataset) {
        boolean delete = Datasets.delete(dataset);
        log.info(delete);
    }

    public void update(String dataset) {
        View<GenericRecord> genericRecordView = Datasets.load(dataset);
        DatasetDescriptor descriptor = genericRecordView.getDataset().getDescriptor();
        Schema schema = descriptor.getSchema();

        List<Schema.Field> fields = schema.getFields();
        //fields.add(new Schema.Field("inner", schema, "inner doc", new TextNode("inner")));

        Dataset<GenericRecord> update = Datasets.update(dataset, descriptor);
        log.info(update);
    }

    public void load(String dataset) {
        View<GenericRecord> genericRecordView = Datasets.load(dataset);
        log.info(genericRecordView);
        DatasetReader<GenericRecord> genericRecords = genericRecordView.newReader();
        for (Iterator<GenericRecord> iterator = genericRecords.iterator(); iterator.hasNext(); ) {
            GenericRecord genericRecord = iterator.next();
            log.info(genericRecord);
        }
    }

    public void view(String dataset) {
        View<GenericRecord> genericRecordView = Datasets.load(dataset);
        Dataset<GenericRecord> genericRecordViewDataset = genericRecordView.getDataset();

        log.info("with 貂蝉10000000");
        RefinableView<GenericRecord> genericRecordRefinableView = genericRecordViewDataset.with("left", "貂蝉10000000");
        for (Iterator<GenericRecord> iterator = genericRecordRefinableView.newReader().iterator(); iterator.hasNext(); ) {
            GenericRecord genericRecord = iterator.next();
            log.info(genericRecord);
        }

        System.out.println();
        log.info("from 貂蝉10000000 to 貂蝉10000010");
        genericRecordRefinableView = genericRecordViewDataset.from("left", "貂蝉10000000")
                .to("left", "貂蝉100000000");
        for (Iterator<GenericRecord> iterator = genericRecordRefinableView.newReader().iterator(); iterator.hasNext(); ) {
            GenericRecord genericRecord = iterator.next();
            log.info(genericRecord);
        }
    }

    public void exists(String dataset) {
        boolean exists = Datasets.exists(dataset);
        log.info(exists);
    }

    public void list(String dataset) {
        Collection<URI> collection = Datasets.list(dataset);
        for (Iterator<URI> iterator = collection.iterator(); iterator.hasNext(); ) {
            log.info(iterator.next());
        }
    }

    public void addRecord(String dataset, int count) {
        View<GenericRecord> genericRecordView = Datasets.load(dataset);

        DatasetWriter<GenericRecord> genericRecordDatasetWriter = genericRecordView.newWriter();
        for (int i = 1; i <= count; i++) {
            GenericData.Record record = new GenericData.Record(genericRecordView.getSchema());
            record.put("left", "貂蝉" + i);
            record.put("right", "西施" + i);
            log.info(record);
            genericRecordDatasetWriter.write(record);
        }
        genericRecordDatasetWriter.close();
    }
}
