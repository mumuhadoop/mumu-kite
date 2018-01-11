package com.lovecws.mumu.kite.util;

import com.lovecws.mumu.kite.entity.BaseUserEntity;
import org.apache.avro.Schema;
import org.apache.avro.file.DataFileReader;
import org.apache.avro.file.DataFileWriter;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericDatumReader;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.io.*;
import org.apache.avro.reflect.ReflectData;

import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.IOException;
import java.lang.reflect.Field;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 解析avro文件
 * @date 2018-01-11 09:21:
 */
public class AvroUtil {

    public void parseAvroFile(String filePath) {
        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>();
        DataFileReader<GenericRecord> dataFileReader = null;
        try {
            dataFileReader = new DataFileReader<GenericRecord>(new File(filePath), reader);
        } catch (IOException e) {
            e.printStackTrace();
        }
        System.out.println(dataFileReader.getSchema());
        while (dataFileReader.hasNext()) {
            GenericRecord genericRecord = dataFileReader.next();
            System.out.println(genericRecord);
        }
    }

    public void createAvroFile(String outputPath) {
        Schema schema = ReflectData.get().getSchema(BaseUserEntity.class);

        DatumWriter<GenericRecord> datumWriter = new GenericDatumWriter<GenericRecord>(schema);

        DataFileWriter<GenericRecord> dataFileWriter = new DataFileWriter<GenericRecord>(datumWriter);

        try {
            dataFileWriter = dataFileWriter.create(schema, new File(outputPath));
            for (int i = 1; i <= 10000; i++) {
                GenericRecord genericRecord = new GenericData.Record(schema);
                genericRecord.put("username", "cws" + i);
                genericRecord.put("userpassword", "cws" + i);
                genericRecord.put("age", i);
                genericRecord.put("sex", i % 2 == 0 ? "f" : "m");
                dataFileWriter.append(genericRecord);
            }
            dataFileWriter.close();
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    /**
     * 将对象序列化成字节数组
     *
     * @param object
     * @return
     */
    public byte[] serialize(Object object) {
        Schema schema = ReflectData.get().getSchema(object.getClass());
        GenericRecord record = new GenericData.Record(schema);

        Field[] declaredFields = object.getClass().getDeclaredFields();
        try {
            for (Field field : declaredFields) {
                field.setAccessible(true);
                record.put(field.getName(), field.get(object));
            }
            System.out.println(record);
        } catch (IllegalAccessException e) {
            e.printStackTrace();
        }


        ByteArrayOutputStream byteArrayOutputStream = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<GenericRecord>(schema);
        BinaryEncoder encoder = EncoderFactory.get().binaryEncoder(byteArrayOutputStream, null);
        try {
            writer.write(record, encoder);
            encoder.flush();
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                byteArrayOutputStream.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
        return byteArrayOutputStream.toByteArray();
    }

    /**
     * 反序列化 将字节数组反序列化成对象
     *
     * @param bs
     */
    public <T> T deserialize(byte[] bs, Class<T> clazz) {
        Schema schema = ReflectData.get().getSchema(BaseUserEntity.class);

        DatumReader<GenericRecord> reader = new GenericDatumReader<GenericRecord>(schema);
        BinaryDecoder decoder = DecoderFactory.get().binaryDecoder(bs, null);
        try {
            GenericRecord record = reader.read(null, decoder);
            System.out.println(record);

            T newInstance = clazz.newInstance();
            Field[] declaredFields = clazz.getDeclaredFields();
            for (Field field : declaredFields) {
                field.setAccessible(true);
                Object recordValue = record.get(field.getName());
                if (recordValue != null) {
                    Class fieldType = field.getType();
                    switch (fieldType.getTypeName()) {
                        case "java.lang.String":
                            field.set(newInstance, recordValue.toString());
                            break;
                        case "int":
                        case "java.lang.Integer":
                            field.set(newInstance, (int) recordValue);
                            break;
                    }
                }
            }
            return newInstance;
        } catch (IOException | IllegalAccessException | InstantiationException e) {
            e.printStackTrace();
        }
        return null;
    }
}
