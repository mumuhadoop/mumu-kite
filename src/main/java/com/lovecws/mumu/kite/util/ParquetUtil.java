package com.lovecws.mumu.kite.util;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import parquet.column.ParquetProperties;
import parquet.example.data.Group;
import parquet.example.data.GroupFactory;
import parquet.example.data.simple.SimpleGroupFactory;
import parquet.hadoop.ParquetReader;
import parquet.hadoop.ParquetWriter;
import parquet.hadoop.example.GroupReadSupport;
import parquet.hadoop.example.GroupWriteSupport;
import parquet.schema.MessageType;
import parquet.schema.MessageTypeParser;

import java.io.IOException;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: parquet工具类
 * @date 2018-01-11 12:01:
 */
public class ParquetUtil {

    private static final MessageType schema = MessageTypeParser.parseMessageType(
            "message Pair {\n" +
                    " required binary lover (UTF8);\n" +
                    " required binary sex (UTF8);\n" +
                    "}");

    /**
     * 写入parquet数据
     *
     * @param parquetFile parquet文件路径
     */
    public void write(String parquetFile) {
        Configuration conf = new Configuration();
        Path path = new Path(parquetFile);
        GroupWriteSupport writeSupport = new GroupWriteSupport();
        GroupWriteSupport.setSchema(schema, conf);
        ParquetWriter<Group> writer = null;
        try {
            writer = new ParquetWriter<Group>(path, writeSupport,
                    ParquetWriter.DEFAULT_COMPRESSION_CODEC_NAME,
                    ParquetWriter.DEFAULT_BLOCK_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    ParquetWriter.DEFAULT_PAGE_SIZE,
                    ParquetWriter.DEFAULT_IS_DICTIONARY_ENABLED,
                    ParquetWriter.DEFAULT_IS_VALIDATING_ENABLED,
                    ParquetProperties.WriterVersion.PARQUET_1_0, conf);
            for (int i = 1; i < 10000; i++) {
                GroupFactory groupFactory = new SimpleGroupFactory(schema);
                Group group = groupFactory.newGroup()
                        .append("lover", "cws" + i)
                        .append("sex", i % 2 == 0 ? "f" : "m");
                writer.write(group);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                writer.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }

    /**
     * 读取parquet数据
     *
     * @param parquetFile parquet文件路径
     */
    public void read(String parquetFile) {
        GroupReadSupport readSupport = new GroupReadSupport();
        ParquetReader<Group> reader = null;
        try {
            reader = new ParquetReader<Group>(new Path(parquetFile), readSupport);

            Group group = null;
            while ((group = reader.read()) != null) {
                System.out.println(group);
            }
        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                reader.close();
            } catch (IOException e) {
                e.printStackTrace();
            }
        }
    }
}
