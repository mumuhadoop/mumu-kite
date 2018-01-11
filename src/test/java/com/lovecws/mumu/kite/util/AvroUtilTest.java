package com.lovecws.mumu.kite.util;

import com.lovecws.mumu.kite.entity.BaseUserEntity;
import org.junit.Test;

/**
 * @author babymm
 * @version 1.0-SNAPSHOT
 * @Description: 测试读取avro文件
 * @date 2018-01-11 09:34:
 */
public class AvroUtilTest {

    @Test
    public void parseAvroFile() {
        new AvroUtil().parseAvroFile("E:\\mumu\\kite\\kitenamespace\\partitiondataset\\sex_copy=f\\6df33790-f2f5-4d23-80fa-d1232a0d0621.avro");
    }

    @Test
    public void createAvroFile() {
        new AvroUtil().createAvroFile("E:\\mumu\\kite\\kitenamespace\\partitiondataset\\user2.avro");
    }

    @Test
    public void serialize() {
        BaseUserEntity userEntity = new BaseUserEntity();
        userEntity.setAge(12);
        userEntity.setSex("m");
        userEntity.setUsername("ganliang");
        userEntity.setUserpassword("123456");
        byte[] serialize = new AvroUtil().serialize(userEntity);
        System.out.println(serialize);
    }

    @Test
    public void deserialize() {
        BaseUserEntity userEntity = new BaseUserEntity();
        userEntity.setAge(12);
        userEntity.setSex("m");
        userEntity.setUsername("ganliang");
        userEntity.setUserpassword("123456");
        byte[] serialize = new AvroUtil().serialize(userEntity);
        System.out.println(serialize);

        BaseUserEntity baseUserEntity = new AvroUtil().deserialize(serialize, BaseUserEntity.class);
        System.out.println(baseUserEntity);
    }
}
