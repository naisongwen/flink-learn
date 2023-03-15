package org.learn.flink.connector.kafka;

import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.junit.Test;

import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class TestKafka {
    @Test
    public void testReadWriteKafka() throws ExecutionException, InterruptedException {
        Properties kafkaProps = new Properties();
        String kafkaServers = "10.201.0.216:9094";
        kafkaProps.put("bootstrap.servers", kafkaServers);
        kafkaProps.put("key.serializer", StringSerializer.class.getName());
        kafkaProps.put("value.serializer", StringSerializer.class.getName());
        KafkaProducer producer = new KafkaProducer<String, String>(kafkaProps);

        String value="{\"owner\":\"ICSYS\",\"columnNum\":\"24\",\"rowNum\":\"20262\",\"operationType\":\"i\",\"tableName\":\"CP_CHIP_DC_T\",\"scn\":0,\"opTs\":\"2022-04-18 11:01:41\",\"loaderTime\":\"2022-04-18 11:34:29\",\"trainid\":0,\"DBNAME\":\"EDATEST\",\"rowid\":\"AAAAAAAAAAAACcQAAB\",\"load_seq\":0,\"afterColumnList\":{\"WAFER_ID\":\"zzz\",\"STEP_ID\":\"xxx\",\"PARAM_ID\":\"xxx\",\"WAFER_START_TIME\":\"2020-02-07 16:31:08\",\"CHIP_ID\":\"0210\",\"PRODUCT_ID\":\"xxx\",\"PROGRAM_ID\":\"xxx\",\"CHIP_X\":\"01\",\"CHIP_Y\":\"10\",\"VALUE\":6.5,\"SPEC_HIGH\":0,\"SPEC_LOW\":-1,\"NUM_ITEM1\":105,\"NUM_ITEM2\":null,\"NUM_ITEM3\":null,\"UPDATE_TIME\":\"2020-03-25 10:39:34\",\"CHIP_Z\":null}}";
        ProducerRecord<String, String> record = new ProducerRecord<>("testTopic", "testKey",value );//Topic Key Value
        Future future = producer.send(record);
        future.get();
    }
}
