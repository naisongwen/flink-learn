package org.learn.flink.schema;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.apache.kafka.common.TopicPartition;
import org.junit.Test;
import org.learn.flink.connector.kafka.KafkaConf;

import java.util.Collections;
import java.util.List;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.Future;

public class KafkaSchemTest {
    @Test
    public void listTopicTest() throws ExecutionException, InterruptedException {
        Properties pro = new Properties();
        pro.put("bootstrap.servers", KafkaConf.servers);
        //KafkaUtils.getTopicNames(zkAddress)
        ListTopicsResult result = KafkaAdminClient.create(pro).listTopics();
        KafkaFuture<Set<String>> set = result.names();
        System.out.println(set.get());
        ListPartitionReassignmentsResult reassignmentsResult=KafkaAdminClient.create(pro).listPartitionReassignments();
        reassignmentsResult.reassignments().get();
    }

    @Test
    public void testWriteKafka() throws ExecutionException, InterruptedException {

        KafkaProducer producer = new KafkaProducer<String, String>(KafkaConf.buildKafkaProperties(false));

        String value = "{\"owner\":\"ICSYS\",\"columnNum\":\"24\",\"rowNum\":\"20262\",\"operationType\":\"i\",\"tableName\":\"CP_CHIP_DC_T\",\"scn\":0,\"opTs\":\"2022-04-18 11:01:41\",\"loaderTime\":\"2022-04-18 11:34:29\",\"trainid\":0,\"DBNAME\":\"EDATEST\",\"rowid\":\"AAAAAAAAAAAACcQAAB\",\"load_seq\":0,\"afterColumnList\":{\"WAFER_ID\":\"zzz\",\"STEP_ID\":\"xxx\",\"PARAM_ID\":\"xxx\",\"WAFER_START_TIME\":\"2020-02-07 16:31:08\",\"CHIP_ID\":\"0210\",\"PRODUCT_ID\":\"xxx\",\"PROGRAM_ID\":\"xxx\",\"CHIP_X\":\"01\",\"CHIP_Y\":\"10\",\"VALUE\":6.5,\"SPEC_HIGH\":0,\"SPEC_LOW\":-1,\"NUM_ITEM1\":105,\"NUM_ITEM2\":null,\"NUM_ITEM3\":null,\"UPDATE_TIME\":\"2020-03-25 10:39:34\",\"CHIP_Z\":null}}";
        ProducerRecord<String, String> record = new ProducerRecord<>("testTopic", "testKey", value);//Topic Key Value
        Future future = producer.send(record);
        future.get();
    }

    @Test
    public void testReadKafka() {
        KafkaConsumer<?, ?> consumer = new KafkaConsumer(KafkaConf.buildKafkaProperties(false));
        consumer.subscribe(Collections.singletonList("user_events_example1"));
        ConsumerRecords records = consumer.poll(5000);
        List<ConsumerRecord<String, String>> recordList = records.records(new TopicPartition("user_events_example1", 0));
        recordList.forEach(e -> System.out.println(e)
        );
    }
}
