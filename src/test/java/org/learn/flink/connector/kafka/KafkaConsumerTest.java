package org.learn.flink.connector.kafka;

import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.ListPartitionReassignmentsResult;
import org.apache.kafka.clients.admin.ListTopicsResult;
import org.apache.kafka.common.KafkaFuture;
import org.junit.Test;

import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutionException;

public class KafkaConsumerTest {

    @Test
    public void listTopicTest() throws ExecutionException, InterruptedException {
        Properties pro = new Properties();
        pro.put("bootstrap.servers",KafkaConf.servers);
        //KafkaUtils.getTopicNames(zkAddress)
        ListTopicsResult result = KafkaAdminClient.create(pro).listTopics();
        KafkaFuture<Set<String>> set = result.names();
        System.out.println(set.get());
        ListPartitionReassignmentsResult reassignmentsResult=KafkaAdminClient.create(pro).listPartitionReassignments();
        reassignmentsResult.reassignments().get();
    }
}
