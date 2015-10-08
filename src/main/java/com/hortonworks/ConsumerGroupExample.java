/*
 * Copyright 2015 aervits.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.hortonworks;

/**
 *
 * @author aervits
 *
 * https://cwiki.apache.org/confluence/display/KAFKA/Consumer+Group+Example
 */

import java.io.IOException;
import java.util.ArrayList;
import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.BufferedMutator;
import org.apache.hadoop.hbase.client.BufferedMutatorParams;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.ConnectionFactory;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.RetriesExhaustedWithDetailsException;
import org.apache.hadoop.hbase.util.Bytes;

public class ConsumerGroupExample {

    private final ConsumerConnector consumer;
    private final String topic;
    private ExecutorService executor;
    private static final byte[] CF_DEFAULT = Bytes.toBytes("cf");
    private static final byte[] CF_FAMILY = Bytes.toBytes("message");
    private static final Logger LOG = Logger.getLogger(ConsumerGroupExample.class.getName());
    private static Map map = new ConcurrentHashMap();
    
    public ConsumerGroupExample(String a_zookeeper, String a_groupId, String a_topic) {
        consumer = kafka.consumer.Consumer.createJavaConsumerConnector(
                createConsumerConfig(a_zookeeper, a_groupId));
        this.topic = a_topic;
    }

    public void shutdown() {
        if (consumer != null) {
            consumer.shutdown();
        }
        if (executor != null) {
            executor.shutdown();
        }
        try {
            if (!executor.awaitTermination(5000, TimeUnit.MILLISECONDS)) {
                System.out.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
            }
        } catch (InterruptedException e) {
            System.out.println("Interrupted during shutdown, exiting uncleanly");
        }
    }

    public void run(int a_numThreads) {
        Map<String, Integer> topicCountMap = new HashMap<>();
        topicCountMap.put(topic, a_numThreads);
        Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer.createMessageStreams(topicCountMap);
        List<KafkaStream<byte[], byte[]>> streams = consumerMap.get(topic);

        // now launch all the threads
        //
        executor = Executors.newFixedThreadPool(a_numThreads);

        // now create an object to consume the messages
        //
        int threadNumber = 0;
        for (final KafkaStream stream : streams) {
            executor.submit(new ConsumerTest(stream, threadNumber,map));
            threadNumber++;
        }
    }

    private static ConsumerConfig createConsumerConfig(String a_zookeeper, String a_groupId) {
        Properties props = new Properties();
        props.put("zookeeper.connect", a_zookeeper);
        props.put("group.id", a_groupId);
        props.put("zookeeper.session.timeout.ms", "400");
        props.put("zookeeper.sync.time.ms", "200");
        props.put("auto.commit.interval.ms", "1000");

        return new ConsumerConfig(props);
    }

    public static void main(String[] args) {
//        String zooKeeper = args[0];
//        String groupId = args[1];
//        String topic = args[2];
//        int threads = Integer.parseInt(args[3]);

        String zooKeeper = "jetslave5.jetnetname.artem.com:2181,jetmaster1.jetnetname.artem.com:2181,jetmaster2.jetnetname.artem.com:2181";
        String groupId = "StormSpoutConsumerGroup";
        String topic = "opensoc12";
        int threads = 1;

        ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper, groupId, topic);
        LOG.info("Starting Consumer");
        example.run(threads);

        Configuration hbaseConfig = HBaseConfiguration.create();

        hbaseConfig.set("hbase.zookeeper.quorum", "jetmaster2.jetnetname.artem.com,jetslave5.jetnetname.artem.com,jetslave1.jetnetname.artem.com");
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure");

        long start = System.currentTimeMillis();
        int count = 0;
        List<String> messages = new ArrayList<>();
        for(Object key : map.keySet()) {
            messages.add((String) key);
            
            if ((count % 10000) == 0) {
                write(hbaseConfig, messages);
                messages.clear();
                LOG.info(String.format("Currently processed %d", count));
            }
            count++;
        } 
        write(hbaseConfig, messages);

        try {
            Thread.sleep(60000);
        } catch (InterruptedException ie) {

        }
        
        long end = System.currentTimeMillis();
        LOG.info(String.format("Ellapsed: %d", (end - start)/1000));
        
        example.shutdown();
    }
    /**
     * @return the map
     */


    /**
     * @param aMap the map to set
     */
    public static void write(Configuration config, List<String> messages) {

        TableName tableName = TableName.valueOf("api");
        /**
         * a callback invoked when an asynchronous write fails.
         */
        final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
            @Override
            public void onException(RetriesExhaustedWithDetailsException e, BufferedMutator mutator) {
                for (int i = 0; i < e.getNumExceptions(); i++) {
                    LOG.info("Failed to send put " + e.getRow(i) + ".");
                }
            }
        };
        BufferedMutatorParams params = new BufferedMutatorParams(tableName)
                .listener(listener);
        try (Connection connection = ConnectionFactory.createConnection(config);
                final BufferedMutator mutator = connection.getBufferedMutator(params)) {
            List<Put> puts = new ArrayList<>(messages.size());
            Put put;

            for (String message : messages) {

                put = new Put(Bytes.toBytes(message));
                put.addColumn(CF_DEFAULT, CF_FAMILY, Bytes.toBytes(message));
                puts.add(put);

                mutator.mutate(puts);
            }
        } catch (IOException ex) {
            LOG.log(Level.SEVERE, ex.getMessage());
        }
    }

}
