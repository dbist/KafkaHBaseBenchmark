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
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.Set;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.TimeUnit;
import java.util.logging.Level;
import java.util.logging.Logger;

import kafka.consumer.ConsumerConfig;
import kafka.consumer.KafkaStream;
import kafka.javaapi.consumer.ConsumerConnector;

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

	private  int total = 0;

	public synchronized int getTotal() {
		return total;
	}

	public synchronized void setTotal(int total) {
		this.total = total;
	}
	public synchronized void updateTotal(int newNumber) {
		this.total = total +newNumber;
	}

	private final ConsumerConnector consumer;
	private final String topic;
	private ExecutorService executor;
	private static final byte[] CF_DEFAULT = Bytes.toBytes("cf");
	private static final byte[] CF_FAMILY = Bytes.toBytes("message");
	private static final Logger LOG = Logger
			.getLogger(ConsumerGroupExample.class.getName());
	protected LinkedBlockingQueue<Object[]> queue = new LinkedBlockingQueue<>();

	private ExecutorService executorConsumer;

	public ConsumerGroupExample(String a_zookeeper, String a_groupId,
			String a_topic) {
		consumer = kafka.consumer.Consumer
				.createJavaConsumerConnector(createConsumerConfig(a_zookeeper,
						a_groupId));
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
				System.out
						.println("Timed out waiting for consumer threads to shut down, exiting uncleanly");
			}
		} catch (InterruptedException e) {
			System.out
					.println("Interrupted during shutdown, exiting uncleanly");
		}
	}

	public void run(int a_numThreads) {
		Map<String, Integer> topicCountMap = new HashMap<>();
		topicCountMap.put(topic, a_numThreads);
		Map<String, List<KafkaStream<byte[], byte[]>>> consumerMap = consumer
				.createMessageStreams(topicCountMap);
		System.out.println("consumer map size=" + consumerMap.size());
		Collection<List<KafkaStream<byte[], byte[]>>> values = consumerMap
				.values();

		final Configuration hbaseConfig = HBaseConfiguration.create();

		hbaseConfig
				.set("hbase.zookeeper.quorum",
						"jetmaster2.jetnetname.artem.com,jetslave5.jetnetname.artem.com,jetslave1.jetnetname.artem.com");
		hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
		hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure");

		final long start = System.currentTimeMillis();
		for (List<KafkaStream<byte[], byte[]>> streams : values) {
			// List<KafkaStream<byte[], byte[]>> streams =
			// consumerMap.get(topic);
			System.out.println("streams size====" + streams.size());

			// now launch all the threads
			executor = Executors.newFixedThreadPool(a_numThreads);

			executorConsumer = Executors.newFixedThreadPool(a_numThreads);

			// now create an object to consume the messages
			//
			int threadNumber = 0;
			for (final KafkaStream stream : streams) {

				// System.out.println("submiting stream:" + stream.clientId() +
				// " threadId= " + threadNumber + " , stream size" +
				// stream.size());
				executor.submit(new ConsumerTest(stream, threadNumber, queue,
						new Notifier() {
							public synchronized void  flushMsgs() {
								final List<Object[]> messages = new ArrayList<>();
								queue.drainTo(messages);

								executorConsumer.submit(new Runnable(){

									@Override
									public void run() {
										write(hbaseConfig, messages);
										long end = System.currentTimeMillis();
										end = System.currentTimeMillis();
										updateTotal( messages.size());

										System.out.println(String.format("Flushing end: %d total= %d" , (end - start) / 1000, getTotal()) );
									}
									
								});

							}
							@Override
							public void shutdown() {
								long end = System.currentTimeMillis();
								System.out.println(String.format("SHUTODOWN end: %d total= %d" , (end - start) / 1000, total) );
								
							}
						}));
				threadNumber++;
			}

		}
		Set<String> keySet = consumerMap.keySet();
		for (String key : keySet) {
			System.out.println("key=" + key);
		}

	}

	private static ConsumerConfig createConsumerConfig(String a_zookeeper,
			String a_groupId) {
		Properties props = new Properties();
		props.put("zookeeper.connect", a_zookeeper);
		props.put("group.id", a_groupId);
		props.put("zookeeper.session.timeout.ms", "400");
		props.put("zookeeper.sync.time.ms", "200");
		props.put("auto.commit.interval.ms", "1000");

		return new ConsumerConfig(props);
	}

	public static void main(String[] args) {
		// String zooKeeper = args[0];
		// String groupId = args[1];
		String zooKeeper = "jetslave5.jetnetname.artem.com:2181,jetmaster1.jetnetname.artem.com:2181,jetmaster2.jetnetname.artem.com:2181";
		String groupId = "StormSpoutConsumerGroup";
		String topic = args[0];
		int threads = Integer.parseInt(args[1]);

		// int threads = 2;

		ConsumerGroupExample example = new ConsumerGroupExample(zooKeeper,
				groupId, topic);
		LOG.info("Starting Consumer");
		example.run(threads);

//		example.shutdown();
	}

	/**
	 * @return the map
	 */

	/**
	 * @param aMap
	 *            the map to set
	 */
	public static void write(Configuration config, List<Object[]> messages) {

		TableName tableName = TableName.valueOf("api");
		/**
		 * a callback invoked when an asynchronous write fails.
		 */
		final BufferedMutator.ExceptionListener listener = new BufferedMutator.ExceptionListener() {
			@Override
			public void onException(RetriesExhaustedWithDetailsException e,
					BufferedMutator mutator) {
				for (int i = 0; i < e.getNumExceptions(); i++) {
					LOG.info("Failed to send put " + e.getRow(i) + ".");
				}
			}
		};
		BufferedMutatorParams params = new BufferedMutatorParams(tableName)
				.listener(listener);
		try (Connection connection = ConnectionFactory.createConnection(config);
				final BufferedMutator mutator = connection
						.getBufferedMutator(params)) {
			List<Put> puts = new ArrayList<>(messages.size());
			Put put;
			int count=0;
			for (Object[] message : messages) {
				
				String clicks_key = (String) message[0];
				put = new Put(Bytes.toBytes(clicks_key));
				put.addColumn(CF_DEFAULT, CF_FAMILY, Bytes.toBytes(clicks_key));
				puts.add(put);
				count++;
			}
			mutator.mutate(puts);

		} catch (IOException ex) {
			LOG.log(Level.SEVERE, ex.getMessage());
		}
	}

}
