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

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.UUID;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;
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
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author aervits
 */
public class ConsumerTest implements Runnable {

    private static final Logger LOG = Logger.getLogger(ConsumerTest.class.getName());
    private static final byte[] CF_DEFAULT = Bytes.toBytes("cf");
    private static final byte[] CF_FAMILY = Bytes.toBytes("message");
    private KafkaStream m_stream;
    private int m_threadNumber;
    private static KafkaToHBaseConfig config;

    ConsumerTest() {
        try {
            config = Util.getKafkaToHiveConfig("kafka.json");
        } catch (IOException ex) {
            Logger.getLogger(ConsumerTest.class.getName()).log(Level.SEVERE, null, ex);
        }
    }

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
    }

    public void run() {

        Configuration hbaseConfig = HBaseConfiguration.create();

        hbaseConfig.set("hbase.zookeeper.quorum", "jetmaster2.jetnetname.artem.com,jetslave5.jetnetname.artem.com,jetslave1.jetnetname.artem.com");
        hbaseConfig.set("hbase.zookeeper.property.clientPort", "2181");
        hbaseConfig.set("zookeeper.znode.parent", "/hbase-unsecure");

        long start = System.currentTimeMillis();
        List<String> messages = new ArrayList();

        int count = 0;
        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
            try {
                Object[] fields = createValues(new String(it.next().message()));
                messages.add((String) fields[0]);
                count++;

                if ((count % 10000) == 0) {
                    write(hbaseConfig, messages);
                    messages.clear();
                    LOG.info(String.format("Currently processed %d", count));
                }
            } catch (IOException | ClassNotFoundException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        }
        write(hbaseConfig, messages);

        LOG.info("Shutting down Thread: " + m_threadNumber);
        long end = System.currentTimeMillis();
        LOG.info("Time: " + (end - start) / 1000);
    }

    public Object[] createValues(String jsonIn) throws IOException, ClassNotFoundException {

        JsonNode rootNode = getJsonNode(jsonIn);
        Field[] fields = config.mappingConfig.getCombinedFields();
        Object[] values = new Object[fields.length];
        int index = 0;
        for (Field f : fields) {
            String o = getValue(rootNode, f);
            values[index] = o;
            index++;
        }
        return values;
    }

    private JsonNode getJsonNode(String jsonIn) throws IOException {
        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        return mapper.readTree(jsonIn);
    }

    private String getValue(JsonNode rootNode, Field f) throws ClassNotFoundException {
        JsonNode node = rootNode.get(f.name);
        if (node == null) {
            throw new RuntimeException(
                    f.name + " cannot be found in json " + rootNode
                    + " please revisit input json or configuration.");
        }
        if (node.isContainerNode()) {
            return node.toString();
        } else {
            return node.asText();
        }
    }

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
                
            for(String message : messages) {
                
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
