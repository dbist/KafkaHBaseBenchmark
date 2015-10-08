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
import java.util.Map;
import java.util.logging.Level;
import java.util.logging.Logger;
import kafka.consumer.ConsumerIterator;
import kafka.consumer.KafkaStream;

/**
 *
 * @author aervits
 */
public class ConsumerTest extends JsonUtils implements Runnable {

    private static final Logger LOG = Logger.getLogger(ConsumerTest.class.getName());


    private KafkaStream m_stream;
    private int m_threadNumber;
    private Map _map;

    ConsumerTest() {
    }

    public ConsumerTest(KafkaStream a_stream, int a_threadNumber, Map map) {
        m_threadNumber = a_threadNumber;
        m_stream = a_stream;
        _map = map;
    }

    @Override
    public void run() {

        ConsumerIterator<byte[], byte[]> it = m_stream.iterator();
        while (it.hasNext()) {
            
            try {
                Object[] fields = createValues(new String(it.next().message()));
                _map.put(fields[0], fields);
            } catch (IOException | ClassNotFoundException ex) {
                LOG.log(Level.SEVERE, null, ex);
            }
        }
    }

}
