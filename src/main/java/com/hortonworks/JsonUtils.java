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
import java.util.logging.Level;
import java.util.logging.Logger;
import org.codehaus.jackson.JsonFactory;
import org.codehaus.jackson.JsonNode;
import org.codehaus.jackson.map.ObjectMapper;

/**
 *
 * @author aervits
 */
public class JsonUtils {

    private static KafkaToHBaseConfig config;

    public JsonUtils() {
        try {
            // FOR TEST
            // config = Util.getKafkaToHiveConfig("./src/main/resources/kafka.json");
            
            config = Util.getKafkaToHiveConfig("./src/main/resources/kafka.json");
        } catch (IOException ex) {
            Logger.getLogger(ConsumerTest.class.getName()).log(Level.SEVERE, null, ex);
        }
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

    protected JsonNode getJsonNode(String jsonIn) throws IOException {
        JsonFactory factory = new JsonFactory();
        ObjectMapper mapper = new ObjectMapper(factory);
        return mapper.readTree(jsonIn);
    }

    protected String getValue(JsonNode rootNode, Field f) throws ClassNotFoundException {
        JsonNode node = rootNode.get(f.name);
        if (node == null) {
            throw new RuntimeException(f.name + " cannot be found in json " + rootNode + " please revisit input json or configuration.");
        }
        if (node.isContainerNode()) {
            return node.toString();
        } else {
            return node.asText();
        }
    }

}
