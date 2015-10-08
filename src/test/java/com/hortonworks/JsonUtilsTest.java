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

import org.codehaus.jackson.JsonNode;
import org.junit.After;
import org.junit.AfterClass;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;
import static org.junit.Assert.*;
import org.junit.Ignore;

/**
 *
 * @author aervits
 */
public class JsonUtilsTest {
    
    public JsonUtilsTest() {
    }
    
    @BeforeClass
    public static void setUpClass() {
    }
    
    @AfterClass
    public static void tearDownClass() {
    }
    
    @Before
    public void setUp() {
    }
    
    @After
    public void tearDown() {
    }

    /**
     * Test of createValues method, of class JsonUtils.
     */
    @Test
    public void testCreateValuesClick_key() throws Exception {
        System.out.println("createValues");
        String jsonIn = "{\n" +
"  \"click_key\": \"1a4817d8-7412-47f0-88a6-3596e9a4e649\",\n" +
"  \"client_id\": \"-5534114214170524001\",\n" +
"  \"ordinal\": \"9\",\n" +
"  \"datetime\": \"Wed Oct 07 18:05:54 UTC 2015\",\n" +
"  \"user_agent\": \"mozilla\",\n" +
"  \"url\": \"http://jet.com/seach\",\n" +
"  \"params\": \"lskdjf,sdlfkjsldf\",\n" +
"  \"tags\": \"lskdjf,sdlfkjsldf\",\n" +
"  \"dec\": \"0.015939381932791097\",\n" +
"  \"json_example\":  {\n" +
"    \"color\": \"Blue\",\n" +
"    \"sport\": \"Soccer\",\n" +
"    \"food\": \"Spaghetti\"\n" +
"  } ,\n" +
"  \"list_example\": \"list\",\n" +
"  \"map_example\": \"map\",\n" +
"  \"struct_example\": \"struct\",\n" +
"  \"source_message\": \"lskdjf,sdlfkjsldf\",\n" +
"  \"partition_save_date\": \"2015-10-07\"\n" +
"}";
        JsonUtils instance = new JsonUtils();
        String expResult = "1a4817d8-7412-47f0-88a6-3596e9a4e649";
        Object[] result = instance.createValues(jsonIn);
        assertEquals(expResult, result[0]);
    }
    
    /**
     * Test of createValues method, of class JsonUtils.
     */
    @Test
    public void testCreateValuesClick_ID() throws Exception {
        System.out.println("createValues");
        String jsonIn = "{\n" +
"  \"click_key\": \"1a4817d8-7412-47f0-88a6-3596e9a4e649\",\n" +
"  \"client_id\": \"-5534114214170524001\",\n" +
"  \"ordinal\": \"9\",\n" +
"  \"datetime\": \"Wed Oct 07 18:05:54 UTC 2015\",\n" +
"  \"user_agent\": \"mozilla\",\n" +
"  \"url\": \"http://jet.com/seach\",\n" +
"  \"params\": \"lskdjf,sdlfkjsldf\",\n" +
"  \"tags\": \"lskdjf,sdlfkjsldf\",\n" +
"  \"dec\": \"0.015939381932791097\",\n" +
"  \"json_example\":  {\n" +
"    \"color\": \"Blue\",\n" +
"    \"sport\": \"Soccer\",\n" +
"    \"food\": \"Spaghetti\"\n" +
"  } ,\n" +
"  \"list_example\": \"list\",\n" +
"  \"map_example\": \"map\",\n" +
"  \"struct_example\": \"struct\",\n" +
"  \"source_message\": \"lskdjf,sdlfkjsldf\",\n" +
"  \"partition_save_date\": \"2015-10-07\"\n" +
"}";
        JsonUtils instance = new JsonUtils();
        String expResult = "-5534114214170524001";
        Object[] result = instance.createValues(jsonIn);
        assertEquals(expResult, result[1]);
    }

    /**
     * Test of getJsonNode method, of class JsonUtils.
     */
    @Ignore
    public void testGetJsonNode() throws Exception {
        System.out.println("getJsonNode");
        String jsonIn = "";
        JsonUtils instance = new JsonUtils();
        JsonNode expResult = null;
        JsonNode result = instance.getJsonNode(jsonIn);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }

    /**
     * Test of getValue method, of class JsonUtils.
     */
    @Ignore
    public void testGetValue() throws Exception {
        System.out.println("getValue");
        JsonNode rootNode = null;
        Field f = null;
        JsonUtils instance = new JsonUtils();
        String expResult = "";
        String result = instance.getValue(rootNode, f);
        assertEquals(expResult, result);
        // TODO review the generated test code and remove the default call to fail.
        fail("The test case is a prototype.");
    }
    
}
