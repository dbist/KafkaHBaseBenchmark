package com.hortonworks;

import java.io.File;

import java.io.IOException;
import java.util.List;
import java.util.logging.Logger;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang.StringUtils;

/**
 * Created by christoph on 8/26/15.
 */
public class Util {

    private static Logger LOG = Logger.getLogger(Util.class.getName());

    public static KafkaToHBaseConfig getKafkaToHiveConfig(String fileName) throws IOException {
        
        List<String> configCP = FileUtils.readLines(new File(fileName));
        
       // List<String> configCP = IOUtils.readLines(ClassLoader.getSystemResourceAsStream(fileName));
        String configString = StringUtils.join(configCP, "\n");
        LOG.info("using config '" + configString + "'");

        return KafkaToHBaseConfig.parse(configString);
    }
}

