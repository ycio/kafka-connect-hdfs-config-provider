package io.github.ycio.kafkahdfsconfigprovider;

import org.apache.commons.io.FileUtils;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HdfsConfigPropertiesLoader {
    Map<String, String> read(String path, Set<String> keys) {
        final Properties properties = loadProperties(path);

        Map<String, String> data = new HashMap<>();
        for(String key : properties.stringPropertyNames()) {
            if (keys.contains(key)) {
                String value = properties.getProperty(key);
                data.put(key, value);
            }
        }
        return data;
    }

    private Properties loadProperties(String path) {
        String content;
        try {
            content = FileUtils.readFileToString(new File(path), StandardCharsets.UTF_8);
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final Properties properties = new Properties();
        try {
            properties.load(new StringReader(content));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }
        return properties;
    }
}
