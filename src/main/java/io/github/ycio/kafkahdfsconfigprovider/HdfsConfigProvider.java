package io.github.ycio.kafkahdfsconfigprovider;

import org.apache.commons.io.FileUtils;
import org.apache.kafka.common.config.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.Charset;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HdfsConfigProvider implements org.apache.kafka.common.config.provider.ConfigProvider{
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsConfigProvider.class);

    @Override
    public ConfigData get(String path) {
        return get(path, Collections.emptySet());
    }

    @Override
    public ConfigData get(String path, Set<String> keys) {
        LOGGER.info("get() - path = '{}' keys = '{}'", path, keys);

        String content;
        try {
            content = FileUtils.readFileToString(new File(path), Charset.forName("UTF-8"));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        final Properties properties = new Properties();
        try {
            properties.load(new StringReader(content));
        } catch (IOException e) {
            throw new RuntimeException(e);
        }

        Map<String, String> data = new HashMap<>();
        for(String key : properties.stringPropertyNames()) {
            if (keys.contains(key)) {
                String value = properties.getProperty(key);
                data.put(key, value);
            }
        }

        return new ConfigData(data);
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
