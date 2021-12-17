package io.github.ycio.kafkahdfsconfigprovider;

import org.apache.kafka.common.config.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class HdfsConfigProvider implements org.apache.kafka.common.config.provider.ConfigProvider{
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsConfigProvider.class);
    String pathPrefix = "";

    @Override
    public ConfigData get(String path) {
        return get(getPath(path), Collections.emptySet());
    }

    private String getPath(String path) {
        return Paths.get(pathPrefix, path).toString();
    }

    @Override
    public ConfigData get(String path, Set<String> keys) {
        return new ConfigData(new HdfsConfigPropertiesLoader().read(getPath(path), keys));
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey("path.prefix")) {
            pathPrefix = configs.get("path.prefix").toString();
        }
    }
}
