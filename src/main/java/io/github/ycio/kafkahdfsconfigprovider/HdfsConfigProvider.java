package io.github.ycio.kafkahdfsconfigprovider;

import com.google.common.collect.ImmutableMap;
import org.apache.kafka.common.config.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Collections;
import java.util.Map;
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
        ImmutableMap<String, String> map = ImmutableMap.of(
                "theName", "theValue",
                "theName2", "theValue2"
        );
        return new ConfigData(map);
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {

    }
}
