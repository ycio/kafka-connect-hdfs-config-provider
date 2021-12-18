package io.github.ycio.kafkahdfsconfigprovider;

import org.apache.kafka.common.config.ConfigData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.nio.file.Paths;
import java.util.Collections;
import java.util.Map;
import java.util.Set;

public class HdfsConfigProvider implements org.apache.kafka.common.config.provider.ConfigProvider{
    private static final Logger LOGGER = LoggerFactory.getLogger(HdfsConfigProvider.class);
    String pathPrefix = "";
    String hdfsPrincipal;
    String hdfsKeytab;
    String namenodePrincipal;
    String hdfsUrl;

    @Override
    public ConfigData get(String path) {
        return get(getPath(path), Collections.emptySet());
    }

    private String getPath(String path) {
        return Paths.get(pathPrefix, path).toString();
    }

    @Override
    public ConfigData get(String path, Set<String> keys) {
        return new ConfigData(new HdfsConfigPropertiesLoader(
                hdfsUrl,
                namenodePrincipal,
                hdfsPrincipal,
                hdfsKeytab
        ).read(getPath(path), keys));
    }

    @Override
    public void close() {
    }

    @Override
    public void configure(Map<String, ?> configs) {
        if (configs.containsKey("path.prefix")) {
            pathPrefix = configs.get("path.prefix").toString();
        }

        hdfsUrl = configs.get("hdfs.url").toString();
        hdfsPrincipal = configs.get("hdfs.principal").toString();
        hdfsKeytab = configs.get("hdfs.keytab").toString();
        namenodePrincipal = configs.get("namenode.principal").toString();

        LOGGER.info("path.prefix=%s hdfs.url=%s hdfs.principal=%s hdfs.keytab=%s namenode.principal=%s",
                pathPrefix, hdfsUrl, hdfsPrincipal, hdfsKeytab, namenodePrincipal
        );
    }
}
