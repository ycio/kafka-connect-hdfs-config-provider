package io.github.ycio.kafkahdfsconfigprovider;

import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.DistributedFileSystem;
import org.apache.hadoop.security.UserGroupInformation;

import java.io.IOException;
import java.io.StringReader;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;
import java.util.Properties;
import java.util.Set;

public class HdfsConfigPropertiesLoader {
    private String hdfsUrl;
    private String namenodePrincipal;
    private String hdfsPrincipal;
    private String hdfsKeytab;

    public HdfsConfigPropertiesLoader(String hdfsUrl, String namenodePrincipal, String hdfsPrincipal, String hdfsKeytab) {
        this.hdfsUrl = hdfsUrl;
        this.namenodePrincipal = namenodePrincipal;
        this.hdfsPrincipal = hdfsPrincipal;
        this.hdfsKeytab = hdfsKeytab;
    }

    Map<String, String> read(String path, Set<String> keys) {
        final Properties properties;
        try {
            properties = loadProperties(path);
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
        return data;
    }

    private Properties loadProperties(String path) throws IOException {
        Configuration conf = new Configuration();
        conf.set("hadoop.security.authentication", "kerberos");
        conf.set("hadoop.security.authorization", "true");
        conf.set("fs.defaultFS", hdfsUrl);
        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
        conf.set("dfs.namenode.kerberos.principal.pattern", namenodePrincipal);
        UserGroupInformation.setConfiguration(conf);
        UserGroupInformation.loginUserFromKeytab(hdfsPrincipal, hdfsKeytab);

        FileSystem fs = FileSystem.get(conf);
        FSDataInputStream dataInputStream = fs.open(new Path(path));
        String content = IOUtils.toString(dataInputStream, StandardCharsets.UTF_8);

        final Properties properties = new Properties();
        properties.load(new StringReader(content));
        return properties;
    }
}
