package ir.airline.parquetwriter.config;

import lombok.RequiredArgsConstructor;
import org.apache.hadoop.conf.Configuration;
import org.springframework.context.annotation.Bean;

@org.springframework.context.annotation.Configuration
@RequiredArgsConstructor
public class MinioConfig {

    private final ParquetWriterConfig parquetWriterConfig;

    @Bean
    public Configuration hadoopConfiguration() {
        Configuration conf = new Configuration();
        conf.set("fs.s3a.endpoint", parquetWriterConfig.getMinio().getUrl());
        conf.set("fs.s3a.access.key", parquetWriterConfig.getMinio().getAccessKey());
        conf.set("fs.s3a.secret.key", parquetWriterConfig.getMinio().getSecretKey());
        conf.set("fs.s3a.path.style.access", "true");
        conf.set("fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem");
        conf.set("fs.s3a.connection.ssl.enabled", "false");
        return conf;
    }
}
