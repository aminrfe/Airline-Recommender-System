package ir.airline.parquetwriter.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "parquet-writer")
public class ParquetWriterConfig {

    private Kafka kafka = new Kafka();

    private Minio minio = new Minio();

    private Iceberg iceberg = new Iceberg();

    private Table table = new Table();


    @Data
    public static class Kafka {

        private String topic;
    }

    @Data
    public static class Minio {

        private String url;

        private String accessKey;

        private String secretKey;

        private String bucket;
    }

    @Data
    public static class Iceberg {

        private String catalogUrl;

        private String catalogUser;

        private String catalogPassword;

        private String warehousePath;
    }

    @Data
    public static class Table {

        private String namespace;

        private String name;

        private int batchSize;
    }
}
