package ir.airline.sparkrecommender.config;

import lombok.Data;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Data
@Configuration
@ConfigurationProperties(prefix = "spark-recommender")
public class SparkRecommenderProperties {

    private MinioProperties minioProperties = new MinioProperties();
    private IcebergProperties icebergProperties = new IcebergProperties();
    private TableProperties tableProperties = new TableProperties();
    private PostgresProperties postgresProperties = new PostgresProperties();

    @Data
    public static class MinioProperties {
        private String url;
        private String accessKey;
        private String secretKey;
        private String bucket;
    }

    @Data
    public static class IcebergProperties {
        private String catalogUrl;
        private String catalogUser;
        private String catalogPassword;
        private String warehousePath;
    }

    @Data
    public static class TableProperties {
        private String catalogName;
        private String databaseName;
        private String tableName;
    }

    @Data
    public static class PostgresProperties {
        private String url;
        private String user;
        private String password;
        private String driver = "org.postgresql.Driver";
    }

    public String getFullSourceTableName() {
        return String.format("%s.%s.%s",
                tableProperties.getCatalogName(),
                tableProperties.getDatabaseName(),
                tableProperties.getTableName());
    }
}
