package ir.airline.recommenderjob.config;

import lombok.RequiredArgsConstructor;
import org.apache.spark.sql.SparkSession;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class SparkConfig {

    private final SparkRecommenderProperties properties;

    @Bean
    public SparkSession sparkSession() {
        String catalogName = properties.getTableProperties().getCatalogName();
        String catalogConfigPrefix = "spark.sql.catalog." + catalogName;

        return SparkSession.builder()
                .appName("AirlineRecommenderJob")
                .master("local[*]")
                .config("spark.ui.enabled", "false")
                .config("spark.sql.extensions", "org.apache.iceberg.spark.extensions.IcebergSparkSessionExtensions")

                .config(catalogConfigPrefix, "org.apache.iceberg.spark.SparkCatalog")
                .config(catalogConfigPrefix + ".type", "jdbc")
                .config(catalogConfigPrefix + ".uri", properties.getIcebergProperties().getCatalogUrl())
                .config(catalogConfigPrefix + ".jdbc.user", properties.getIcebergProperties().getCatalogUser())
                .config(catalogConfigPrefix + ".jdbc.password", properties.getIcebergProperties().getCatalogPassword())
                .config(catalogConfigPrefix + ".warehouse", properties.getIcebergProperties().getWarehousePath())

                .config("spark.hadoop.fs.s3a.endpoint", properties.getMinioProperties().getUrl())
                .config("spark.hadoop.fs.s3a.access.key", properties.getMinioProperties().getAccessKey())
                .config("spark.hadoop.fs.s3a.secret.key", properties.getMinioProperties().getSecretKey())
                .config("spark.hadoop.fs.s3a.path.style.access", "true")
                .config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")
                .config("spark.hadoop.fs.s3a.connection.ssl.enabled", "false")

                .config("spark.sql.warehouse.dir", properties.getIcebergProperties().getWarehousePath())

                .getOrCreate();
    }
}