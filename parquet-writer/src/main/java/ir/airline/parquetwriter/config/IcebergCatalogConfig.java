package ir.airline.parquetwriter.config;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.iceberg.CatalogUtil;
import org.apache.iceberg.catalog.Catalog;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

@Configuration
@RequiredArgsConstructor
public class IcebergCatalogConfig {

    private final ParquetWriterConfig parquetWriterConfig;
    private final org.apache.hadoop.conf.Configuration hadoopConfiguration;

    @Bean
    public Catalog icebergCatalog() {
        Map<String, String> properties = Map.of(
                "uri", parquetWriterConfig.getIceberg().getCatalogUrl(),
                "jdbc.user", parquetWriterConfig.getIceberg().getCatalogUser(),
                "jdbc.password", parquetWriterConfig.getIceberg().getCatalogPassword(),
                "warehouse", parquetWriterConfig.getIceberg().getWarehousePath()
        );

        return CatalogUtil.loadCatalog(
                "org.apache.iceberg.jdbc.JdbcCatalog",
                "postgres_catalog",
                properties,
                hadoopConfiguration
        );
    }
}
