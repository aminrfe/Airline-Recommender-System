package ir.airline.parquetwriter.util;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.SneakyThrows;
import org.apache.avro.Schema;
import org.apache.iceberg.avro.AvroSchemaUtil;

@NoArgsConstructor(access = AccessLevel.PACKAGE)
public class AirlineEventSchema {

    public static final Schema AVRO_SCHEMA = loadSchema();
    public static final org.apache.iceberg.Schema ICEBERG_SCHEMA = AvroSchemaUtil.toIceberg(AVRO_SCHEMA);

    private static final String AVRO_SCHEMA_PATH = "avro/airline-schema.avsc";

    @SneakyThrows
    private static Schema loadSchema() {
        try (var in = AirlineEventSchema.class.getClassLoader()
                .getResourceAsStream(AVRO_SCHEMA_PATH)) {
            return new Schema.Parser().parse(in);
        }
    }
}
