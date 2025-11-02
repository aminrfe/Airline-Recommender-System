package ir.airline.parquetwriter.iceberg;

import ir.airline.parquetwriter.config.ParquetWriterConfig;
import ir.airline.parquetwriter.iceberg.writer.ParquetWriterService;
import ir.airline.parquetwriter.util.AirlineEventSchema;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionSpec;
import org.apache.iceberg.Schema;
import org.apache.iceberg.Table;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.data.Record;
import org.apache.iceberg.types.Types;
import org.springframework.stereotype.Component;

@Component
@RequiredArgsConstructor
@Slf4j
public class IcebergTableManager {

    private final Catalog catalog;
    private final ParquetWriterService parquetWriter;
    private final ParquetWriterConfig config;

    private Table table;
    private TableIdentifier tableId;
    private static int rows;

    @PostConstruct
    public void init() {
        log.info("KafkaConsumer bean created. Listening on topic: {}",
                config.getKafka().getTopic());
        tableId = TableIdentifier.of(config.getTable().getNamespace(), config.getTable().getName());
        ensureTableExists();
        table = catalog.loadTable(tableId);
    }

    private void ensureTableExists() {
        if (catalog.tableExists(tableId)) {
            log.info("Table {} exists.", tableId);
            return;
        }
        Schema schema = AirlineEventSchema.ICEBERG_SCHEMA;
        // TODO: Add partition key
        PartitionSpec spec = PartitionSpec.unpartitioned();
        catalog.createTable(tableId, schema, spec);
        log.info("Created Iceberg table {} with day(timestamp) partition", tableId);
    }

    public void appendRecords(List<GenericRecord> avroRecords) throws IOException {
        List<Record> icebergRecords = convertToIcebergRecords(avroRecords);
        List<DataFile> dataFiles = parquetWriter.writeParquet(table, icebergRecords);
        AppendFiles append = table.newAppend();
        dataFiles.forEach(append::appendFile);
        append.commit();
        log.info("Appended {} records to {}", avroRecords.size(), tableId);

        // TODO: Remove
        rows = rows + icebergRecords.size();
        System.out.println("✅ Total rows in Iceberg table: " + rows);
    }

    private List<Record> convertToIcebergRecords(List<GenericRecord> avroRecords) {
        List<Record> outList = new ArrayList<>();
        Schema schema = table.schema();

        for (GenericRecord avro : avroRecords) {
            org.apache.iceberg.data.GenericRecord out = org.apache.iceberg.data.GenericRecord.create(schema);

            LocalDateTime ts = null;

            for (Types.NestedField col : schema.columns()) {
                Object v = avro.get(col.name());

                if ("timestamp".equals(col.name())) {
                    if (v instanceof Long l) {
                        ts = LocalDateTime.ofInstant(Instant.ofEpochMilli(l), ZoneOffset.UTC);
                        v = ts;
                    } else if (v instanceof CharSequence s) {
                        try {
                            Instant inst;
                            try {
                                inst = Instant.parse(s.toString());
                            } catch (Exception ignore) {
                                inst = LocalDateTime.parse(s.toString()).toInstant(ZoneOffset.UTC);
                            }
                            ts = LocalDateTime.ofInstant(inst, ZoneOffset.UTC);
                            v = ts;
                        } catch (Exception e) {
                            v = null;
                        }
                    } else {
                        v = null;
                    }
                }

                out.setField(col.name(), v);
            }

            if (ts != null) {
                outList.add(out);
            }
        }
        return outList;
    }
}
