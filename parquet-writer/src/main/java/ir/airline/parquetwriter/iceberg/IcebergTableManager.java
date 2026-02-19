package ir.airline.parquetwriter.iceberg;

import ir.airline.parquetwriter.config.ParquetWriterConfig;
import ir.airline.parquetwriter.iceberg.writer.ParquetWriterService;
import ir.airline.parquetwriter.util.SchemaUtil;
import jakarta.annotation.PostConstruct;
import java.io.IOException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.iceberg.AppendFiles;
import org.apache.iceberg.DataFile;
import org.apache.iceberg.PartitionKey;
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
        Schema schema = SchemaUtil.ICEBERG_SCHEMA;
        PartitionSpec spec = PartitionSpec.builderFor(schema)
                .day("timestamp")
                .build();
        catalog.createTable(tableId, schema, spec);
        log.info("Created Iceberg table {} partitioned by day(timestamp)", tableId);
    }

    public void appendRecords(List<GenericRecord> avroRecords) throws IOException {
        List<Record> icebergRecords = convertToIcebergRecords(avroRecords);
        Map<LocalDate, List<Record>> byDay = icebergRecords.stream()
                .collect(Collectors.groupingBy(this::extractDay));
        AppendFiles append = table.newAppend();
        for (var entry : byDay.entrySet()) {
            LocalDate day = entry.getKey();
            List<Record> dayRecords = entry.getValue();
            PartitionKey pk = new PartitionKey(table.spec(), table.schema());
            int daysSinceEpoch = (int) day.toEpochDay();
            pk.set(0, daysSinceEpoch);

            String partitionDir = day + "/";
            String filename = ParquetWriterService.newParquetFilename();
            String relativePath = partitionDir + filename;
            DataFile df = parquetWriter.writeParquet(table, dayRecords, relativePath, pk);
            append.appendFile(df);
        }
        append.commit();
    }

    private LocalDate extractDay(Record r) {
        Object tsObj = r.getField("timestamp");
        if (!(tsObj instanceof LocalDateTime ts)) {
            throw new IllegalStateException("Record timestamp is not LocalDateTime: " + tsObj);
        }
        return ts.toLocalDate();
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
