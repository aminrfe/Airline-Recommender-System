package ir.airline.parquetwriter.consumer;

import ir.airline.parquetwriter.config.ParquetWriterConfig;
import ir.airline.parquetwriter.iceberg.IcebergTableManager;
import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import jakarta.annotation.PreDestroy;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.stereotype.Service;

@Service
@Slf4j
public class KafkaConsumer {

    private final IcebergTableManager tableManager;
    private final ParquetWriterConfig config;
    private final List<GenericRecord> batch = new ArrayList<>();
    private final int batchSize;

    public KafkaConsumer(IcebergTableManager tableManager, ParquetWriterConfig config) {
        this.tableManager = tableManager;
        this.config = config;
        this.batchSize = config.getTable().getBatchSize();
    }

    @KafkaListener(topics = "${parquet-writer.kafka.topic}")
    public void consume(List<GenericRecord> records) {
        log.info("Received {} records from Kafka", records.size());
        batch.addAll(records);

        if (batch.size() >= batchSize) {
            flush();
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down, flushing {} remaining records", batch.size());
        if (!batch.isEmpty()) flush();
    }

    private void flush() {
        try {
            tableManager.appendRecords(new ArrayList<>(batch));
            log.info("Flushed {} records to Iceberg", batch.size());
            batch.clear();
        } catch (IOException e) {
            log.error("Failed to append batch to Iceberg", e);
        }
    }
}
