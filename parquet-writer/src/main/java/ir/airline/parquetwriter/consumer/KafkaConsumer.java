package ir.airline.parquetwriter.consumer;

import ir.airline.parquetwriter.config.ParquetWriterConfig;
import ir.airline.parquetwriter.iceberg.IcebergTableManager;
import ir.airline.parquetwriter.util.SchemaUtil;
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
    private final List<GenericRecord> batch = new ArrayList<>();
    private final int batchSize;

    public KafkaConsumer(IcebergTableManager tableManager, ParquetWriterConfig config) {
        this.tableManager = tableManager;
        this.batchSize = config.getTable().getBatchSize();
    }

    @KafkaListener(
            topics = "${parquet-writer.kafka.topic}",
            groupId = "${spring.kafka.consumer.group-id}",
            containerFactory = "kafkaListenerContainerFactory"
    )
    public void consume(List<byte[]> messages) {
        log.info("Received {} Avro binary messages from Kafka", messages.size());
        if (messages.isEmpty()) {
            return;
        }

        for (byte[] payload : messages) {
            try {
                GenericRecord record = SchemaUtil.decode(payload, SchemaUtil.AVRO_SCHEMA);
                batch.add(record);
            } catch (Exception e) {
                log.error("Failed to decode Avro message: {}", e.getMessage());
            }
        }
        if (batch.size() >= batchSize) {
            flush();
        }
    }

    @PreDestroy
    public void shutdown() {
        log.info("Shutting down, flushing {} remaining records", batch.size());
        if (!batch.isEmpty()) {
            flush();
        }
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
