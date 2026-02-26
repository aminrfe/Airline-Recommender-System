package ir.airline.ingester;

import ir.airline.ingester.util.AvroUtil;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class IngesterService {

    private final KafkaTemplate<String, byte[]> kafkaTemplate;

    @Value("${spring.kafka.ingester.topic}")
    private String topic;

    public void sendRecord(Map<String, String> recordData) {
        try {
            log.info("Sending record: {}", recordData);
            GenericRecord avro = AvroUtil.createAvroRecord(recordData);
            byte[] payload = AvroUtil.encode(avro);

            kafkaTemplate.send(topic, payload);
            log.info("Avro (binary) event sent to Kafka topic {}", topic);

        } catch (Exception e) {
            log.error("Failed to encode/send Avro", e);
        }
    }
}
