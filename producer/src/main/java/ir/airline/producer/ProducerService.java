package ir.airline.producer;

import java.util.Map;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericRecord;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class ProducerService {

    private final KafkaTemplate<String, GenericRecord> kafkaTemplate;

    @Value("${spring.kafka.producer.topic}")
    private String topic;

    public void sendRecord(Map<String, String> recordData) {
        log.info("Sending record: {}", recordData);
        GenericRecord avroRecord = AvroRecordCreator.createAvroRecord(recordData);
        System.out.println(avroRecord.toString());
        ProducerRecord<String, GenericRecord> producerRecord =
                new ProducerRecord<>(topic, null, avroRecord);

        kafkaTemplate.send(producerRecord);
        log.info("Avro event sent to Kafka topic {}: {}", topic, avroRecord);
    }
}
