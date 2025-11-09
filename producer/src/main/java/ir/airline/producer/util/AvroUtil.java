package ir.airline.producer.util;

import ir.airline.producer.IngesterConstants;
import java.io.ByteArrayOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.avro.generic.GenericDatumWriter;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;
import org.apache.avro.io.BinaryEncoder;
import org.apache.avro.io.DatumWriter;
import org.apache.avro.io.EncoderFactory;

public class AvroUtil {

    private static final String AVRO_SCHEMA_PATH = "avro/airline-schema.avsc";
    private static final Schema SCHEMA;

    static {
        try {
            InputStream schemaStream = AvroUtil.class.getClassLoader()
                    .getResourceAsStream(AVRO_SCHEMA_PATH);
            SCHEMA = new Schema.Parser().parse(schemaStream);
        } catch (IOException e) {
            throw new RuntimeException("Failed to load Avro schema", e);
        }
    }

    public static GenericRecord createAvroRecord(Map<String, String> recordData) {
        GenericRecord record = new GenericData.Record(SCHEMA);

        record.put(IngesterConstants.PASSENGER_ID, recordData.get(IngesterConstants.PASSENGER_ID));
        record.put(IngesterConstants.FIRST_NAME, recordData.get(IngesterConstants.FIRST_NAME));
        record.put(IngesterConstants.LAST_NAME, recordData.get(IngesterConstants.LAST_NAME));
        record.put(IngesterConstants.GENDER, recordData.get(IngesterConstants.GENDER));
        record.put(IngesterConstants.AGE, Integer.parseInt(recordData.get(IngesterConstants.AGE)));
        record.put(IngesterConstants.NATIONALITY, recordData.get(IngesterConstants.NATIONALITY));
        record.put(IngesterConstants.AIRPORT_NAME, recordData.get(IngesterConstants.AIRPORT_NAME));
        record.put(IngesterConstants.AIRPORT_COUNTRY_CODE, recordData.get(IngesterConstants.AIRPORT_COUNTRY_CODE));
        record.put(IngesterConstants.COUNTRY_NAME, recordData.get(IngesterConstants.COUNTRY_NAME));
        record.put(IngesterConstants.AIRPORT_CONTINENT, recordData.get(IngesterConstants.AIRPORT_CONTINENT));
        record.put(IngesterConstants.CONTINENTS, recordData.get(IngesterConstants.CONTINENTS));
        record.put(IngesterConstants.DEPARTURE_DATE, recordData.get(IngesterConstants.DEPARTURE_DATE));
        record.put(IngesterConstants.ARRIVAL_AIRPORT, recordData.get(IngesterConstants.ARRIVAL_AIRPORT));
        record.put(IngesterConstants.PILOT_NAME, recordData.get(IngesterConstants.PILOT_NAME));
        record.put(IngesterConstants.FLIGHT_STATUS, recordData.get(IngesterConstants.FLIGHT_STATUS));

        record.put(IngesterConstants.TIMESTAMP, System.currentTimeMillis());
        return record;
    }

    public static byte[] encode(GenericRecord record) throws Exception {
        Schema schema = record.getSchema();
        ByteArrayOutputStream out = new ByteArrayOutputStream();
        DatumWriter<GenericRecord> writer = new GenericDatumWriter<>(schema);
        BinaryEncoder enc = EncoderFactory.get().binaryEncoder(out, null);
        writer.write(record, enc);
        enc.flush();
        return out.toByteArray();
    }
}
