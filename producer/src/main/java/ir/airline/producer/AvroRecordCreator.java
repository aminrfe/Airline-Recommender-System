package ir.airline.producer;

import java.io.IOException;
import java.io.InputStream;
import java.util.Map;
import org.apache.avro.generic.GenericRecord;
import org.apache.avro.generic.GenericData;
import org.apache.avro.Schema;

public class AvroRecordCreator {

    private static final String AVRO_SCHEMA_PATH = "avro/airline-schema.avsc";
    private static final Schema SCHEMA;

    static {
        try {
            InputStream schemaStream = AvroRecordCreator.class.getClassLoader()
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
}
