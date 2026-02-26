package ir.airline.ingester;

import java.io.FileReader;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import lombok.RequiredArgsConstructor;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVRecord;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class DatasetSimulator {

    private final IngesterService ingesterService;

    public void simulateFromCSV(String filePath) throws IOException {
        List<Map<String, String>> dataset = new ArrayList<>();

        try (FileReader reader = new FileReader(filePath, StandardCharsets.UTF_8)) {
            Iterable<CSVRecord> records = CSVFormat.DEFAULT
                    .withFirstRecordAsHeader()
                    .parse(reader);

            for (CSVRecord record : records) {
                Map<String, String> data = new HashMap<>();

                data.put(IngesterConstants.PASSENGER_ID, record.get(IngesterConstants.PASSENGER_ID_CSV));
                data.put(IngesterConstants.FIRST_NAME, record.get(IngesterConstants.FIRST_NAME_CSV));
                data.put(IngesterConstants.LAST_NAME, record.get(IngesterConstants.LAST_NAME_CSV));
                data.put(IngesterConstants.GENDER, record.get(IngesterConstants.GENDER_CSV));
                data.put(IngesterConstants.AGE, record.get(IngesterConstants.AGE_CSV));
                data.put(IngesterConstants.NATIONALITY, record.get(IngesterConstants.NATIONALITY_CSV));
                data.put(IngesterConstants.AIRPORT_NAME, record.get(IngesterConstants.AIRPORT_NAME_CSV));
                data.put(IngesterConstants.AIRPORT_COUNTRY_CODE, record.get(IngesterConstants.AIRPORT_COUNTRY_CODE_CSV));
                data.put(IngesterConstants.COUNTRY_NAME, record.get(IngesterConstants.COUNTRY_NAME_CSV));
                data.put(IngesterConstants.AIRPORT_CONTINENT, record.get(IngesterConstants.AIRPORT_CONTINENT_CSV));
                data.put(IngesterConstants.CONTINENTS, record.get(IngesterConstants.CONTINENTS_CSV));
                data.put(IngesterConstants.DEPARTURE_DATE, record.get(IngesterConstants.DEPARTURE_DATE_CSV));
                data.put(IngesterConstants.ARRIVAL_AIRPORT, record.get(IngesterConstants.ARRIVAL_AIRPORT_CSV));
                data.put(IngesterConstants.PILOT_NAME, record.get(IngesterConstants.PILOT_NAME_CSV));
                data.put(IngesterConstants.FLIGHT_STATUS, record.get(IngesterConstants.FLIGHT_STATUS_CSV));

                dataset.add(data);
            }
        }
        dataset.forEach(ingesterService::sendRecord);
    }
}

