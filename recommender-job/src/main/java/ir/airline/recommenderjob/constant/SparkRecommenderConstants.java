package ir.airline.recommenderjob.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SparkRecommenderConstants {

    // --- Input Schema Fields (from Avro/Iceberg) ---
    public static final String PASSENGER_ID = "passengerId";
    public static final String ARRIVAL_AIRPORT = "arrivalAirport";
    public static final String FLIGHT_STATUS = "flightStatus";
    public static final String TIMESTAMP = "timestamp";

    // --- Indexed and Prediction Fields (for ALS) ---
    public static final String USER_INDEX = "user_index";
    public static final String ITEM_INDEX = "item_index";
}
