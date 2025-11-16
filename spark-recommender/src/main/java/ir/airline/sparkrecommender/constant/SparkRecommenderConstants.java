package ir.airline.sparkrecommender.constant;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SparkRecommenderConstants {

    // --- Input Schema Fields (from Avro/Iceberg) ---
    public static final String PASSENGER_ID = "passengerId";
    public static final String ARRIVAL_AIRPORT = "arrivalAirport";
    public static final String FLIGHT_STATUS = "flightStatus";
    public static final String TIMESTAMP = "timestamp";

    // --- Intermediate Mapped Fields (Spark Internal) ---
    public static final String USER_ID_MAPPED = "user_id";
    public static final String FLIGHT_ID_MAPPED = "flight_id";
    public static final String RATING = "rating";

    // --- Indexed and Prediction Fields (for ALS) ---
    public static final String USER_INDEX = "user_index";
    public static final String ITEM_INDEX = "item_index";
    public static final String PREDICTED_RATING = "predicted_rating";

    // --- Output Fields (PostgreSQL Table Columns) ---
    public static final String RANK_OUT = "rank";
    public static final String USER_ID_OUT = "userId";
    public static final String FLIGHT_ID_OUT = "flightId";
    public static final String PREDICTED_RATING_OUT = "predictedRating";
}
