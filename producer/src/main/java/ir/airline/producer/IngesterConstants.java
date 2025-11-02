package ir.airline.producer;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;

@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class IngesterConstants {

    // Constants for CSV dataset field names
    public static final String PASSENGER_ID_CSV = "Passenger ID";
    public static final String FIRST_NAME_CSV = "First Name";
    public static final String LAST_NAME_CSV = "Last Name";
    public static final String GENDER_CSV = "Gender";
    public static final String AGE_CSV = "Age";
    public static final String NATIONALITY_CSV = "Nationality";
    public static final String AIRPORT_NAME_CSV = "Airport Name";
    public static final String AIRPORT_COUNTRY_CODE_CSV = "Airport Country Code";
    public static final String COUNTRY_NAME_CSV = "Country Name";
    public static final String AIRPORT_CONTINENT_CSV = "Airport Continent";
    public static final String CONTINENTS_CSV = "Continents";
    public static final String DEPARTURE_DATE_CSV = "Departure Date";
    public static final String ARRIVAL_AIRPORT_CSV = "Arrival Airport";
    public static final String PILOT_NAME_CSV = "Pilot Name";
    public static final String FLIGHT_STATUS_CSV = "Flight Status";

    // Constants for Avro field names
    public static final String PASSENGER_ID = "passengerId";
    public static final String FIRST_NAME = "firstName";
    public static final String LAST_NAME = "lastName";
    public static final String GENDER = "gender";
    public static final String AGE = "age";
    public static final String NATIONALITY = "nationality";
    public static final String AIRPORT_NAME = "airportName";
    public static final String AIRPORT_COUNTRY_CODE = "airportCountryCode";
    public static final String COUNTRY_NAME = "countryName";
    public static final String AIRPORT_CONTINENT = "airportContinent";
    public static final String CONTINENTS = "continents";
    public static final String DEPARTURE_DATE = "departureDate";
    public static final String ARRIVAL_AIRPORT = "arrivalAirport";
    public static final String PILOT_NAME = "pilotName";
    public static final String FLIGHT_STATUS = "flightStatus";
    public static final String TIMESTAMP = "timestamp";

}
