package ir.airline.recommenderapi.dto;

import java.time.LocalDateTime;

public record RecommendationDto(
        String passengerId,
        String airportCode,
        String city,
        String country,
        LocalDateTime generatedAt
) {}