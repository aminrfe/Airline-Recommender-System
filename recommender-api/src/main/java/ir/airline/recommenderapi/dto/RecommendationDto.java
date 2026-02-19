package ir.airline.recommenderapi.dto;

import java.time.LocalDateTime;

public record RecommendationDto(
        String passengerId,
        String arrivalAirport,
        double score,
        LocalDateTime generatedAt
) {}
