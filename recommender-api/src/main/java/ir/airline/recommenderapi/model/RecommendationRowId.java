package ir.airline.recommenderapi.model;

import java.io.Serializable;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.EqualsAndHashCode;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@EqualsAndHashCode
public class RecommendationRowId implements Serializable {
    private String passengerId;
    private String arrivalAirport;
    private LocalDateTime generatedAt;
}
