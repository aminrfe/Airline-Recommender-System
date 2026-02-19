package ir.airline.recommenderapi.model;

import jakarta.persistence.Column;
import jakarta.persistence.Entity;
import jakarta.persistence.Id;
import jakarta.persistence.IdClass;
import jakarta.persistence.Table;
import java.time.LocalDateTime;
import lombok.AllArgsConstructor;
import lombok.Builder;
import lombok.Getter;
import lombok.NoArgsConstructor;
import lombok.Setter;

@Entity
@Table(name = "recommendations", schema = "public")
@Getter
@Setter
@NoArgsConstructor
@AllArgsConstructor
@Builder
@IdClass(RecommendationRowId.class)
public class RecommendationRow {

    @Id
    @Column(name = "passenger_id", nullable = false)
    private String passengerId;

    @Id
    @Column(name = "arrival_airport", nullable = false)
    private String arrivalAirport;

    @Id
    @Column(name = "generated_at", nullable = false)
    private LocalDateTime generatedAt;

    @Column(name = "score", nullable = false)
    private double score;
}
