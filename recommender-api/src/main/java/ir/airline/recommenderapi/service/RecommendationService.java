package ir.airline.recommenderapi.service;

import ir.airline.recommenderapi.dto.RecommendationDto;
import ir.airline.recommenderapi.repository.RecommendationRepository;
import java.time.LocalDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
public class RecommendationService {

    private final RecommendationRepository repo;

    public List<RecommendationDto> latestForPassenger(String passengerId) {
        return repo.findLatestForPassenger(passengerId).stream()
                .filter(r -> r.getArrivalCity() != null)
                .map(r -> new RecommendationDto(r.getPassengerId(), r.getArrivalAirportCode(), r.getArrivalCity(),
                        r.getArrivalCountry(), r.getGeneratedAt()))
                .toList();
    }

    public List<RecommendationDto> forPassengerBetween(String passengerId, LocalDateTime start, LocalDateTime end) {
        return repo.findByPassengerIdAndGeneratedAtGreaterThanEqualAndGeneratedAtLessThanOrderByScoreDesc(
                        passengerId, start, end
                ).stream()
                .filter(r -> r.getArrivalCity() != null)
                .map(r -> new RecommendationDto(
                        r.getPassengerId(),
                        r.getArrivalAirportCode(),
                        r.getArrivalCity(),
                        r.getArrivalCountry(),
                        r.getGeneratedAt()
                ))
                .toList();
    }
}
