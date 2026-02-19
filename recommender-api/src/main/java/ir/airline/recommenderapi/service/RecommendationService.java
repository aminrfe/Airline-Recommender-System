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
                .map(r -> new RecommendationDto(r.getPassengerId(), r.getArrivalAirport(), r.getScore(), r.getGeneratedAt()))
                .toList();
    }

    public List<RecommendationDto> forPassengerAt(String passengerId, LocalDateTime generatedAt) {
        return repo.findByPassengerIdAndGeneratedAtOrderByScoreDesc(passengerId, generatedAt).stream()
                .map(r -> new RecommendationDto(r.getPassengerId(), r.getArrivalAirport(), r.getScore(), r.getGeneratedAt()))
                .toList();
    }
}
