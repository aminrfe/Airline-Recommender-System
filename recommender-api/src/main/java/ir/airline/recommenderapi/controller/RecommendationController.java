package ir.airline.recommenderapi.controller;

import io.swagger.v3.oas.annotations.Parameter;
import ir.airline.recommenderapi.dto.RecommendationDto;
import ir.airline.recommenderapi.service.RecommendationService;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.util.List;
import lombok.RequiredArgsConstructor;
import org.springframework.format.annotation.DateTimeFormat;
import org.springframework.http.ResponseEntity;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PathVariable;
import org.springframework.web.bind.annotation.RequestMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

@RestController
@RequiredArgsConstructor
@RequestMapping("/api/recommendations")
public class RecommendationController {

    private final RecommendationService service;

    @GetMapping("/{passengerId}/latest")
    public List<RecommendationDto> latest(
            @Parameter(required = true) @PathVariable("passengerId") String passengerId
    ) {
        return service.latestForPassenger(passengerId);
    }

    @GetMapping("/{passengerId}")
    public ResponseEntity<List<RecommendationDto>> byDate(
            @Parameter(required = true) @PathVariable("passengerId") String passengerId,
            @RequestParam("date") @DateTimeFormat(iso = DateTimeFormat.ISO.DATE) LocalDate date
    ) {
        LocalDateTime start = date.atStartOfDay();
        LocalDateTime end = date.plusDays(1).atStartOfDay();
        return ResponseEntity.ok(service.forPassengerBetween(passengerId, start, end));
    }
}
