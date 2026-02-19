package ir.airline.recommenderapi.repository;

import ir.airline.recommenderapi.model.RecommendationRow;
import ir.airline.recommenderapi.model.RecommendationRowId;
import java.time.LocalDateTime;
import java.util.List;
import org.springframework.data.jpa.repository.JpaRepository;
import org.springframework.data.jpa.repository.Query;
import org.springframework.data.repository.query.Param;

public interface RecommendationRepository extends JpaRepository<RecommendationRow, RecommendationRowId> {

    @Query("""
              select r
              from RecommendationRow r
              where r.passengerId = :passengerId
                and r.generatedAt = (
                  select max(r2.generatedAt)
                  from RecommendationRow r2
                  where r2.passengerId = :passengerId
                )
              order by r.score desc
          """)
    List<RecommendationRow> findLatestForPassenger(@Param("passengerId") String passengerId);

    @Query("""
              select max(r.generatedAt)
              from RecommendationRow r
              where r.passengerId = :passengerId
          """)
    LocalDateTime findLatestGeneratedAt(@Param("passengerId") String passengerId);

    List<RecommendationRow> findByPassengerIdAndGeneratedAtOrderByScoreDesc(String passengerId, LocalDateTime generatedAt);
}
