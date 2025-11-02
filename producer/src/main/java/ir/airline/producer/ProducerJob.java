package ir.airline.producer;

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Component;
import org.springframework.util.ResourceUtils;

@Component
@RequiredArgsConstructor
@Slf4j
public class ProducerJob {

    private final DatasetSimulator datasetSimulator;

    @Scheduled(fixedRate = 3600000)
    public void runSimulationJob() {
        try {
            String filePath = ResourceUtils.getFile("classpath:airline-dataset.csv")
                    .getAbsolutePath();
            datasetSimulator.simulateFromCSV(filePath);
            log.info("Simulation job completed successfully.");
        } catch (Exception e) {
            log.error("Error during simulation job: {}", e.getMessage());
        }
    }
}
