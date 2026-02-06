package ir.airline.sparkrecommender.job;

import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.*;
import static org.apache.spark.sql.functions.*;

import ir.airline.sparkrecommender.config.SparkRecommenderProperties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Column;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class RecommenderJob {

    private final SparkSession spark;
    private final SparkRecommenderProperties props;

    @Scheduled(initialDelay = 5000, fixedDelay = Long.MAX_VALUE)
    public void executeJob() {
        log.info("--- Starting Spark Iceberg Recommender Job ---");

        try {
            // 1. Load from Iceberg
            Dataset<Row> rawData = spark.table(props.getFullSourceTableName());

            // 2. Feature Engineering (Implicit Ratings)
            // Replicating status_weight logic from Python
            Column statusWeight = when(col(FLIGHT_STATUS).equalTo("On Time"), lit(1.0))
                    .when(col(FLIGHT_STATUS).equalTo("Delayed"), lit(0.6))
                    .when(col(FLIGHT_STATUS).equalTo("Cancelled"), lit(0.0))
                    .otherwise(lit(0.3));

            Dataset<Row> interactions = rawData
                    .withColumn("w", statusWeight)
                    .groupBy(PASSENGER_ID, ARRIVAL_AIRPORT)
                    .agg(sum("w").alias("strength"))
                    .filter(col("strength").gt(0));

            // 3. Indexing
            StringIndexerModel userIndexer = new StringIndexer()
                    .setInputCol(PASSENGER_ID)
                    .setOutputCol(USER_INDEX)
                    .fit(interactions);

            StringIndexerModel itemIndexer = new StringIndexer()
                    .setInputCol(ARRIVAL_AIRPORT)
                    .setOutputCol(ITEM_INDEX)
                    .fit(interactions);

            Dataset<Row> indexed = itemIndexer.transform(userIndexer.transform(interactions))
                    .withColumn(USER_INDEX, col(USER_INDEX).cast("int"))
                    .withColumn(ITEM_INDEX, col(ITEM_INDEX).cast("int"))
                    .withColumn("rating", col("strength").cast("float"));

            // Mapping tables to recover String IDs later
            Dataset<Row> userMap = indexed.select(col(USER_INDEX), col(PASSENGER_ID)).distinct();
            Dataset<Row> itemMap = indexed.select(col(ITEM_INDEX), col(ARRIVAL_AIRPORT)).distinct();

            // 4. Model Training (Matching Python Params)
            ALS als = new ALS()
                    .setUserCol(USER_INDEX)
                    .setItemCol(ITEM_INDEX)
                    .setRatingCol("rating")
                    .setImplicitPrefs(true)
                    .setRank(20)
                    .setRegParam(0.1)
                    .setAlpha(20.0)
                    .setMaxIter(15)
                    .setColdStartStrategy("drop");

            ALSModel model = als.fit(indexed);

            // 5. Generate Recommendations
            Dataset<Row> recs = model.recommendForAllUsers(5);

            // Flatten and Join back to String Labels
            Dataset<Row> finalRecs = recs
                    .select(col(USER_INDEX), explode(col("recommendations")).as("rec"))
                    .select(col(USER_INDEX), col("rec.item_index").as(ITEM_INDEX), col("rec.rating").as("score"))
                    .join(userMap, USER_INDEX)
                    .join(itemMap, ITEM_INDEX)
                    .select(col(PASSENGER_ID), col(ARRIVAL_AIRPORT), col("score"));

            // 6. Save as CSV to MinIO/S3A
            String outputPath = "s3a://" + props.getMinioProperties().getBucket() + "/output/recommendations";

            finalRecs.coalesce(1)
                    .write()
                    .mode(SaveMode.Overwrite)
                    .option("header", "true")
                    .csv(outputPath);

            log.info("Job completed. Output saved to: {}", outputPath);

        } catch (Exception e) {
            log.error("Error in Spark Job", e);
        }
    }
}
