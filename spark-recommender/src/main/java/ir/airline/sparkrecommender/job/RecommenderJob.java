package ir.airline.sparkrecommender.job;

import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.ARRIVAL_AIRPORT;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.FLIGHT_ID_MAPPED;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.FLIGHT_ID_OUT;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.FLIGHT_STATUS;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.ITEM_INDEX;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.PASSENGER_ID;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.PREDICTED_RATING;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.PREDICTED_RATING_OUT;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.RANK_OUT;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.RATING;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.TIMESTAMP;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.USER_ID_MAPPED;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.USER_ID_OUT;
import static ir.airline.sparkrecommender.constant.SparkRecommenderConstants.USER_INDEX;
import static org.apache.spark.sql.functions.col;
import static org.apache.spark.sql.functions.explode;
import static org.apache.spark.sql.functions.from_unixtime;
import static org.apache.spark.sql.functions.lit;
import static org.apache.spark.sql.functions.rank;

import ir.airline.sparkrecommender.config.SparkRecommenderProperties;
import java.time.LocalDate;
import java.util.Properties;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.spark.ml.evaluation.RegressionEvaluator;
import org.apache.spark.ml.feature.StringIndexer;
import org.apache.spark.ml.feature.StringIndexerModel;
import org.apache.spark.ml.recommendation.ALS;
import org.apache.spark.ml.recommendation.ALSModel;
import org.apache.spark.sql.Dataset;
import org.apache.spark.sql.Row;
import org.apache.spark.sql.SaveMode;
import org.apache.spark.sql.SparkSession;
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.apache.spark.sql.types.DataTypes;
import org.springframework.core.env.Environment;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class RecommenderJob {

    private final SparkSession spark;
    private final SparkRecommenderProperties props;
    private final Environment env;

    private record IndexedData(Dataset<Row> indexedDf) {}

    /**
     * Executes the daily recommendation job using a cron expression.
     * Runs daily at 2:00 AM (0 0 2 * * *).
     */
    @Scheduled(cron = "0 0 2 * * *")
    public void executeDailyJob() {
        log.info("--- Starting Daily Spark Recommender Job ---");
        LocalDate yesterday = LocalDate.now().minusDays(1);
        String yesterdayStr = yesterday.toString();

        Dataset<Row> indexedDf = null;

        try {
            // 1. Load Data
            Dataset<Row> eventsDf = loadData(yesterdayStr);
            if (eventsDf.isEmpty()) {
                log.warn("No data found for {}. Skipping job execution.", yesterdayStr);
                return;
            }

            // 2. Preprocess & Index
            Dataset<Row> ratingsDf = createRatings(eventsDf);
            if (ratingsDf.isEmpty()) {
                log.warn("No usable ratings data found after filtering. Skipping job execution.");
                return;
            }

            IndexedData indexedData = indexData(ratingsDf);
            indexedDf = indexedData.indexedDf();
            indexedDf.cache();

            // 3. Train Model
            ALSModel model = trainModel(indexedDf);

            // 4. Predict & Rank
            Dataset<Row> finalRecommendations = generateRankedRecs(model, indexedDf);

            // 5. Save Results to PostgreSQL
            saveRecommendationsToPostgres(finalRecommendations);

        } catch (Exception e) {
            log.error("Critical error during daily recommender job execution.", e);
        } finally {
            // 6. Cleanup
            if (indexedDf != null) {
                indexedDf.unpersist();
                log.info("Unpersisted cached data.");
            }
            log.info("--- Daily Spark Recommender Job Finished ---");
        }
    }

    // --- Data Loading and Preprocessing Methods ---

    /**
     * Loads the event data from the Iceberg table filtered by the event date.
     * Uses 'timestamp' field and casts it to Date for filtering.
     */
    private Dataset<Row> loadData(String dateString) {
        String tableName = props.getFullSourceTableName();
        log.info("Loading data from Iceberg table: {}", tableName);
        return spark.table(tableName)
                // Convert long (timestamp-millis) to Date for filtering
                .filter(
                        from_unixtime(col(TIMESTAMP).divide(1000), "yyyy-MM-dd").cast(DataTypes.DateType).equalTo(lit(dateString))
                );
    }

    /**
     * Transforms raw flight data into user-item ratings (implicit feedback).
     */
    private Dataset<Row> createRatings(Dataset<Row> eventsDf) {
        // Filter for successful flights and assign a constant rating of 1.0 (implicit preference)
        Dataset<Row> ratingsDf = eventsDf
                .filter(col(FLIGHT_STATUS).notEqual("Cancelled"))
                .select(
                        col(PASSENGER_ID).as(USER_ID_MAPPED),
                        col(ARRIVAL_AIRPORT).as(FLIGHT_ID_MAPPED)
                )
                .withColumn(RATING, lit(1.0));

        log.info("Created {} non-zero ratings based on successful trips.", ratingsDf.count());
        return ratingsDf;
    }

    /**
     * Indexes the string user_id and flight_id into numeric indices required by ALS.
     */
    private IndexedData indexData(Dataset<Row> ratingsDf) {
        StringIndexerModel userIndexer = new StringIndexer()
                .setInputCol(USER_ID_MAPPED)
                .setOutputCol(USER_INDEX)
                .fit(ratingsDf);
        Dataset<Row> indexedUserDf = userIndexer.transform(ratingsDf);

        StringIndexerModel itemIndexer = new StringIndexer()
                .setInputCol(FLIGHT_ID_MAPPED)
                .setOutputCol(ITEM_INDEX)
                .fit(indexedUserDf);
        Dataset<Row> indexedDf = itemIndexer.transform(indexedUserDf);

        log.info("User and item IDs indexed successfully.");
        return new IndexedData(indexedDf);
    }

    // --- Model Training and Prediction Methods ---

    /**
     * Trains the Alternating Least Squares (ALS) model and evaluates RMSE.
     */
    private ALSModel trainModel(Dataset<Row> indexedDf) {
        Dataset<Row>[] splits = indexedDf.randomSplit(new double[]{0.8, 0.2});
        Dataset<Row> trainingData = splits[0];
        Dataset<Row> testData = splits[1];

        ALS als = new ALS()
                .setMaxIter(10)
                .setRegParam(0.1)
                .setUserCol(USER_INDEX)
                .setItemCol(ITEM_INDEX)
                .setRatingCol(RATING)
                .setColdStartStrategy("drop")
                .setImplicitPrefs(true); // Implicit feedback model

        log.info("Training ALS model (ImplicitPrefs: true)...");
        ALSModel model = als.fit(trainingData);

        // Evaluation
        Dataset<Row> predictions = model.transform(testData);
        RegressionEvaluator evaluator = new RegressionEvaluator()
                .setMetricName("rmse")
                .setLabelCol(RATING)
                .setPredictionCol("prediction");

        double rmse = evaluator.evaluate(predictions.na().drop());
        log.info("Model Evaluation: RMSE on test data = {}", rmse);

        return model;
    }

    /**
     * Generates top recommendations, maps them back to string IDs, and assigns a rank.
     */
    private Dataset<Row> generateRankedRecs(ALSModel model, Dataset<Row> indexedDf) {
        log.info("Generating top 10 recommendations for all users.");
        Dataset<Row> allUserRecs = model.recommendForAllUsers(10);

        // Explode recommendations
        Dataset<Row> indexedRecs = allUserRecs
                .select(col(USER_INDEX), explode(col("recommendations")).as("rec"))
                .select(
                        col(USER_INDEX),
                        col("rec.item_index").as(ITEM_INDEX),
                        col("rec.rating").as(PREDICTED_RATING)
                );

        // Create lookup tables for string IDs
        Dataset<Row> userLookup = indexedDf.select(USER_ID_MAPPED, USER_INDEX).distinct();
        Dataset<Row> itemLookup = indexedDf.select(FLIGHT_ID_MAPPED, ITEM_INDEX).distinct();

        // Join to map back to original IDs
        Dataset<Row> finalRecommends = indexedRecs
                .join(userLookup, USER_INDEX)
                .join(itemLookup, ITEM_INDEX)
                .select(USER_ID_MAPPED, FLIGHT_ID_MAPPED, PREDICTED_RATING);

        // Add a 'rank' column
        WindowSpec userWindow = Window.partitionBy(USER_ID_MAPPED).orderBy(col(PREDICTED_RATING).desc());
        return finalRecommends.withColumn(RANK_OUT, rank().over(userWindow));
    }

    /**
     * Writes the final ranked recommendations DataFrame to the PostgreSQL API table.
     */
    private void saveRecommendationsToPostgres(Dataset<Row> rankedRecommends) {
        String recommendationTable = props.getOutputRecommendationTable();
        log.info("Saving {} recommendation rows to PostgreSQL table: {}", rankedRecommends.count(), recommendationTable);

        // Setup JDBC connection properties
        Properties jdbcProps = new Properties();
        jdbcProps.put("user", env.getRequiredProperty("spring.datasource.username"));
        jdbcProps.put("password", env.getRequiredProperty("spring.datasource.password"));
        jdbcProps.put("driver", env.getRequiredProperty("spring.datasource.driver-class-name"));
        String jdbcUrl = env.getRequiredProperty("spring.datasource.url");

        // Write the DataFrame to the JDBC sink.
        rankedRecommends
                .select(
                        // Map internal names to target JPA/DB column names
                        col(USER_ID_MAPPED).as(USER_ID_OUT),
                        col(FLIGHT_ID_MAPPED).as(FLIGHT_ID_OUT),
                        col(PREDICTED_RATING).as(PREDICTED_RATING_OUT),
                        col(RANK_OUT)
                )
                .write()
                .mode(SaveMode.Overwrite)
                .jdbc(jdbcUrl, recommendationTable, jdbcProps);

        log.info("Recommendations successfully saved and previous data overwritten.");
    }
}
