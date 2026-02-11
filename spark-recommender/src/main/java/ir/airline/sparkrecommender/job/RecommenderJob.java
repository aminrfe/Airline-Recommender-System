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
import org.apache.spark.sql.expressions.Window;
import org.apache.spark.sql.expressions.WindowSpec;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.stereotype.Service;

@Service
@RequiredArgsConstructor
@Slf4j
public class RecommenderJob {

    private final SparkSession spark;
    private final SparkRecommenderProperties props;

    private static final String RECO_TABLE = "public.recommendations";
    private static final boolean EVAL_ENABLED = false;
    private static final int TOP_K = 5;
    private static final int CANDIDATES = 50;
    private static final long RAND_SEED = 42L;

    @Scheduled(initialDelay = 5000, fixedDelay = Long.MAX_VALUE)
    public void executeJob() {
        log.info("--- Starting Spark Iceberg Recommender Job ---");

        try {
            Dataset<Row> rawData = spark.table(props.getFullSourceTableName());

            Column statusWeight = when(col(FLIGHT_STATUS).equalTo("On Time"), lit(1.0))
                    .when(col(FLIGHT_STATUS).equalTo("Delayed"), lit(0.6))
                    .when(col(FLIGHT_STATUS).equalTo("Cancelled"), lit(0.0))
                    .otherwise(lit(0.3));

            Dataset<Row> interactions = rawData
                    .withColumn("w", statusWeight)
                    .groupBy(PASSENGER_ID, ARRIVAL_AIRPORT)
                    .agg(sum("w").alias("strength"))
                    .filter(col("strength").gt(0));

            StringIndexerModel userIndexer = new StringIndexer()
                    .setInputCol(PASSENGER_ID)
                    .setOutputCol(USER_INDEX)
                    .setHandleInvalid("skip")
                    .fit(interactions);

            StringIndexerModel itemIndexer = new StringIndexer()
                    .setInputCol(ARRIVAL_AIRPORT)
                    .setOutputCol(ITEM_INDEX)
                    .setHandleInvalid("skip")
                    .fit(interactions);

            Dataset<Row> indexed = itemIndexer.transform(userIndexer.transform(interactions))
                    .withColumn(USER_INDEX, col(USER_INDEX).cast("int"))
                    .withColumn(ITEM_INDEX, col(ITEM_INDEX).cast("int"))
                    .withColumn("rating", col("strength").cast("float"))
                    .select(col(PASSENGER_ID), col(ARRIVAL_AIRPORT), col(USER_INDEX), col(ITEM_INDEX), col("rating"));

            Dataset<Row> userMap = indexed.select(col(USER_INDEX), col(PASSENGER_ID)).distinct();
            Dataset<Row> itemMap = indexed.select(col(ITEM_INDEX), col(ARRIVAL_AIRPORT)).distinct();

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

            if (EVAL_ENABLED) {
                runOfflineEvaluation(indexed, als);
            }

            ALSModel model = als.fit(indexed);

            Dataset<Row> recs = model.recommendForAllUsers(TOP_K);

            Dataset<Row> finalRecs = recs
                    .select(col(USER_INDEX), explode(col("recommendations")).as("rec"))
                    .select(col(USER_INDEX),
                            col("rec.item_index").as(ITEM_INDEX),
                            col("rec.rating").as("score"))
                    .join(userMap, USER_INDEX)
                    .join(itemMap, ITEM_INDEX)
                    .select(col(PASSENGER_ID), col(ARRIVAL_AIRPORT), col("score"));

            saveRecommendationsToPostgres(finalRecs);

        } catch (Exception e) {
            log.error("Error in Spark Job", e);
        }
    }

    private void saveRecommendationsToPostgres(Dataset<Row> finalRecs) {
        Dataset<Row> repartitioned = finalRecs.repartition(8);

        repartitioned.write()
                .format("jdbc")
                .option("url", props.getPostgresProperties().getUrl())
                .option("dbtable", RECO_TABLE)
                .option("user", props.getPostgresProperties().getUser())
                .option("password", props.getPostgresProperties().getPassword())
                .option("driver", props.getPostgresProperties().getDriver())
                .option("batchsize", "5000")
                .mode(SaveMode.Overwrite)
                .save();

        log.info("Recommendations saved to Postgres table: {}", RECO_TABLE);
    }


    /**
     * Offline validation using leave-one-out per user:
     * - Hold out 1 interaction per user as test
     * - Train ALS on the rest
     * - Recommend top-K (after filtering "seen" items)
     * - Compute HitRate@K and MRR@K
     * - Compare to a popularity baseline
     */
    private void runOfflineEvaluation(Dataset<Row> indexed, ALS als) {
        log.info("--- Running Offline Evaluation (leave-one-out) ---");

        // Guard: need enough data
        long rowCount = indexed.count();
        if (rowCount < 10) {
            log.warn("Not enough rows ({}) for a meaningful evaluation. Skipping.", rowCount);
            return;
        }

        Dataset<Row> withRand = indexed.withColumn("rand", rand(RAND_SEED));
        WindowSpec wUser = Window.partitionBy(col(USER_INDEX)).orderBy(col("rand").desc());

        Dataset<Row> ranked = withRand.withColumn("rn", row_number().over(wUser));

        Dataset<Row> test = ranked
                .filter(col("rn").equalTo(1))
                .select(col(USER_INDEX), col(ITEM_INDEX).alias("true_item"));

        Dataset<Row> train = ranked
                .filter(col("rn").gt(1))
                .drop("rn", "rand");

        Dataset<Row> trainUsers = train.select(col(USER_INDEX)).distinct();
        test = test.join(trainUsers, USER_INDEX);

        long usersEvaluated = test.count();
        if (usersEvaluated == 0) {
            log.warn("No users with >= 2 interactions (cannot do leave-one-out). Skipping evaluation.");
            return;
        }

        ALSModel evalModel = als.fit(train);

        Dataset<Row> recs = evalModel.recommendForAllUsers(CANDIDATES);

        Dataset<Row> recFlat = recs
                .select(col(USER_INDEX), explode(col("recommendations")).as("rec"))
                .select(
                        col(USER_INDEX),
                        col("rec.item_index").alias(ITEM_INDEX),
                        col("rec.rating").alias("score")
                );

        Dataset<Row> seen = train.select(col(USER_INDEX), col(ITEM_INDEX)).distinct();

        Dataset<Row> recFiltered = recFlat.join(
                seen,
                recFlat.col(USER_INDEX).equalTo(seen.col(USER_INDEX))
                        .and(recFlat.col(ITEM_INDEX).equalTo(seen.col(ITEM_INDEX))),
                "left_anti"
        );

        WindowSpec wRec = Window.partitionBy(col(USER_INDEX)).orderBy(col("score").desc());

        Dataset<Row> topK = recFiltered
                .withColumn("rank", row_number().over(wRec))
                .filter(col("rank").leq(TOP_K))
                .select(col(USER_INDEX), col(ITEM_INDEX), col("rank"));

        Dataset<Row> joined = test.join(
                topK,
                test.col(USER_INDEX).equalTo(topK.col(USER_INDEX))
                        .and(test.col("true_item").equalTo(topK.col(ITEM_INDEX))),
                "left"
        );

        Row alsMetrics = joined
                .withColumn("hit", when(col("rank").isNotNull(), lit(1.0)).otherwise(lit(0.0)))
                .withColumn("rr", when(col("rank").isNotNull(), lit(1.0).divide(col("rank"))).otherwise(lit(0.0)))
                .agg(
                        avg("hit").alias("HitRate"),
                        avg("rr").alias("MRR")
                )
                .first();

        double alsHitRate = alsMetrics.getAs("HitRate");
        double alsMrr = alsMetrics.getAs("MRR");

        log.info("ALS Evaluation (K={}): users={}, HitRate@{}={}, MRR@{}={}",
                TOP_K, usersEvaluated, TOP_K, alsHitRate, TOP_K, alsMrr);

        Dataset<Row> popTop = train.groupBy(col(ITEM_INDEX))
                .agg(sum(col("rating")).alias("pop"))
                .orderBy(col("pop").desc())
                .limit(TOP_K);

        WindowSpec wPop = Window.orderBy(col("pop").desc());
        Dataset<Row> popTopRanked = popTop
                .withColumn("rank", row_number().over(wPop))
                .select(col(ITEM_INDEX), col("rank"));

        Dataset<Row> users = test.select(col(USER_INDEX)).distinct();

        Dataset<Row> popRecs = users.crossJoin(popTopRanked)
                .select(col(USER_INDEX), col(ITEM_INDEX), col("rank"));

        Dataset<Row> popJoined = test.join(
                popRecs,
                test.col(USER_INDEX).equalTo(popRecs.col(USER_INDEX))
                        .and(test.col("true_item").equalTo(popRecs.col(ITEM_INDEX))),
                "left"
        );

        Row popMetrics = popJoined
                .withColumn("hit", when(col("rank").isNotNull(), lit(1.0)).otherwise(lit(0.0)))
                .withColumn("rr", when(col("rank").isNotNull(), lit(1.0).divide(col("rank"))).otherwise(lit(0.0)))
                .agg(
                        avg("hit").alias("HitRate"),
                        avg("rr").alias("MRR")
                )
                .first();

        double popHitRate = popMetrics.getAs("HitRate");
        double popMrr = popMetrics.getAs("MRR");

        log.info("Popularity Baseline (K={}): users={}, HitRate@{}={}, MRR@{}={}",
                TOP_K, usersEvaluated, TOP_K, popHitRate, TOP_K, popMrr);

        if (alsHitRate > popHitRate) {
            log.info("ALS beats popularity on HitRate@{} ({} vs {}).", TOP_K, alsHitRate, popHitRate);
        } else {
            log.warn("ALS does NOT beat popularity on HitRate@{} ({} vs {}). Consider more data/tuning.",
                    TOP_K, alsHitRate, popHitRate);
        }
    }
}
