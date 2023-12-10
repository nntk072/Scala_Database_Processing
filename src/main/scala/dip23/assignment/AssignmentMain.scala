package dip23.assignment

// add anything that is required here
import org.apache.spark.ml.clustering.KMeans
import org.apache.spark.ml.feature.VectorAssembler
import org.apache.spark.sql.{DataFrame, Row, SparkSession}
import org.apache.spark.sql.expressions.Window
import org.apache.spark.sql.functions._
import org.apache.spark.sql.types._


object AssignmentMain extends App {
    // Create the Spark session
	val spark: SparkSession = SparkSession.builder()
        .appName("assignment")
        .config("spark.driver.host", "localhost")
        .master("local")
        .getOrCreate()

    // suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel("ERROR")


    // COMP.CS.320 Data-Intensive Programming, Assignment
    //
    // The instructions for the tasks in the assignment are given in
    // the markdown file "Assignment-tasks.md" at the root of the repository.
    // This file contains only the given starting code not the instructions.
    //
    // The tasks that can be done in either Scala or Python.
    // This is the Scala version intended for local development.
    //
    // For the local development the source data for the tasks can be located in the data folder of the repository.
    // For example, instead of the path "data/sales/video_game_sales.csv"
    // for the Basic Task 1, you should use the path "../data/sales/video_game_sales.csv".
    //
    // Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    //
    // Comment out the additional tasks 2 and 3 if you did not implement them.
    //
    // Don't forget to submit your solutions to Moodle once your group is finished with the assignment.



    printTaskLine("Basic Task 1")
    // ==========================
    val videoGameDataFrame: DataFrame = spark.read
      .option("inferSchema", "true")
      .option("header", "true")
      .csv("data/sales/video_game_sales.csv")
    videoGameDataFrame.printSchema()
    // Consider first 10 years 2000-2009
    val firstTenYearsDF = videoGameDataFrame.filter(col("Year").between(2000, 2009))
    val bestEUPublisher: String = firstTenYearsDF.groupBy("Publisher")
      .sum("EU_Sales").withColumnRenamed("sum(EU_Sales)", "EU_Sales")
      .sort(desc("EU_Sales"))
      .select("Publisher")
      .first().getAs[String]("Publisher")

    val bestEUPublisherSales: DataFrame = firstTenYearsDF.filter(col("Publisher") === bestEUPublisher)
      .groupBy("Year")
      .agg(round(sum("EU_Sales"), 2).alias("EU_Total"), round(sum("Global_Sales"), 2).alias("Global_Total"))
      .sort("Year")


    println(s"The publisher with the highest total video game sales in European Union is: '$bestEUPublisher'")
    println("Sales data for the publisher:")
    bestEUPublisherSales.show(10)



    printTaskLine("Basic Task 2")
    // ==========================
    val file_path = "data/nhl_shots.parquet"
    val shotsFullColumns: DataFrame = spark.read.parquet(file_path)
    val columnsDropped = Seq("shotID", "period", "location", "shotType")
    val shotsDF: DataFrame = shotsFullColumns.drop(columnsDropped: _*)
    println(s"Number of rows in data frame: ${shotsDF.count()}")


    printTaskLine("Basic Task 3")
    // ==========================
    val columnsSelected = Seq("season", "game_id", "homeTeamCode", "awayTeamCode", "isPlayOffGame", "homeGoals", "awayGoals", "lastGoalTime")

    val gamesShotOrderedDF = shotsDF.orderBy("time")
      .groupBy("game_id", "season", "homeTeamCode", "awayTeamCode", "isPlayOffGame")

    val gamesDF: DataFrame = gamesShotOrderedDF
      .agg(
          count(when((col("event") === "GOAL") && (col("team") === "HOME"), 1)).alias("homeGoals"),
          count(when((col("event") === "GOAL") && (col("team") === "AWAY"), 1)).alias("awayGoals"),
          max(when(col("event") === "GOAL", col("time"))).alias("lastGoalTime")
      )
      .na.fill(0)
      .selectExpr(columnsSelected: _*)
      .distinct()

    gamesDF.show(5)



    printTaskLine("Basic Task 4")
    // ==========================
    val playoffDF: DataFrame = gamesDF.filter(col("isPlayOffGame") === 1)
      .withColumn("homeTeamWon", when(col("homeGoals") > col("awayGoals"), 1).otherwise(0))
      .withColumn("awayTeamWon", when(col("homeGoals") < col("awayGoals"), 1).otherwise(0))
      .groupBy("season", "homeTeamCode")
      .agg(
          countDistinct("game_id").alias("games"),
          sum("homeTeamWon").alias("wins"),
          sum("awayTeamWon").alias("losses")
      )
      .withColumnRenamed("homeTeamCode", "teamCode")
      .union(
          gamesDF.filter(col("isPlayOffGame") === 1)
            .withColumn("homeTeamWon", when(col("homeGoals") > col("awayGoals"), 1).otherwise(0))
            .withColumn("awayTeamWon", when(col("homeGoals") < col("awayGoals"), 1).otherwise(0))
            .groupBy("season", "awayTeamCode")
            .agg(
                countDistinct("game_id").alias("games"),
                sum("awayTeamWon").alias("wins"),
                sum("homeTeamWon").alias("losses")
            )
            .withColumnRenamed("awayTeamCode", "teamCode")
      )
      .groupBy("season", "teamCode")
      .agg(
          sum("games").alias("games"),
          sum("wins").alias("wins"),
          sum("losses").alias("losses")
      )
      .orderBy("season", "teamCode")

    playoffDF.show()



    printTaskLine("Basic Task 5")
    // ==========================
    val windowSpec = Window.partitionBy("season").orderBy(desc("wins"), desc("losses"))
    val bestPlayoffTeams = playoffDF.withColumn("rank", rank().over(windowSpec))
      .filter(col("rank") === 1)
      .drop("rank")
      .orderBy("season")

    bestPlayoffTeams.show()

    val bestPlayoffTeam2022 = bestPlayoffTeams.filter(col("season") === 2022).first()

    println("Best playoff team in 2022:")
    println(s"    Team: ${bestPlayoffTeam2022.getAs[String]("teamCode")}")
    println(s"    Games: ${bestPlayoffTeam2022.getAs[Long]("games")}")
    println(s"    Wins: ${bestPlayoffTeam2022.getAs[Long]("wins")}")
    println(s"    Losses: ${bestPlayoffTeam2022.getAs[Long]("losses")}")
    println("=========================================================")



    printTaskLine("Basic Task 6")
    // ==========================
    val regularSeasonDF: DataFrame = gamesDF.filter(col("isPlayOffGame") === 0)
      .withColumn("homeTeamWon", when(col("homeGoals") > col("awayGoals"), 1).otherwise(0))
      .withColumn("awayTeamWon", when(col("homeGoals") < col("awayGoals"), 1)
        .when(col("homeGoals") === col("awayGoals"), 1)
        .otherwise(0))
      .withColumn("points", when(col("homeGoals") > col("awayGoals") && col("lastGoalTime") / 60 < 60, 3)
        .when(col("homeGoals") > col("awayGoals") && col("lastGoalTime") / 60 > 60, 2)
        .when(col("homeGoals") < col("awayGoals") && col("lastGoalTime") / 60 > 60, 1)
        .when(col("homeGoals") === col("awayGoals"), 1)
        .otherwise(0))
      .groupBy("season", "homeTeamCode")
      .agg(
          countDistinct("game_id").alias("games"),
          sum("homeTeamWon").alias("wins"),
          sum("awayTeamWon").alias("losses"),
          sum("homeGoals").alias("goalsScored"),
          sum("awayGoals").alias("goalsConceded"),
          sum("points").alias("points")
      )
      .withColumnRenamed("homeTeamCode", "teamCode")
      .union(
          gamesDF.filter(col("isPlayOffGame") === 0)
            .withColumn("homeTeamWon", when(col("homeGoals") > col("awayGoals"), 1)
              .when(col("homeGoals") === col("awayGoals"), 1)
              .otherwise(0))
            .withColumn("awayTeamWon", when(col("homeGoals") < col("awayGoals"), 1).otherwise(0))
            .withColumn("points", when(col("homeGoals") < col("awayGoals") && col("lastGoalTime") / 60 < 60, 3)
              .when(col("homeGoals") < col("awayGoals") && col("lastGoalTime") / 60 > 60, 2)
              .when(col("homeGoals") > col("awayGoals") && col("lastGoalTime") / 60 > 60, 1)
              .when(col("homeGoals") === col("awayGoals"), 1)
              .otherwise(0))
            .groupBy("season", "awayTeamCode")
            .agg(
                countDistinct("game_id").alias("games"),
                sum("awayTeamWon").alias("wins"),
                sum("homeTeamWon").alias("losses"),
                sum("awayGoals").alias("goalsScored"),
                sum("homeGoals").alias("goalsConceded"),
                sum("points").alias("points")
            )
            .withColumnRenamed("awayTeamCode", "teamCode")
      )
      .groupBy("season", "teamCode")
      .agg(
          sum("games").alias("games"),
          sum("wins").alias("wins"),
          sum("losses").alias("losses"),
          sum("goalsScored").alias("goalsScored"),
          sum("goalsConceded").alias("goalsConceded"),
          sum("points").alias("points")
      )
      .orderBy("season", "teamCode")

    regularSeasonDF.show()



    printTaskLine("Basic Task 7")
    // ==========================
    val


    windowSpec_1 = Window.partitionBy("season").orderBy(asc("points"))
    val worstRegularTeams: DataFrame = regularSeasonDF.withColumn("rank", rank().over(windowSpec_1))
      .filter(col("rank") === 1)
      .drop("rank")
      .orderBy("season")

    worstRegularTeams.show()

    worstRegularTeams.show()

    val worstRegularTeam2022: Row = worstRegularTeams.filter(col("season") === 2022).first()

    println("Worst regular season team in 2022:")
    println(s"    Team: ${worstRegularTeam2022.getAs[String]("teamCode")}")
    println(s"    Games: ${worstRegularTeam2022.getAs[Long]("games")}")
    println(s"    Wins: ${worstRegularTeam2022.getAs[Long]("wins")}")
    println(s"    Losses: ${worstRegularTeam2022.getAs[Long]("losses")}")
    println(s"    Goals scored: ${worstRegularTeam2022.getAs[Long]("goalsScored")}")
    println(s"    Goals conceded: ${worstRegularTeam2022.getAs[Long]("goalsConceded")}")
    println(s"    Points: ${worstRegularTeam2022.getAs[Long]("points")}")



    printTaskLine("Additional Task 1")
    // ===============================
    val myManualSchema = StructType(Array(
        StructField("Rank", IntegerType, nullable = true),
        StructField("Name", StringType, nullable = true),
        StructField("Platform", StringType, nullable = true),
        StructField("Year", StringType, nullable = true),
        StructField("Genre", StringType, nullable = true),
        StructField("Publisher", StringType, nullable = true),
        StructField("NA_Sales", DoubleType, nullable = true),
        StructField("EU_Sales", DoubleType, nullable = true),
        StructField("JP_Sales", DoubleType, nullable = true),
        StructField("Other_Sales", DoubleType, nullable = true),
        StructField("Global_Sales", DoubleType, nullable = true)
    ))

    val videoGameDataFrame_1: DataFrame = spark.read
      .schema(myManualSchema)
      .csv("data/sales/video_game_sales.csv")

    // Print the schema of the DataFrame
    videoGameDataFrame_1.printSchema()
    val cachedPlayOffGamesDF = gamesDF.filter(col("isPlayOffGame") === 1).cache()

    val playoffDF_1: DataFrame = cachedPlayOffGamesDF
      .withColumn("homeTeamWon", when(col("homeGoals") > col("awayGoals"), 1).otherwise(0))
      .withColumn("awayTeamWon", when(col("homeGoals") < col("awayGoals"), 1).otherwise(0))
      .groupBy("season", "homeTeamCode")
      .agg(
          countDistinct("game_id").alias("games"),
          sum("homeTeamWon").alias("wins"),
          sum("awayTeamWon").alias("losses")
      )
      .withColumnRenamed("homeTeamCode", "teamCode")
      .union(
          cachedPlayOffGamesDF
            .withColumn("homeTeamWon", when(col("homeGoals") > col("awayGoals"), 1).otherwise(0))
            .withColumn("awayTeamWon", when(col("homeGoals") < col("awayGoals"), 1).otherwise(0))
            .groupBy("season", "awayTeamCode")
            .agg(
                countDistinct("game_id").alias("games"),
                sum("awayTeamWon").alias("wins"),
                sum("homeTeamWon").alias("losses")
            )
            .withColumnRenamed("awayTeamCode", "teamCode")
      )
      .groupBy("season", "teamCode")
      .agg(
          sum("games").alias("games"),
          sum("wins").alias("wins"),
          sum("losses").alias("losses")
      )
      .orderBy("season", "teamCode")
    cachedPlayOffGamesDF.unpersist()
    playoffDF_1.show()
    val cachedGamesDF = gamesDF.filter(col("isPlayOffGame") === 0).cache()

    val regularSeasonDF_1: DataFrame = cachedGamesDF
      .withColumn("homeTeamWon", when(col("homeGoals") > col("awayGoals"), 1).otherwise(0))
      .withColumn("awayTeamWon", when(col("homeGoals") < col("awayGoals"), 1)
        .when(col("homeGoals") === col("awayGoals"), 1)
        .otherwise(0))
      .withColumn("points", when(col("homeGoals") > col("awayGoals") && col("lastGoalTime") / 60 < 60, 3)
        .when(col("homeGoals") > col("awayGoals") && col("lastGoalTime") / 60 > 60, 2)
        .when(col("homeGoals") < col("awayGoals") && col("lastGoalTime") / 60 > 60, 1)
        .when(col("homeGoals") === col("awayGoals"), 1)
        .otherwise(0))
      .groupBy("season", "homeTeamCode")
      .agg(
          countDistinct("game_id").alias("games"),
          sum("homeTeamWon").alias("wins"),
          sum("awayTeamWon").alias("losses"),
          sum("homeGoals").alias("goalsScored"),
          sum("awayGoals").alias("goalsConceded"),
          sum("points").alias("points")
      )
      .withColumnRenamed("homeTeamCode", "teamCode")
      .union(
          cachedGamesDF.filter(col("isPlayOffGame") === 0)
            .withColumn("homeTeamWon", when(col("homeGoals") > col("awayGoals"), 1)
              .when(col("homeGoals") === col("awayGoals"), 1)
              .otherwise(0))
            .withColumn("awayTeamWon", when(col("homeGoals") < col("awayGoals"), 1).otherwise(0))
            .withColumn("points", when(col("homeGoals") < col("awayGoals") && col("lastGoalTime") / 60 < 60, 3)
              .when(col("homeGoals") < col("awayGoals") && col("lastGoalTime") / 60 > 60, 2)
              .when(col("homeGoals") > col("awayGoals") && col("lastGoalTime") / 60 > 60, 1)
              .when(col("homeGoals") === col("awayGoals"), 1)
              .otherwise(0))
            .groupBy("season", "awayTeamCode")
            .agg(
                countDistinct("game_id").alias("games"),
                sum("awayTeamWon").alias("wins"),
                sum("homeTeamWon").alias("losses"),
                sum("awayGoals").alias("goalsScored"),
                sum("homeGoals").alias("goalsConceded"),
                sum("points").alias("points")
            )
            .withColumnRenamed("awayTeamCode", "teamCode")
      )
      .groupBy("season", "teamCode")
      .agg(
          sum("games").alias("games"),
          sum("wins").alias("wins"),
          sum("losses").alias("losses"),
          sum("goalsScored").alias("goalsScored"),
          sum("goalsConceded").alias("goalsConceded"),
          sum("points").alias("points")
      )
      .orderBy("season", "teamCode")
    cachedGamesDF.unpersist()

    regularSeasonDF_1.show()

    printTaskLine("Additional Task 2")
    // ===============================
    // some constants that could be useful
    val englishLetters: String = "abcdefghijklmnopqrstuvwxyz"
    val finnishLetters: String = englishLetters + "åäö"
    val whiteSpace: String = " "
    val punctuationMark: Char = '-'
    val twoPunctuationMarks: String = "--"
    val allowedEnglishOneLetterWords: List[String] = List("a", "i")
    val wikiStr: String = "wiki"

    val englishStr: String = "English"
    val finnishStr: String = "Finnish"

    import spark.implicits._
    val commonWordsEn: DataFrame = spark.read.textFile("data/wikipedia/en")
      .flatMap(line => line.split(whiteSpace))
      .map(word => word.toLowerCase.replaceAll("[^a-z-]", ""))
      .map(word => word.replaceAll(s"-$punctuationMark", ""))
      .filter(word => word.nonEmpty && !word.contains(twoPunctuationMarks))
      .filter(word => word.forall(englishLetters.contains(_)))
      .filter(word => word.length > 1 || allowedEnglishOneLetterWords.contains(word))
      .groupBy("value")
      .count()
      .withColumnRenamed("value", "word")
      .sort(desc("count"))
      .limit(10)
      .cache()

    println("The ten most common English words that appear in the English articles:")
    commonWordsEn.show()


    val common5LetterWordsFi: DataFrame = spark.read.textFile("data/wikipedia/fi")
      .flatMap(line => line.split(whiteSpace))
      .map(word => word.toLowerCase.replaceAll("[^a-zåäö-]", ""))
      .filter(word => word.nonEmpty && !word.contains(twoPunctuationMarks))
      .filter(word => word.forall(finnishLetters.contains(_)))
      .filter(word => word.length > 1 && !word.contains(wikiStr))
      .groupBy("value")
      .count()
      .withColumnRenamed("value", "word")
      .filter(length(col("word")) === 5)
      .sort(desc("count"))
      .limit(5)
      .cache()

    println("The five most common 5-letter Finnish words that appear in the Finnish articles:")
    common5LetterWordsFi.show()


    val longestWord: String = spark.read.textFile("data/wikipedia/fi")
      .flatMap(line => line.split(whiteSpace))
      .map(word => word.toLowerCase.replaceAll("[^a-zåäö-]", ""))
      .filter(word => word.nonEmpty && !word.contains(twoPunctuationMarks))
      .filter(word => word.forall(finnishLetters.contains(_)))
      .filter(word => word.length > 1 && !word.contains(wikiStr))
      .groupBy("value")
      .count()
      .filter(col("count") >= 150)
      .sort(length(col("value")).desc)
      .limit(1)
      .first()
      .getAs[String]("value")

    println(s"The longest word appearing at least 150 times is '$longestWord'")


    val averageWordLengths: DataFrame = spark.read.textFile("data/wikipedia/en")
      .flatMap(line => line.split(whiteSpace))
      .map(word => word.toLowerCase.replaceAll("[^a-z-]", ""))
      .filter(word => word.nonEmpty && !word.contains(twoPunctuationMarks))
      .filter(word => word.length > 1 || allowedEnglishOneLetterWords.contains(word))
      .map(word => (word, word.length, englishStr)) // Add language here
      .toDF("word", "length", "language")
      .groupBy("language")
      .agg(
          round(sum(col("length")) / count(col("word")), 2).alias("average_word_length")
      )
      .orderBy("language")
      .cache()
      .union(
          spark.read.textFile("data/wikipedia/fi")
            .flatMap(line => line.split(whiteSpace))
            .map(word => word.toLowerCase.replaceAll("[^a-zåäö-]", ""))
            .filter(word => word.nonEmpty && !word.contains(twoPunctuationMarks))
            .filter(word => word.length > 1 && !word.contains(wikiStr))
            .map(word => (word, word.length, finnishStr)) // Add language here
            .toDF("word", "length", "language")
            .groupBy("language")
            .agg(
                round(sum(col("length")) / count(col("word")), 2).alias("average_word_length")
            )
            .orderBy("language")
      )
      .orderBy(desc("average_word_length"))

    println("The average word lengths:")
    averageWordLengths.show()



    printTaskLine("Additional Task 3")
    // ===============================
    // some helpful constants
    val startK: Int = 7
    val seedValue: Long = 1

    // the building id for Sähkötalo building at Hervanta campus
    val hervantaBuildingId: String = "102363858X"
    val hervantaPostalCode: Int = 33720

    val maxAllowedClusterDistance: Double = 3.0


    // returns the distance between points (lat1, lon1) and (lat2, lon2) in kilometers
    // based on https://community.esri.com/t5/coordinate-reference-systems-blog/distance-on-a-sphere-the-haversine-formula/ba-p/902128
    private def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
        val R: Double = 6378.1  // radius of Earth in kilometers
        val phi1 = scala.math.toRadians(lat1)
        val phi2 = scala.math.toRadians(lat2)
        val deltaPhi = scala.math.toRadians(lat2 - lat1)
        val deltaLambda = scala.math.toRadians(lon2 - lon1)

        val a = scala.math.sin(deltaPhi * deltaPhi / 4.0) +
            scala.math.cos(phi1) * scala.math.cos(phi2) * scala.math.sin(deltaLambda * deltaLambda / 4.0)

        2 * R * scala.math.atan2(scala.math.sqrt(a), scala.math.sqrt(1 - a))
    }

    val buildingsDF = spark.read.parquet("data/buildings.parquet")

    val selectedColumns = Seq("latitude_wgs84", "longitude_wgs84")
    val assembler = new VectorAssembler().setInputCols(selectedColumns.toArray).setOutputCol("features")
    val assembledDF = assembler.transform(buildingsDF)
    val haversineUDF = udf((lat: Double, lon: Double) => haversine(lat, lon, clusterCenter(0), clusterCenter(1)))

    val kMeans = new KMeans().setK(startK).setSeed(seedValue)
    val kMeansModel = kMeans.fit(assembledDF)
    val sahkotaloCluster = kMeansModel.transform(assembledDF)
      .filter(col("building_id") === hervantaBuildingId)
      .select("prediction")
      .first()
      .getInt(0)
    val clusterBuildings = kMeansModel.transform(assembledDF)
      .filter(col("prediction") === sahkotaloCluster)
    val clusterCenter = kMeansModel.clusterCenters(sahkotaloCluster)
    val distancesDF = clusterBuildings.withColumn("distance", haversineUDF(col("latitude_wgs84"), col("longitude_wgs84")))
    val maxDistance = distancesDF.agg(max("distance")).first().getDouble(0)
    private var currentK = startK - 1
    private var currentClusterBuildings = clusterBuildings
    currentClusterBuildings = currentClusterBuildings.drop("prediction")

    while (currentK >= 2 && maxDistance >= maxAllowedClusterDistance) {
        val kMeans = new KMeans().setK(currentK).setSeed(seedValue)
        val kMeansModel = kMeans.fit(currentClusterBuildings)
        val sahkotaloCluster = kMeansModel.transform(currentClusterBuildings)
          .filter(col("building_id") === hervantaBuildingId)
          .select("prediction")
          .first()
          .getInt(0)
        currentClusterBuildings.unpersist()
        val clusterBuildings = kMeansModel.transform(currentClusterBuildings)
          .filter(col("prediction") === sahkotaloCluster)
      currentClusterBuildings = clusterBuildings
        currentClusterBuildings = currentClusterBuildings.drop("prediction")
        currentK -= 1
    }

    val finalCluster: DataFrame = currentClusterBuildings

    val clusterBuildingCount = finalCluster.count()
    val clusterHervantaBuildingCount = finalCluster.filter(col("postal_code") === hervantaPostalCode).count()

    println(s"Buildings in the final cluster: $clusterBuildingCount")
    print(s"Hervanta buildings in the final cluster: $clusterHervantaBuildingCount ")
    println(s"(${scala.math.round(10000.0*clusterHervantaBuildingCount/clusterBuildingCount)/100.0}% of all buildings in the final cluster)")
    println("===========================================================================================")



    // Stop the Spark session
    spark.stop()

    private def printTaskLine(taskName: String): Unit = {
        val equalsLine: String = "=".repeat(taskName.length)
        println(s"$equalsLine\n$taskName\n$equalsLine")
    }
}
