package dip23.assignment

// add anything that is required here
import org.apache.spark.sql.DataFrame
import org.apache.spark.sql.Row
import org.apache.spark.sql.SparkSession


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
    // For example, instead of the path "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/sales/video_game_sales.csv"
    // for the Basic Task 1, you should use the path "../data/sales/video_game_sales.csv".
    //
    // Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    //
    // Comment out the additional tasks 2 and 3 if you did not implement them.
    //
    // Don't forget to submit your solutions to Moodle once your group is finished with the assignment.



    printTaskLine("Basic Task 1")
    // ==========================
    val bestEUPublisher: String = ???

    val bestEUPublisherSales: DataFrame = ???


    println(s"The publisher with the highest total video game sales in European Union is: '${bestEUPublisher}'")
    println("Sales data for the publisher:")
    bestEUPublisherSales.show(10)



    printTaskLine("Basic Task 2")
    // ==========================
    val shotsDF: DataFrame = ???



    printTaskLine("Basic Task 3")
    // ==========================
    val gamesDF: DataFrame = ???



    printTaskLine("Basic Task 4")
    // ==========================
    val playoffDF: DataFrame = ???



    printTaskLine("Basic Task 5")
    // ==========================
    val bestPlayoffTeams: DataFrame = ???

    bestPlayoffTeams.show()

    val bestPlayoffTeam2022: Row = ???

    println("Best playoff team in 2022:")
    println(s"    Team: ${bestPlayoffTeam2022.getAs[String]("teamCode")}")
    println(s"    Games: ${bestPlayoffTeam2022.getAs[Long]("games")}")
    println(s"    Wins: ${bestPlayoffTeam2022.getAs[Long]("wins")}")
    println(s"    Losses: ${bestPlayoffTeam2022.getAs[Long]("losses")}")
    println("=========================================================")



    printTaskLine("Basic Task 6")
    // ==========================
    val regularSeasonDF: DataFrame = ???



    printTaskLine("Basic Task 7")
    // ==========================
    val worstRegularTeams: DataFrame = ???

    worstRegularTeams.show()

    val worstRegularTeam2022: Row = ???

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


    val commonWordsEn: DataFrame = ???

    println("The ten most common English words that appear in the English articles:")
    commonWordsEn.show()


    val common5LetterWordsFi: DataFrame = ???

    println("The five most common 5-letter Finnish words that appear in the Finnish articles:")
    common5LetterWordsFi.show()


    val longestWord: String = ???

    println(s"The longest word appearing at least 150 times is '${longestWord}'")


    val averageWordLengths: DataFrame = ???

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
    def haversine(lat1: Double, lon1: Double, lat2: Double, lon2: Double): Double = {
        val R: Double = 6378.1  // radius of Earth in kilometers
        val phi1 = scala.math.toRadians(lat1)
        val phi2 = scala.math.toRadians(lat2)
        val deltaPhi = scala.math.toRadians(lat2 - lat1)
        val deltaLambda = scala.math.toRadians(lon2 - lon1)

        val a = scala.math.sin(deltaPhi * deltaPhi / 4.0) +
            scala.math.cos(phi1) * scala.math.cos(phi2) * scala.math.sin(deltaLambda * deltaLambda / 4.0)

        2 * R * scala.math.atan2(scala.math.sqrt(a), scala.math.sqrt(1 - a))
    }


    val finalCluster: DataFrame = ???

    val clusterBuildingCount: Long = ???
    val clusterHervantaBuildingCount: Long = ???

    println(s"Buildings in the final cluster: ${clusterBuildingCount}")
    print(s"Hervanta buildings in the final cluster: ${clusterHervantaBuildingCount} ")
    println(s"(${scala.math.round(10000.0*clusterHervantaBuildingCount/clusterBuildingCount)/100.0}% of all buildings in the final cluster)")
    println("===========================================================================================")



    // Stop the Spark session
    spark.stop()

    def printTaskLine(taskName: String): Unit = {
        val equalsLine: String = "=".repeat(taskName.length)
        println(s"${equalsLine}\n${taskName}\n${equalsLine}")
    }
}
