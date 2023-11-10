"""Assignment for Data-Intensive Programming"""

import math
from typing import List

# add anything that is required here
from pyspark.sql import DataFrame
from pyspark.sql import Row
from pyspark.sql import SparkSession


def main():
    # Create the Spark session
    spark: SparkSession = SparkSession.builder \
                                      .appName("assignment") \
                                      .config("spark.driver.host", "localhost") \
                                      .master("local") \
                                      .getOrCreate()

    # suppress informational log messages related to the inner working of Spark
    spark.sparkContext.setLogLevel("ERROR")



    # COMP.CS.320 Data-Intensive Programming, Assignment
    #
    # The instructions for the tasks in the assignment are given in
    # the markdown file "Assignment-tasks.md" at the root of the repository.
    # This file contains only the given starting code not the instructions.
    #
    # The tasks that can be done in either Scala or Python.
    # This is the Scala version intended for local development.
    #
    # For the local development the source data for the tasks can be located in the data folder of the repository.
    # For example, instead of the path "abfss://shared@tunics320f2023gen2.dfs.core.windows.net/assignment/sales/video_game_sales.csv"
    # for the Basic Task 1, you should use the path "../data/sales/video_game_sales.csv".
    #
    # Each task is separated by the printTaskLine() function. Add your solutions to replace the question marks.
    #
    # Comment out the additional tasks 2 and 3 if you did not implement them.
    #
    # Don't forget to submit your solutions to Moodle once your group is finished with the assignment.



    printTaskLine("Basic Task 1")
    # ==========================
    bestEUPublisher: str = __MISSING_IMPLEMENTATION__

    bestEUPublisherSales: DataFrame = __MISSING_IMPLEMENTATION__

    print(f"The publisher with the highest total video game sales in European Union is: '{bestEUPublisher}'")
    print("Sales data for the publisher:")
    bestEUPublisherSales.show(10)



    printTaskLine("Basic Task 2")
    # ==========================
    shotsDF: DataFrame = __MISSING_IMPLEMENTATION__



    printTaskLine("Basic Task 3")
    # ==========================
    gamesDF: DataFrame = __MISSING_IMPLEMENTATION__



    printTaskLine("Basic Task 4")
    # ==========================
    playoffDF: DataFrame = __MISSING_IMPLEMENTATION__



    printTaskLine("Basic Task 5")
    # ==========================
    bestPlayoffTeams: DataFrame = __MISSING_IMPLEMENTATION__

    bestPlayoffTeams.show()


    bestPlayoffTeam2022: Row = __MISSING_IMPLEMENTATION__

    bestPlayoffTeam2022Dict: dict = bestPlayoffTeam2022.asDict()
    print("Best playoff team in 2022:")
    print(f"    Team: {bestPlayoffTeam2022Dict.get('teamCode')}")
    print(f"    Games: {bestPlayoffTeam2022Dict.get('games')}")
    print(f"    Wins: {bestPlayoffTeam2022Dict.get('wins')}")
    print(f"    Losses: {bestPlayoffTeam2022Dict.get('losses')}")
    print("=========================================================")



    printTaskLine("Basic Task 6")
    # ==========================
    regularSeasonDF: DataFrame = __MISSING_IMPLEMENTATION__



    printTaskLine("Basic Task 7")
    # ==========================
    worstRegularTeams: DataFrame = __MISSING_IMPLEMENTATION__

    worstRegularTeams.show()


    worstRegularTeam2022: Row = __MISSING_IMPLEMENTATION__

    worstRegularTeam2022Dict: dict = worstRegularTeam2022.asDict()
    print("Worst regular season team in 2022:")
    print(f"    Team: {worstRegularTeam2022Dict.get('teamCode')}")
    print(f"    Games: {worstRegularTeam2022Dict.get('games')}")
    print(f"    Wins: {worstRegularTeam2022Dict.get('wins')}")
    print(f"    Losses: {worstRegularTeam2022Dict.get('losses')}")
    print(f"    Goals scored: {worstRegularTeam2022Dict.get('goalsScored')}")
    print(f"    Goals conceded: {worstRegularTeam2022Dict.get('goalsConceded')}")
    print(f"    Points: {worstRegularTeam2022Dict.get('points')}")



    printTaskLine("Additional Task 1")
    # ================================



    printTaskLine("Additional Task 2")
    # ================================
    # some constants that could be useful
    englishLetters: str = "abcdefghijklmnopqrstuvwxyz"
    finnishLetters: str = englishLetters + "åäö"
    whiteSpace: str = " "
    punctuationMark: str = '-'
    twoPunctuationMarks: str = "--"
    allowedEnglishOneLetterWords: List[str] = ["a", "i"]
    wikiStr: str = "wiki"

    englishStr: str = "English"
    finnishStr: str = "Finnish"


    commonWordsEn: DataFrame = __MISSING_IMPLEMENTATION__

    print("The ten most common English words that appear in the English articles:")
    commonWordsEn.show()


    common5LetterWordsFi: DataFrame = __MISSING_IMPLEMENTATION__

    print("The five most common 5-letter Finnish words that appear in the Finnish articles:")
    common5LetterWordsFi.show()


    longestWord: str = __MISSING_IMPLEMENTATION__

    print(f"The longest word appearing at least 150 times is '{longestWord}'")


    averageWordLengths: DataFrame = __MISSING_IMPLEMENTATION__

    print("The average word lengths:")
    averageWordLengths.show()



    printTaskLine("Additional Task 3")
    # ================================
    # some helpful constants
    startK: int = 7
    seedValue: int = 1

    # the building id for Sähkötalo building at Hervanta campus
    hervantaBuildingId: str = "102363858X"
    hervantaPostalCode: int = 33720

    maxAllowedClusterDistance: float = 3.0

    # returns the distance between points (lat1, lon1) and (lat2, lon2) in kilometers
    # based on https://community.esri.com/t5/coordinate-reference-systems-blog/distance-on-a-sphere-the-haversine-formula/ba-p/902128
    def haversine(lat1: float, lon1: float, lat2: float, lon2: float) -> float:
        R: float = 6378.1  # radius of Earth in kilometers
        phi1 = math.radians(lat1)
        phi2 = math.radians(lat2)
        deltaPhi = math.radians(lat2 - lat1)
        deltaLambda = math.radians(lon2 - lon1)

        a = (
            math.sin(deltaPhi * deltaPhi / 4.0) +
            math.cos(phi1) * math.cos(phi2) * math.sin(deltaLambda * deltaLambda / 4.0)
        )

        return 2 * R * math.atan2(math.sqrt(a), math.sqrt(1 - a))


    finalCluster: DataFrame = __MISSING_IMPLEMENTATION__

    clusterBuildingCount: int = __MISSING_IMPLEMENTATION__
    clusterHervantaBuildingCount: int = __MISSING_IMPLEMENTATION__

    print(f"Buildings in the final cluster: {clusterBuildingCount}")
    print(f"Hervanta buildings in the final cluster: {clusterHervantaBuildingCount} ", end="")
    print(f"({round(100*clusterHervantaBuildingCount/clusterBuildingCount, 2)}% of all buildings in the final cluster)")
    print("===========================================================================================")



    # Stop the Spark session
    spark.stop()


# Helper function to separate the task outputs from each other
def printTaskLine(taskName: str) -> None:
    equals_line: str = "=" * len(taskName)
    print(f"{equals_line}\n{taskName}\n{equals_line}")


if __name__ == "__main__":
    main()
