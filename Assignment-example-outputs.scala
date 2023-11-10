// Databricks notebook source
// MAGIC %md
// MAGIC # Data-Intensive Programming - Assignment
// MAGIC
// MAGIC ## Example outputs
// MAGIC
// MAGIC This notebook contains some example outputs and additional hints for the assignment tasks.
// MAGIC
// MAGIC Your output from the tasks do not have to match these examples exactly. Instead these are provided to help you confirm that you are on the right track with the tasks.
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 1
// MAGIC
// MAGIC Example output from the task:
// MAGIC - - - - - - - - - - - - - - -
// MAGIC
// MAGIC ```text
// MAGIC The publisher with the highest total video game sales in European Union is: ´Nintendo'
// MAGIC Sales data for the publisher:
// MAGIC +----+--------+------------+
// MAGIC |Year|EU_Total|Global_Total|
// MAGIC +----+--------+------------+
// MAGIC |2000|    6.42|       34.05|
// MAGIC |2001|    8.84|       45.37|
// MAGIC |2002|    9.89|       48.31|
// MAGIC |2003|    7.08|       38.14|
// MAGIC |2004|   12.43|       60.65|
// MAGIC |2005|   42.69|      127.47|
// MAGIC |2006|   60.35|      205.61|
// MAGIC |2007|   32.81|      104.18|
// MAGIC |2008|   25.13|       91.22|
// MAGIC |2009|   36.18|      128.89|
// MAGIC +----+--------+------------+
// MAGIC ```
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 2
// MAGIC
// MAGIC No output is needed from this task.
// MAGIC
// MAGIC Some hints about the `shotsDF` data frame:
// MAGIC
// MAGIC - There should be `1279180` rows in the data frame.
// MAGIC - The source data contains 15 columns but not all of them will be needed in the other basic tasks. Consider dropping those columns that are not needed.
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 3
// MAGIC
// MAGIC No output is needed from this task.
// MAGIC
// MAGIC Some hints about the task:
// MAGIC
// MAGIC - There is data from `15079` games and thus the data frame `gamesDF` should have this many rows.
// MAGIC - It might be easier to handle the games that did not have any goal-scoring shots and those that did have at least one goal-scoring shot separately. The results could then be combined for the final result.
// MAGIC - The source dataset does contain few peculiarities like missing goal-scoring shots or several goal-scoring shots marked for the same second in the same game. These instances are rare and you are allowed to implement your solution without thinking about them. I.e., you are allowed to assume that all source data rows are valid and that there is no missing data.
// MAGIC - Your data frame might have the rows in a different order but at least the following data should be included in `gamesDF`:
// MAGIC
// MAGIC ```text
// MAGIC +------+-------+------------+------------+-------------+---------+---------+------------+
// MAGIC |season|game_id|homeTeamCode|awayTeamCode|isPlayOffGame|homeGoals|awayGoals|lastGoalTime|
// MAGIC +------+-------+------------+------------+-------------+---------+---------+------------+
// MAGIC |  2011|  21188|         L.A|         EDM|            0|        2|        0|        3448|
// MAGIC |  2012|  20023|         TOR|         BUF|            0|        1|        2|        3498|
// MAGIC |  2012|  20425|         COL|         CHI|            0|        2|        5|        3390|
// MAGIC |  2012|  20556|         COL|         DET|            0|        2|        3|        3884|
// MAGIC |  2012|  30116|         NYI|         PIT|            1|        3|        4|        4069|
// MAGIC |  2013|  30236|         MIN|         CHI|            1|        1|        2|        4182|
// MAGIC |  2011|  20749|         BUF|         NYR|            0|        0|        0|           0|
// MAGIC |  2011|  21108|         L.A|         STL|            0|        0|        0|           0|
// MAGIC +------+-------+------------+------------+-------------+---------+---------+------------+
// MAGIC ```
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 4
// MAGIC
// MAGIC No output is needed from this task.
// MAGIC
// MAGIC Some hints about the task:
// MAGIC
// MAGIC - While it is possible to do the task with one chain of operations, it might be helpful to do the task in parts. For example, handling the home teams and away teams separately and combining the results for the final data frame.
// MAGIC - The 2019 playoff season had 24 teams and the other 11 considered seasons had 16 playoff teams each. Thus, the final data frame `playoffDF` should have `200` rows. (11\*16+24)
// MAGIC - Your data frame might have the rows in a different order but at least the following data should be included in `playoffDF`:
// MAGIC
// MAGIC ```text
// MAGIC +------+--------+-----+----+------+
// MAGIC |season|teamCode|games|wins|losses|
// MAGIC +------+--------+-----+----+------+
// MAGIC |  2016|     NSH|   22|  14|     8|
// MAGIC |  2016|     STL|   11|   6|     5|
// MAGIC |  2017|     BOS|   12|   5|     7|
// MAGIC |  2017|     T.B|   17|  11|     6|
// MAGIC |  2011|     NYR|   20|  10|    10|
// MAGIC |  2015|     MIN|    6|   2|     4|
// MAGIC |  2021|     NYR|   20|  10|    10|
// MAGIC +------+--------+-----+----+------+
// MAGIC ```
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 5
// MAGIC
// MAGIC A hint about the task:
// MAGIC
// MAGIC - The window function might be helpful in this task: [https://sparkbyexamples.com/spark/spark-sql-window-functions/](https://sparkbyexamples.com/spark/spark-sql-window-functions/)
// MAGIC
// MAGIC Example output from the task:
// MAGIC - - - - - - - - - - - - - - -
// MAGIC
// MAGIC ```text
// MAGIC +------+--------+-----+----+------+
// MAGIC |season|teamCode|games|wins|losses|
// MAGIC +------+--------+-----+----+------+
// MAGIC |  2011|     L.A|   20|  16|     4|
// MAGIC |  2012|     CHI|   23|  16|     7|   <--- here you might have 15 wins depending on how you counted the goals for the games
// MAGIC |  2013|     L.A|   26|  16|    10|
// MAGIC |  2014|     CHI|   23|  16|     7|
// MAGIC |  2015|     PIT|   24|  16|     8|
// MAGIC |  2016|     PIT|   25|  16|     9|
// MAGIC |  2017|     WSH|   24|  16|     8|
// MAGIC |  2018|     STL|   26|  16|    10|
// MAGIC |  2019|     T.B|   25|  18|     7|
// MAGIC |  2020|     T.B|   23|  16|     7|
// MAGIC |  2021|     COL|   20|  16|     4|
// MAGIC |  2022|     VGK|   22|  16|     6|
// MAGIC +------+--------+-----+----+------+
// MAGIC ```
// MAGIC
// MAGIC and
// MAGIC
// MAGIC ```text
// MAGIC Best playoff team in 2022:
// MAGIC     Team: VGK
// MAGIC     Games: 22
// MAGIC     Wins: 16
// MAGIC     Losses: 6
// MAGIC ```
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 6
// MAGIC
// MAGIC No output is needed from this task.
// MAGIC
// MAGIC Some hints about the task:
// MAGIC
// MAGIC - It might be helpful to do the task in parts. For example, first calculating the points from each game, then calculating whether it was win/loss, and finally doing the aggregation operations.
// MAGIC - The seasons 2011-2016 had 30 teams, seasons 2017-2020 had 31 teams, and seasons 2021-2022 had 32 teams. Thus, the final data frame `regularSeasonDF` should have `368` rows. (6\*30+4\*31+2\*32)
// MAGIC - Your data frame might have the rows in a different order but at least the following data should be included in `regularSeasonDF`:
// MAGIC
// MAGIC ```text
// MAGIC +------+--------+-----+----+------+-----------+-------------+------+
// MAGIC |season|teamCode|games|wins|losses|goalsScored|goalsConceded|points|
// MAGIC +------+--------+-----+----+------+-----------+-------------+------+
// MAGIC |  2021|     CBJ|   82|  33|    49|        258|          297|   103|
// MAGIC |  2021|     CAR|   82|  53|    29|        277|          200|   162|
// MAGIC |  2015|     COL|   82|  35|    47|        212|          240|   111|
// MAGIC |  2022|     NYI|   82|  41|    41|        242|          217|   128|
// MAGIC |  2016|     MIN|   82|  46|    36|        263|          206|   145|
// MAGIC |  2017|     WPG|   82|  48|    34|        273|          216|   153|
// MAGIC |  2022|     CGY|   82|  36|    46|        258|          247|   122|
// MAGIC +------+--------+-----+----+------+-----------+-------------+------+
// MAGIC ```
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Basic Task 7
// MAGIC
// MAGIC Example output from the task:
// MAGIC - - - - - - - - - - - - - - -
// MAGIC
// MAGIC ```text
// MAGIC +------+--------+-----+----+------+-----------+-------------+------+
// MAGIC |season|teamCode|games|wins|losses|goalsScored|goalsConceded|points|
// MAGIC +------+--------+-----+----+------+-----------+-------------+------+
// MAGIC |  2011|     CBJ|   82|  25|    57|        198|          257|    84|
// MAGIC |  2012|     FLA|   48|  12|    36|        109|          170|    44|
// MAGIC |  2013|     BUF|   82|  14|    68|        150|          243|    56|
// MAGIC |  2014|     BUF|   82|  15|    67|        153|          269|    60|
// MAGIC |  2015|     TOR|   82|  23|    59|        192|          240|    83|
// MAGIC |  2016|     COL|   82|  21|    61|        165|          276|    61|
// MAGIC |  2017|     BUF|   82|  24|    58|        198|          278|    80|
// MAGIC |  2018|     OTT|   82|  29|    53|        243|          300|    87|
// MAGIC |  2019|     DET|   71|  14|    57|        142|          265|    49|
// MAGIC |  2020|     BUF|   56|  11|    45|        134|          196|    44|
// MAGIC |  2021|     MTL|   82|  19|    63|        218|          317|    68|
// MAGIC |  2022|     ANA|   82|  20|    62|        206|          335|    68|
// MAGIC +------+--------+-----+----+------+-----------+-------------+------+
// MAGIC ```
// MAGIC
// MAGIC and
// MAGIC
// MAGIC ```text
// MAGIC Worst regular season team in 2022:
// MAGIC     Team: ANA
// MAGIC     Games: 82
// MAGIC     Wins: 20
// MAGIC     Losses: 62
// MAGIC     Goals scored: 206
// MAGIC     Goals conceded: 335
// MAGIC     Points: 68
// MAGIC ```
// MAGIC
// MAGIC - - - - - - - - - - - - - - -
// MAGIC A different way of calculating the game results might give the following output for the data frame:
// MAGIC
// MAGIC ```text
// MAGIC +------+--------+-----+----+------+-----------+-------------+------+
// MAGIC |season|teamCode|games|wins|losses|goalsScored|goalsConceded|points|
// MAGIC +------+--------+-----+----+------+-----------+-------------+------+
// MAGIC |  2011|     CBJ|   82|  25|    57|        197|          257|    84|
// MAGIC |  2012|     FLA|   48|  12|    36|        107|          170|    44|
// MAGIC |  2013|     BUF|   82|  14|    68|        149|          242|    56|
// MAGIC |  2014|     BUF|   82|  15|    67|        153|          268|    60|
// MAGIC |  2015|     TOR|   82|  23|    59|        192|          240|    83|
// MAGIC |  2016|     COL|   82|  21|    61|        165|          276|    61|
// MAGIC |  2017|     BUF|   82|  24|    58|        198|          277|    80|
// MAGIC |  2018|     OTT|   82|  29|    53|        243|          299|    88|
// MAGIC |  2019|     DET|   71|  14|    57|        142|          265|    49|
// MAGIC |  2020|     BUF|   56|  11|    45|        133|          196|    44|
// MAGIC |  2021|     MTL|   82|  19|    63|        216|          316|    68|
// MAGIC |  2022|     ANA|   82|  20|    62|        205|          335|    68|
// MAGIC +------+--------+-----+----+------+-----------+-------------+------+
// MAGIC ```
// MAGIC
// MAGIC And some other variations could also be possible.
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Task 1
// MAGIC
// MAGIC No special hints for this task other than those that have already been given in the task instructions.
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Task 2
// MAGIC
// MAGIC Some hints about the task:
// MAGIC
// MAGIC - There should be `728103` lines in the raw text collection of the English articles.
// MAGIC - There should be `714745` lines in the raw text collection of the Finnish articles.
// MAGIC - Your numbers might not be exactly the same as given below but they should give an indication on whether you have understood the word cleaning task in the way the task creator intended:
// MAGIC     - A total of `2423149` words were found from after cleaning from the English articles. From these `114698` were distinct words.
// MAGIC     - A total of `1394751` words were found from after cleaning from the Finnish articles. From these `241146` were distinct words.
// MAGIC     - The number of distinct words indicate that a lot "made-up" words are included in the count. However, in this task any further cleaning is not necessary.
// MAGIC
// MAGIC
// MAGIC Example output from the task:
// MAGIC - - - - - - - - - - - - - - -
// MAGIC
// MAGIC ```text
// MAGIC The ten most common English words that appear in the English articles:
// MAGIC +----+------+
// MAGIC |word| count|
// MAGIC +----+------+
// MAGIC | the|161631|
// MAGIC |  of| 85770|
// MAGIC | and| 68132|
// MAGIC |  in| 62938|
// MAGIC |  to| 48409|
// MAGIC |   a| 43486|
// MAGIC | was| 22860|
// MAGIC |  by| 18914|
// MAGIC |  as| 18684|
// MAGIC | for| 17182|
// MAGIC +----+------+
// MAGIC ```
// MAGIC
// MAGIC ```text
// MAGIC The five most common 5-letter Finnish words that appear in the Finnish articles:
// MAGIC +-----+-----+
// MAGIC | word|count|
// MAGIC +-----+-----+
// MAGIC |mutta| 3567|
// MAGIC |hänen| 2054|
// MAGIC |jonka| 1873|
// MAGIC |jossa| 1635|
// MAGIC |ollut| 1614|
// MAGIC +-----+-----+
// MAGIC ```
// MAGIC
// MAGIC ```text
// MAGIC The longest word appearing at least 150 times is 'yhdysvaltalainen'
// MAGIC The average word lengths:
// MAGIC +--------+-------------------+
// MAGIC |language|average_word_length|
// MAGIC +--------+-------------------+
// MAGIC | Finnish|               7.85|
// MAGIC | English|               5.26|
// MAGIC +--------+-------------------+
// MAGIC ```
// MAGIC

// COMMAND ----------

// MAGIC %md
// MAGIC ## Additional Task 3
// MAGIC
// MAGIC Example output from the task:
// MAGIC - - - - - - - - - - - - - - -
// MAGIC
// MAGIC The output from the iterations is not asked for in the task instructions but is given here to give indication whether your solution is going in the right direction. These values are created using the seed value 1 for the K-Means algorithm.
// MAGIC
// MAGIC ```text
// MAGIC (k=7, iteration=1) Buildings: 343556 -> 84990, Maximum distance to the center withing 'Sähkötalo' cluster: 82.68 km
// MAGIC (k=6, iteration=2) Buildings: 84990 -> 25299, Maximum distance to the center withing 'Sähkötalo' cluster: 28.03 km
// MAGIC (k=5, iteration=3) Buildings: 25299 -> 10581, Maximum distance to the center withing 'Sähkötalo' cluster: 13.61 km
// MAGIC (k=4, iteration=4) Buildings: 10581 -> 2304, Maximum distance to the center withing 'Sähkötalo' cluster: 10.94 km
// MAGIC (k=3, iteration=5) Buildings: 2304 -> 809, Maximum distance to the center withing 'Sähkötalo' cluster: 9.09 km
// MAGIC (k=2, iteration=6) Buildings: 809 -> 757, Maximum distance to the center withing 'Sähkötalo' cluster: 2.75 km
// MAGIC Buildings in the final cluster: 757
// MAGIC Hervanta buildings in the final cluster: 669 (88.38% of all buildings in the final cluster)
// MAGIC ```
// MAGIC
