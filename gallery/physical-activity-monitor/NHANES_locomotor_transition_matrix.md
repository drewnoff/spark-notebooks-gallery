#Analyzing physical activity monitor data. Part I. Transition matrix

The inspiration and ideas for this lab were taken
from [Timofey Pyrkov's talk](https://www.youtube.com/watch?v=9DoBLwvvZDA) on Yandex Data Science conference.

In nowdays it's quite easy to collect locomotor activity data using portable physical activity monitors (wearable devices, smarphones). And it's quite interesting to find a way to process this data in such a way that it could tell us something
about our mortality risks and wellness score.

## NHANES dataset

There is publicly available dataset called NHANES which contains
locomotor activity tracks along with various demographic data for thousands of respondents.
To get instruction on how to obtain codebooks and load this dataset refer to `NHANES_data` spark notebook in the same repository.

```Scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val spark = sparkSession
val SEED = 181
```

### Physical activity monitor data

```Scala
val PaxDF = spark.read
  .format("parquet")
  .load("./notebooks/spark-notebooks-gallery/gallery/physical-activity-monitor/data/paxraw.parquet")
  

PaxDF.describe("SEQN", "PAXINTEN", "PAXDAYSAS", "PAXHOUR", "PAXMINUT").show
```

```
+-------+------------------+------------------+------------------+------------------+------------------+
|summary|              SEQN|          PAXINTEN|         PAXDAYSAS|           PAXHOUR|          PAXMINUT|
+-------+------------------+------------------+------------------+------------------+------------------+
|  count|         147124122|         147124122|         147124122|         147124122|         147124122|
|   mean|31278.131119443486|275.77466697813156|3.9989230182117925|11.496694953938281| 29.49969170249322|
| stddev| 5901.679060963683|1908.7407319232314|1.9994503635886995|6.9220845205709685|17.318105453699477|
|    min|             21005|                 0|                 1|                 0|                 0|
|    max|             41474|             32767|                 7|                23|                59|
+-------+------------------+------------------+------------------+------------------+------------------+
```

###Demographics

```Scala
```