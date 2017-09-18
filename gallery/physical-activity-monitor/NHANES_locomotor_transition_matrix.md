# Analyzing physical activity monitor data. Part I. Transition matrix

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
val DemoDF = spark.read
  .format("parquet")
  .load("./notebooks/spark-notebooks-gallery/gallery/physical-activity-monitor/data/demo.parquet")

DemoDF.limit(3).show
```

## Cleaning physical activity monitor data

The physical activity monitors (PAMs) used in NHANES were programmed to detect and record the magnitude of acceleration 
or “intensity” of movement. Intensity readings were summed over each 1-minute epoch.

The dataset has some abnormally high "intensity" values stored by several devices. We can plot intensity value distribution.

```Scala
CustomPlotlyChart(PaxDF.where($"PAXINTEN" > 1000).sample(withReplacement=false, 0.05),
                  layout="""{title: 'Intencity value distribution', 
                             yaxis: {type: 'log'},
                             xaxis: {title: 'Intensity'},
                             bargap: 0.02}""",
                  dataOptions="{type: 'histogram', opacity: 0.7}",
                  dataSources="{x: 'PAXINTEN'}",
                  maxPoints=5000)
```

<img src="http://telegra.ph/file/0f8b28c43edc71a7146f4.png" width=900>
</img>

Count the number of devices which recorded abnormally high intensity values

```Scala
PaxDF.where($"PAXINTEN" > 27000).select($"SEQN").distinct.count
```

```
449
```

Let's remove those devices from the dataset.

We will create a broadcasted variable containing a set of Respondent sequence numbers (`SEQN`) with abnormally high intensity values.

```Scala
val broadcastedBlackList = spark.sparkContext.broadcast(
  PaxDF.where($"PAXINTEN" > 27000).select($"SEQN").distinct
  .collect.map(_(0).asInstanceOf[Int]).toSet
)

def inBlacklistUDF = udf((seqNum: Int) => {
  broadcastedBlackList.value.contains(seqNum)
})

val PaxUnreliable = PaxDF.where(inBlacklistUDF($"SEQN"))

val PaxReliable = PaxDF.where(!inBlacklistUDF($"SEQN"))

println("Number of reliable devices: " + PaxReliable.select($"SEQN").distinct.count)
println("Number of unreliable devices " + PaxUnreliable.select($"SEQN").distinct.count)
```

```
Number of reliable devices: 14182
Number of unreliable devices 449
```

Now that we have respondent sequence numbers for somewhat reliable and unreliable data we can have a look at
raw intensity values over the tracking period. We will make and add synthetic `datetime` column to be able order records more easily and make timeseries charts.

```Scala
val reliableSeqNumSample = PaxReliable.select($"SEQN").distinct
  .sample(false, 0.01, SEED)
  .limit(10)
  .collect
  .map(_(0).asInstanceOf[Int]).toList

val PaxReliableWithDT = PaxReliable
                        .withColumn("datetime", concat($"PAXDAYSAS", lit(".01.2005 "), $"PAXHOUR", lit(":"), $"PAXMINUT"))
                        .withColumn("time", unix_timestamp($"datetime", "d.MM.yyyy HH:mm"))
                        .withColumn("datetime", from_unixtime($"time"))
                        
CustomPlotlyChart(PaxReliableWithDT
                    .where($"SEQN" === reliableSeqNumSample(3))
                    .where($"PAXDAYSAS" > 1 && $"PAXDAYSAS" < 4), // showing only two days
                  layout="""{title: 'Physical activity monitor data', 
                           yaxis: {title: 'Device Intensity Value'},
                           showlegend: false}""",
                  dataOptions="""{
                    colorscale: 'Electric',
                    autocolorscale: true
                  }""",
                  dataSources="""{
                    x: 'datetime',
                    y: 'PAXINTEN'
                  }""",
                 maxPoints=3000)
```

<img src="http://telegra.ph/file/a70c10f9a09ae51447f13.png" width=900>
</img>

This is a locomotor activity data. What can we do with this data? What can it tell us about human health or age?
It's hard to directly compare different locomotor activity tracks because much of individual's social activity habbits are mixed into this data.
But we're interested to extract physiological information from noisy locomotor activity.
For that we need to perform feature engineering.
  
## Transition Matrix

One way to describe locomotor activity track is to apply the model of [Markov process](https://en.wikipedia.org/wiki/Markov_chain) to it
which is described by probabilities of transitions from one state to another. We can define such probabilities in a form of [transition matrix](https://en.wikipedia.org/wiki/Stochastic_matrix).

First, we need to define finite state space of such a locomotor activity process, let's say something like: low, medium, high, very high level of activity.

After that we need to go through a physical activity intensity track of an individual person and count probabilities of transitions from one level of activity to another
for given person. As a result we'll get a locomotor activity transition matrix for a single person which can be treated as a locomotor findgerprint of a person.

### Spark ML Bucketizer

To define finite state space of locomotor activities we can use [Bucketizer](https://spark.apache.org/docs/latest/ml-features.html#bucketizer) transformer from Spark ML library.

We can look at intencity value distribution recorded from reliable trackers to define desired intensity levels.

```Scala
CustomPlotlyChart(PaxReliableWithDT.where($"PAXINTEN" < 18000).sample(withReplacement=false, 0.05),
                  layout="""{title: 'Intencity value distribution recorded from reliable trackers', 
                             yaxis: {type: 'log'},
                             xaxis: {title: 'Intensity'},
                             bargap: 0.02}""",
                  dataOptions="{type: 'histogram', opacity: 0.7}",
                  dataSources="{x: 'PAXINTEN'}",
                  maxPoints=5000)
```

<img src="http://telegra.ph/file/e72c1d2f0261b0b3e74d8.png" width=900>
</img>

```Scala
import org.apache.spark.ml.feature.Bucketizer

val splits = Array(0, 30, 100, 300, 600, 900, 1400, 2000, 3500, 5000, Double.PositiveInfinity)

val bucketizer = new Bucketizer()
  .setInputCol("PAXINTEN")
  .setOutputCol("activityLevel")
  .setSplits(splits)
  
val bucketedPax = bucketizer
  .transform(PaxReliableWithDT
             .withColumn("totalInten", $"PAXINTEN".cast(LongType))
             .withColumn("PAXINTEN", $"PAXINTEN".cast(DoubleType)))
  .withColumn("activityLevel", $"activityLevel".cast(IntegerType))

bucketedPax.select($"activityLevel").distinct.orderBy($"activityLevel").show
```

```
+-------------+
|activityLevel|
+-------------+
|            0|
|            1|
|            2|
|            3|
|            4|
|            5|
|            6|
|            7|
|            8|
|            9|
+-------------+
```

### Computing Transition Matrix with Spark SQL Window Functions

To compute a transition matrix we need to collect previous minute activity of a given person for each minute of activity for the same person.

That's where Spark SQL Funcitons come in handy.

```Scala
import org.apache.spark.sql.expressions.Window

val windowSpec = Window.partitionBy("SEQN").orderBy("time")

val withLastMinuteDF = bucketedPax
  .select($"SEQN", $"totalInten", $"activityLevel", $"time")
  .withColumn("previousMinuteActivity", lag("activityLevel", 1).over(windowSpec))
  .withColumn("previousMinuteActivity", when(isnull($"previousMinuteActivity"), -1).otherwise($"previousMinuteActivity"))
```

Here we specified a window to contain all records from one Respondent (partitioned by `SEQN`) and ordered by `time`. 

And we're using `lag` window funciton to access previous record in specified window which in this case is a previous minute activity level of a given person.

Now that we have previous minute activity level we can start to build a transition matrix of desired size.
We can store the transition matrix `W` in a form of `Array[Array[Double]]` where `W(i)(j)` has the value of probability of transition from state `j` to state `i`.
 
First we will store in `W(i)(j)` a number of transitions from level `j` to level `i` of a given person and after that we will devide this value by total number of transitions in recorded track for the person.

Let's obtain total number of transitions from one sate to another.

```Scala
def initTransitionMatrix = udf{ (currentActivityLevel: Int, previousActivityLevel: Int, size: Int) => {
  val W = Array.fill(size, size)(0.0)
  if (previousActivityLevel >= 0)
    W.updated(currentActivityLevel, W(currentActivityLevel).updated(previousActivityLevel, 1.0))
  else
    W
  
}}

val dfW = withLastMinuteDF.withColumn("W", initTransitionMatrix($"activityLevel", $"previousMinuteActivity", lit(10)))
```

For each record we created a  matrix with single transition count: from previous minute acitivty level to current one.

```Scala
case class RespondentTrMatrix(seqn: Int, totalInten: Long, totalCount: Long, W: Array[Array[Double]])

val initTrMatrixDS = dfW.select($"SEQN", $"totalInten", lit(1L).as("totalCount"), $"W").as[RespondentTrMatrix]
```

Finally we need to sum all single-transition matrices of a single respondent
and divide the result matrix by total number of transition of the respondent.

```Scala

val sumTrMatrixDS = initTrMatrixDS.rdd
  .map(l => (l.seqn, l))
  .reduceByKey((l, r) => {
    val elementWiseArraySum = (a: Array[Double], b: Array[Double]) => {
      a.zip(b).map { case (x, y) => x + y }
    }
    val elementWiseMatrixSum = (c: Array[Array[Double]], d: Array[Array[Double]]) => {
      c.zip(d).map { case (x, y) => elementWiseArraySum(x, y) }
    }
    RespondentTrMatrix(l.seqn, l.totalInten + r.totalInten, l.totalCount + r.totalCount, elementWiseMatrixSum(l.W, r.W)) 
  })
  .map(r => {
    val trMatrix = r._2
    trMatrix.copy(W = trMatrix.W.map(_.map(_ / trMatrix.totalCount)))
  })
  .toDS
  
sumTrMatrixDS.write.format("parquet").mode("overwrite")
.save("./notebooks/spark-notebooks-gallery/gallery/physical-activity-monitor/data/10_inten_tr_matrix.parquet")
```

It's a good idea to persist computed transition matrices.

```Scala
val computedTrMatrixDS = spark.read
  .format("parquet")
  .load("./notebooks/spark-notebooks-gallery/gallery/physical-activity-monitor/data/10_inten_tr_matrix.parquet")
  .as[RespondentTrMatrix]
```

We also obtained respondent cumulative intensity value. Let's take a look at its distribution.

```Scala
CustomPlotlyChart(computedTrMatrixDS.where($"totalInten" < 10e6).toDF,
                  layout="""{title: 'Cumulative intensity value distribution', 
                             xaxis: {title: 'Cumulative intensity value per week'},
                             bargap: 0.02}""",
                  dataOptions="{type: 'histogram', opacity: 0.7}",
                  dataSources="{x: 'totalInten'}",
                  maxPoints=8000)
```

<img src="http://telegra.ph/file/f801e7ec237d705c7fbb4.png" width=900>
</img>

There is a peak in distribution at very low cumulative intensity value. 
Low cumulative intensity value might be obtained when the physical activity monitor has not been used. 
We can filter out these measurements.

```Scala
val trMatrixCleanedDS = computedTrMatrixDS
                        .where($"totalInten" < 1e7 && $"totalInten" > 1e5)

trMatrixCleanedDS.count
```

Now we can vizualise transition matrices for different respondents.

```Scala
val sampleTrMatrices = trMatrixCleanedDS
                      .sample(false, 0.1, SEED).limit(10)
                      .collect
                      .map(_.W)
                      

def plotTrMatrix(trMatrix: Array[Array[Double]]) = {
  val trMatrixPlotData = trMatrix
                          .zipWithIndex.toSeq.toDF("transitions", "toActivityLevel")
                          .withColumn("fromActivityLevel", $"toActivityLevel")
  
  CustomPlotlyChart(trMatrixPlotData,
                  layout="""{title: 'Physical activity Transition matrix',
                             xaxis: {title: 'from physical activity level'}, 
                             yaxis: {title: 'to physical activity level'},
                             width: 600, height: 600}""",
                  dataOptions="""{type: 'heatmap', 
                                  colorscale: 'Viridis',
                                  reversescale: false,
                                  colorbar: {
                                    title: 'Probability',
                                    tickmode: 'array',
                                    tickvals: [0, 0.02, 0.04, 0.06, 0.08, 0.1],
                                    ticktext: ['0', '0.02', '0.04', '0.06', '0.08', '>0.1']
                                  },
                                  zmin: 0.0, zmax: 0.10}""",
                  dataSources="{x: 'fromActivityLevel', y: 'toActivityLevel', z: 'transitions'}")
}
```

```Scala
plotTrMatrix(sampleTrMatrices(0))
```

<img src="http://telegra.ph/file/93523014244b5f8b33750.png" width=600>
</img>

```Scala
plotTrMatrix(sampleTrMatrices(4))
```

<img src="http://telegra.ph/file/432297f2f90c25d253a8f.png" width=600>
</img>


```Scala
plotTrMatrix(sampleTrMatrices(5))
```

<img src="http://telegra.ph/file/94997619943ce60b575fc.png" width=600>
</img>

A transitoin matrix looks like a locomotor fingerprint and we already can say something about a person's activity patterns
from looking into her locomotor transition matrix.

## Aging

We collected and cleaned a lot of data recorded by physical activity monitors and extracted quite interesting feature from raw data.

Interesting to see how does physiological changes encoded in transition matrix relates to the aging process.

For that we'll use Principal component analysis ([PCA](https://spark.apache.org/docs/latest/ml-features.html#pca)).
We'll project high dimentional feature space which is in our case is 10x10=100
(the number of elements in the matrix) of possibly correlated variables
into lower dimentional space of linearly uncorrelated variables called principal components.

We'll use [PCA](https://spark.apache.org/docs/latest/ml-features.html#pca) feature transformaer from Spark ML library. 

```Scala
import org.apache.spark.ml.linalg.{Vector, Vectors}
import org.apache.spark.ml.feature.PCA

def logScaleFeaturesUDF = udf{ (vec: Vector) => Vectors.dense(vec.toArray.map(v => -scala.math.log(1e-7 + v)))}

val flattenTrMatrixDF = computedTrMatrixDS.where($"totalInten" < 1e7 && $"totalInten" > 1e5).rdd
  .map(l => (l.seqn, Vectors.dense(l.W.flatten)))
  .toDF("SEQN", "features")
  .withColumn("logFeatures", logScaleFeaturesUDF($"features"))
  .join(DemoDF, "SEQN")
  .where($"RIDAGEYR" >= 35)
```

Here we flatten the matrix into single feature `Vector` and apply log transformation to the feature vector. 
Also we join age data from demographics dataset. 
And let's consider only respondents older than 35 years (since we're interested in the aging process).

```Scala
val pca = new PCA()
  .setInputCol("logFeatures")
  .setOutputCol("pcaFeatures")
  .setK(3)
  .fit(flattenTrMatrixDF)

val withLocomotorPCA = pca.transform(flattenTrMatrixDF).select("SEQN", "pcaFeatures", "RIDAGEYR")

withLocomotorPCA.limit(3).show(false)
```

Let's extract each principal component as an individual feature 
and compute mean value and standart deviation of each principal component for given age.

```Scala
def getItemUDF = udf{ (vec: Vector, idx: Int) => vec(idx)}

val locomotorPCvsAge = withLocomotorPCA
  .select($"SEQN", $"RIDAGEYR".as("age"),
          getItemUDF($"pcaFeatures", lit(0)).as("PC1"),
          getItemUDF($"pcaFeatures", lit(1)).as("PC2"),
          getItemUDF($"pcaFeatures", lit(2)).as("PC3"))

val matrixPCvsAge = locomotorPCvsAge.groupBy($"age").agg(
  mean($"PC1").as("meanPC1"), stddev($"PC1").as("stdPC1"),
  mean($"PC2").as("meanPC2"), stddev($"PC2").as("stdPC2"),
  mean($"PC3").as("meanPC3"), stddev($"PC3").as("stdPC3")
)

def plotPrincipalComponentVsAge(componentNum: Int) = {
  CustomPlotlyChart(matrixPCvsAge.orderBy($"age"),
                  layout=s"""{title: 'PC$componentNum vs Age', 
                           xaxis: {title: 'Chronological age'},
                           yaxis: {title: 'PC$componentNum'},
                           showlegend: false}""",
                  dataOptions="""{
                    type: 'scatter',
                    line: {width: 2},
                    error_y: {type: 'data', visible: true, thickness: 0.5, width: 0}
                  }""",
                  dataSources=s"""{
                    x: 'age',
                    y: 'meanPC$componentNum',
                    error_y: {array: 'stdPC$componentNum'}
                  }""")
}
```

```Scala
plotPrincipalComponentVsAge(1)
```
<img src="http://telegra.ph/file/2169fc38fc5bee10e4d27.png" width=900>
</img>

```Scala
plotPrincipalComponentVsAge(2)
```

<img src="http://telegra.ph/file/3efe14c4a27f7ee2cf459.png" width=900>
</img>

```Scala
plotPrincipalComponentVsAge(3)
```

<img src="http://telegra.ph/file/48207747f0e639f5d8173.png" width=900>
</img>


PCA analysis finds the largest variance associated with age.
That means that `PC1` of locomotor transition matrix contains information about biological age.

```Scala
CustomPlotlyChart(
  locomotorPCvsAge, 
  layout="""{
        title: 'PCA of the Locomotor Transition Matrix',
        height: 900,
        xaxis: {title: 'PC1'},
        yaxis: {title: 'PC2'},
        hovermode: 'closest'
    }""",
  dataOptions="""{
    mode: 'markers',
    type: 'scatter',
    marker: {
        sizemode: 'area',
        size: 12,
        opacity: 0.75,
        colorscale: 'Jet',
        reversescale: true,
        colorbar: {
          title: 'Age',
          thickness: 8.0
        }
    }}""",
  dataSources="""{
    x: 'PC1',
    y: 'PC2',
    marker: {color: 'age'}}""",
  maxPoints=6000)
```

<img src="http://telegra.ph/file/ea603c53a27d9829cd36b.png" width=900>
</img>

We've obtained pretty strong feature and there are plenty of things we can do with this. For example, we can use it for mortality risks prediction based on locomotor activity.

Let's try to conduct this analysis in the next part of this lab.