# Analyzing time series cryptocurrencies data

This notebook is highly inspired by [@sarahpan](https://blog.timescale.com/@sarahpan) great [article](https://blog.timescale.com/analyzing-ethereum-bitcoin-and-1200-cryptocurrencies-using-postgresql-3958b3662e51) on analysis of cryptocurrencies data using PostgreSQL.
The main goal of this notebook is not to provide you with Ethereum, Bitcoin and other cryptocurrencies insights (for this one please refer to @sarahpan's article) but to get you familar with tools which Apache Spark ecosystem can provide you to perform similar kind of analysis.

In this notebook we will perform time series data analysis using **Apache Spark**, a time series library for Apache Spark called **[Flint](https://github.com/twosigma/flint)** and interactive computaions and visualization capabilities of **Spark Notebook**.

## Data

[Direct link](https://timescaledata.blob.core.windows.net/datasets/crypto_data.tar.gz) to download the dataset. 
Also [here](https://blog.timescale.com/analyzing-ethereum-bitcoin-and-1200-cryptocurrencies-using-postgresql-downloading-the-dataset-a1bbc2d4d992)
one can find detailed description for the dataset.

We are interested in two files:
 - btc_prices.csv — A CSV file with the daily BTC price data (OHLCV format), spanning seven years from 2010 to 2017 across multiple currencies (e.g. USD, CAD, EUR, CNY, etc.)
 - crypto_prices.csv — A CSV file with the daily cryptocurrency price data (OHLCV format) for over 1200 cryptocurrencies since 2013


This is a small dataset and Spark lanched on laptop in local mode should be enough to work with it.

## Requirements

`spark 2.0` or higher, `2.11.7 or higher`. 
We also need to provide custom dependencies on [Flint](https://github.com/twosigma/flint) library. 
It's not published on maven central repository but you can simpliy build it from the source and publish locally by running
```
sbt publishLocal
```

After that provide coordinates for dependencies in `customDeps` section of Notebook metadata (Edit -> Edit Notebook Metadata):

```
"customDeps": [
    "com.twosigma % flint_2.11 % 0.2.0-SNAPSHOT"
  ]
```

## Reading the data

```scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val spark = sparkSession
```

Apache Spark supports direct read from `csv` files and can try to automatically infer schema.
But we also can provide our own schema while reading `csv` data. 
This will prevent from full scan of `csv` file to infer schema and it's also more accurate if we know what we're doing.


```scala
val btcPriceSchema = StructType(
    StructField("datetime", StringType, false) ::
    StructField("opening_price", DoubleType, false) ::
    StructField("highest_price", DoubleType, false) ::
    StructField("lowest_price", DoubleType, false) ::
    StructField("closing_price", DoubleType, false) ::
    StructField("volume_btc", DoubleType, false) ::
    StructField("volume_currency", DoubleType, false) ::
    StructField("currency_code", StringType, false) :: Nil
)

val cryptoPriceSchema = StructType(
    StructField("datetime", StringType, false) ::
    StructField("opening_price", DoubleType, false) ::
    StructField("highest_price", DoubleType, false) ::
    StructField("lowest_price", DoubleType, false) ::
    StructField("closing_price", DoubleType, false) ::
    StructField("volume_crypto", DoubleType, false) ::
    StructField("volume_btc", DoubleType, false) ::
    StructField("currency_code", StringType, false) :: Nil
)

val btcPricesDF = spark.read
  .format("csv")
  .schema(btcPriceSchema)
  .load("/path/to/crypto_data/btc_prices.csv")
  .withColumn("time", unix_timestamp($"datetime", "yyyy-MM-dd HH:mm:ssX"))
  .withColumn("date", from_unixtime($"time", "yyyy-MM-dd"))

val cryptoPricesDF = spark.read
  .format("csv")
  .schema(cryptoPriceSchema)
  .load("/path/to/crypto_data/crypto_prices.csv")
  .withColumn("time", unix_timestamp($"datetime", "MM/dd/yyyy HH:mm"))
  .withColumn("date", from_unixtime($"time", "yyyy-MM-dd"))
```

```scala
btcPricesDF.show(5)
```

```
+--------------------|-------------|-------------|------------|-------------|----------|---------------|-------------|----------|----------+
|            datetime|opening_price|highest_price|lowest_price|closing_price|volume_btc|volume_currency|currency_code|      time|      date|
+--------------------|-------------|-------------|------------|-------------|----------|---------------|-------------|----------|----------+
|2013-03-10 20:00:...|        60.56|        60.56|       60.56|        60.56|    0.1981|           12.0|          AUD|1362960000|2013-03-11|
|2013-03-11 20:00:...|        60.56|        60.56|       41.38|        47.78|     47.11|         2297.5|          AUD|1363046400|2013-03-12|
|2013-03-12 20:00:...|        49.01|        59.14|       46.49|        59.14|     49.64|        2501.39|          AUD|1363132800|2013-03-13|
|2013-03-13 20:00:...|        50.15|        59.14|        49.7|        51.16|     31.37|        1592.73|          AUD|1363219200|2013-03-14|
|2013-03-14 20:00:...|        51.16|        58.82|       50.93|        51.79|     16.79|         919.62|          AUD|1363305600|2013-03-15|
+--------------------|-------------|-------------|------------|-------------|----------|---------------|-------------|----------|----------+
only showing top 5 rows
```

```scala
val btcUSDPricesDF = btcPricesDF.where($"currency_code" === "USD")
```

```
+---------------|-------------|-------------|------------|-------------|-------------|----------|-------------|----------|----------+
|       datetime|opening_price|highest_price|lowest_price|closing_price|volume_crypto|volume_btc|currency_code|      time|      date|
+---------------|-------------|-------------|------------|-------------|-------------|----------|-------------|----------|----------+
|6/26/2017 20:00|       5.7E-7|       5.7E-7|      5.7E-7|       5.7E-7|          0.0|       0.0|          EOC|1498496400|2017-06-26|
|6/26/2017 20:00|       1.6E-7|       1.6E-7|      1.6E-7|       1.6E-7|          0.0|       0.0|        RATIO|1498496400|2017-06-26|
|6/26/2017 20:00|     2.335E-4|      2.43E-4|     2.18E-4|     2.139E-4|     90175.87|     20.64|          CPC|1498496400|2017-06-26|
|6/26/2017 20:00|       1.6E-7|       1.6E-7|      1.6E-7|       1.6E-7|          0.0|       0.0|          RYC|1498496400|2017-06-26|
|6/26/2017 20:00|       4.2E-4|       4.2E-4|      4.2E-4|       4.2E-4|          0.0|       0.0|          XBS|1498496400|2017-06-26|
+---------------|-------------|-------------|------------|-------------|-------------|----------|-------------|----------|----------+
only showing top 5 rows
```

## OHLC charts

Since the data is presented in OHLCV format the natural way to plot them would be using OHLC charts.
Spark Notebook comes with build in support (`CustomPlotlyChart`) for [Plotly javascript API](https://plot.ly/javascript/) for data visualizations.
To get some examples on usage of `CustomPlotlyChart` refer to notebooks from `notebooks/viz` directory which comes with Spark Notebook distribution or explore online with [nbviewer](https://viewer.kensu.io/notebooks/viz/00_Data%20Visualisation%20With%20Plotly.snb).

Resulting Plotly charts are interactive so fill free to zoom and hover over data directly from the Notebook.

```scala
CustomPlotlyChart(btcUSDPricesDF,
                  layout="{title: 'BTC price in USD', showlegend: false}",
                  dataOptions="{type: 'ohlc'}",
                  dataSources="""{
                    x: 'date',
                    close: 'closing_price',
                    high: 'highest_price',
                    low: 'lowest_price',
                    open: 'opening_price'
                  }""",
                 maxPoints=3000)
```

**[click here](https://plot.ly/~drewnoff/42.embed)** to see the interactive chart

<img src="http://telegra.ph/file/06b83f914f30b2fb9e9cc.png" width=800>
</img>


## TimeSeriesRDD

Now let's start with some time series analysis. From Flint getting started guide you can find that the entry point into all functionalities for time series analysis in Flint is the `TimeSeriesRDD` class or object. And we can create one from an existing `DataFrame`, for that we have to make sure the `DataFrame` contains a column named "time" of type LongType.
And that's why we performed that extra `.withColumn("time", unix_timestamp($"datetime", "yyyy-MM-dd HH:mm:ssX"))` steps
to create `btcPricesDF` and `cryptoPricesDF` dataframes.

```scala
import com.twosigma.flint.timeseries.TimeSeriesRDD
import scala.concurrent.duration._

val btcUSDPricesTsRdd = TimeSeriesRDD
  .fromDF(dataFrame = btcUSDPricesDF)(isSorted = true, timeUnit = SECONDS)
```

After creating our `TimeSeriesRDD` we can perform various transformations on it. 
If we want to perform some grouping or aggregation on our time series data then there are several options provided by Flint for that depending on your needs. 
Let's say we want to split our data into `14 day` time buckets and obtain some summary information per each bucket.

### Generating clockTS RDD for time buckets

Time buckets could be defined by another `TimeSeriesRDD`. Its timestamps will be used to defined intervals, i.e. two sequential timestamps define an interval.

```scala
import com.twosigma.flint.timeseries.Summarizers

val timeBin = Duration(14, DAYS).toSeconds.toInt

val minMaxTs = btcUSDPricesDF.select(min($"time"), max($"time")).head

val (minTs, maxTs) = (minMaxTs(0).asInstanceOf[Long],  minMaxTs(1).asInstanceOf[Long])

val clockTs = minTs to maxTs by timeBin

val clockTsRdd = TimeSeriesRDD
  .fromDF(dataFrame = spark.sparkContext.parallelize(clockTs).toDF("time"))(isSorted = true, timeUnit = SECONDS)
```

Now that we defined time intervals we're able to apply available summarizers for each interval.

```scala
val meanClosingPricesByTwoWeeks = btcUSDPricesTsRdd
                                    .summarizeIntervals(clockTsRdd, Summarizers.mean("closing_price"))
```

So we obtained `mean` closing price in USD per each `14 day` interval.

```scala
CustomPlotlyChart(meanClosingPricesByTwoWeeks.toDF.withColumn("date", from_unixtime(($"time" / 1e9), "yyyy-MM-dd")),
                  layout="""{
                    title: 'BTC mean closing price in USD over the last seven years (in 14 day intervals)', 
                    showlegend: false, 
                    yaxis: {title: 'Mean Closing Price (BTC/USD)'}}""",
                  dataOptions="{type: 'scatter'}",
                  dataSources="""{
                    x: 'date',
                    y: 'closing_price_mean'
                  }""")
```

**[click here](https://plot.ly/~drewnoff/44.embed)** to see the interactive chart

<img src="http://telegra.ph/file/257728593553c3fc64f73.png" width=800>
</img>


##  Spark SQL Window Functions

Another useful thing in Apache Spark toolbox is [Spark SQL Window Functions](https://databricks.com/blog/2015/07/15/introducing-window-functions-in-spark-sql.html). So what it can be useful for? It  provides the ability to perform calculations across set of rows like calculating a moving average, calculating a cumulative sum, or accessing the values of a row appearing before the current row.

Let's take an example of calculating the day-by-day volatility of BTC
where we want to calculate BTC daily return as a factor of the previous day’s rate.

For that we can use `DataFrame` API or use `SQL` expressions by registering temporary view of our dataframe.

```scala
btcUSDPricesDF.createOrReplaceTempView("btc_usd_prices")

val dailyBTCreturnsDF = spark.sql("""
SELECT time,
       closing_price / lead(closing_price) over prices AS daily_factor
FROM (
   SELECT time,
          closing_price
   FROM btc_usd_prices
   GROUP BY 1,2
) sub window prices AS (ORDER BY time DESC)
""")
```

```scala
CustomPlotlyChart(dailyBTCreturnsDF.withColumn("date", from_unixtime($"time", "yyyy-MM-dd")),
                  layout="""{
                    title: 'BTC daily return (as a factor of the previous day’s rate) over the last seven years', 
                    showlegend: false, 
                    yaxis: {title: 'Daily Return (BTC/USD)', type: 'log'}}""",
                  dataOptions="{type: 'scatter', line: {width: 1}}",
                  dataSources="""{
                    x: 'date',
                    y: 'daily_factor'
                  }""",
                 maxPoints=3000)
```

**[click here](https://plot.ly/~drewnoff/46.embed)** to see the interactive chart

<img src="http://telegra.ph/file/8157ad35d44fe2cd04dbb.png" width=800>
</img>

## Volumes by currency

Now let's get back to `TimeSeriesRDD` and refresh our knowledge on time seriese data summarization
with one more example.
Let's say we want to track changes in volume of BTC in different fiat currencies in 14 day intervals.
For that we can use already seen `.summarizeIntervals` method with additional `key` argument to group results by `currency_code`.

```scala
val btcPricesTsRdd = TimeSeriesRDD.fromDF(dataFrame = btcPricesDF)(isSorted = false, timeUnit = SECONDS)

val btcVolumeByCurrencyByInterval = btcPricesTsRdd.summarizeIntervals(clockTsRdd, 
                                                                      Summarizers.sum("volume_btc"),
                                                                      key=Seq("currency_code"))

val btcVolumeByCurrencyDF = btcVolumeByCurrencyByInterval.toDF
                                 .withColumn("date", from_unixtime(($"time" / 1e9), "yyyy-MM-dd"))
```

```scala
CustomPlotlyChart(btcVolumeByCurrencyDF,
                  layout="""{
                    title: 'Volume of BTC in different fiat currencies over the last seven years (in 14 day intervals, stacked)',
                    barmode: 'stack',
                    yaxis: {title: 'Volume(BTC/fiat)'}}""",
                  dataOptions="{type: 'bar', splitBy: 'currency_code'}",
                  dataSources="""{
                    x: 'date',
                    y: 'volume_btc_sum'
                  }""",
                 maxPoints=3000)
```

**[click here](https://plot.ly/~drewnoff/48.embed)** to see the interactive chart

<img src="http://telegra.ph/file/7004e68ee5af168393b15.png" width=800>
</img>

and to make closer look at `CNY` currency

```scala
CustomPlotlyChart(btcPricesDF.where($"currency_code" === "CNY").where(year($"date") > 2015),
                  layout="""{
                    title: 'Volume of BTC in CNY over the last year', 
                    showlegend: false, 
                    yaxis: {title: 'Volume (BTC/CNY)'}}""",
                  dataOptions="{type: 'scatter'}",
                  dataSources="""{
                    x: 'date',
                    y: 'volume_btc'
                  }""")
```

**[click here](https://plot.ly/~drewnoff/50.embed)** to see the interactive chart

<img src="http://telegra.ph/file/1ccf019b8cd5d73a82b69.png" width=800>
</img>

## Temporal Join

Now we want to obtain ETH prices in fiat currencies.
But in `crypto_prices` table we have only btc prices for all other crypto currencies while prices in fiat currencies we have only for BTC in `btc_prices` table.

```scala
val ethBTCPricesTsRdd = TimeSeriesRDD
  .fromDF(dataFrame = cryptoPricesDF.where($"currency_code" === "ETH"))(isSorted = false, timeUnit = SECONDS)

ethBTCPricesTsRdd.toDF.show(5)
```

```
+-------------------|---------------|-------------|-------------|------------|-------------|-------------|----------|-------------|----------+
|               time|       datetime|opening_price|highest_price|lowest_price|closing_price|volume_crypto|volume_btc|currency_code|      date|
+-------------------|---------------|-------------|-------------|------------|-------------|-------------|----------|-------------|----------+
|1438880400000000000| 8/6/2015 20:00|      0.00281|          0.1|     0.00281|      0.00998|     53584.56|    577.47|          ETH|2015-08-06|
|1438966800000000000| 8/7/2015 20:00|      0.00998|      0.00998|    0.002304|     0.003123|     722558.0|   2958.54|          ETH|2015-08-07|
|1439053200000000000| 8/8/2015 20:00|     0.003123|     0.003631|     0.00229|     0.002815|    737119.57|   2012.14|          ETH|2015-08-08|
|1439139600000000000| 8/9/2015 20:00|     0.002815|     0.002897|    0.002275|       0.0026|    585917.04|   1486.71|          ETH|2015-08-09|
|1439226000000000000|8/10/2015 20:00|       0.0026|     0.004331|    0.002434|     0.003938|   1479695.62|   4812.95|          ETH|2015-08-10|
+-------------------|---------------|-------------|-------------|------------|-------------|-------------|----------|-------------|----------+
only showing top 5 rows
```

And this is where `JOIN` comes in handy.
But in case of time series data it would be a [Temporal Join](https://github.com/twosigma/flint#temporal-join-functions). And again Flint provide several options for that.

Temproal join functions define a matching criteria over time.
It could be an exact match or it can look past or look future to find closest row from other table with timesatmp located within some `tolerance` inteval.


So given BTC prices in fiat currencies for some timestamp in `btc_prices` table we want to find closest ETH prices in BTC within `1 day` from `crypto_prices` table.

```scala
val ethPricesTsRdd = btcPricesTsRdd.futureLeftJoin(ethBTCPricesTsRdd, tolerance = "1d", leftAlias="btc")
                                   .keepRows { row: Row => row.getAs[String]("currency_code") != null }
```

we keep only those records for which matching criteria is met. 

```scala
CustomPlotlyChart(ethPricesTsRdd.toDF
                  .withColumn("currency_closing_price", $"closing_price" * $"btc_closing_price")
                  .where($"btc_currency_code" isin ("USD", "EUR", "GBP")),
                  layout="""{
                    title: 'Closing price of ETH in three different fiat currencies over the last three years', 
                    yaxis: {title: 'Closing Price (ETH/fiat)'}}""",
                  dataOptions="{type: 'scatter', splitBy: 'btc_currency_code'}",
                  dataSources="""{
                    x: 'date',
                    y: 'currency_closing_price'
                  }""", maxPoints = 3000)
```

**[click here](https://plot.ly/~drewnoff/52.embed)** to see the interactive chart

<img src="http://telegra.ph/file/4e5a2e46521d3cad5d639.png" width=800>
</img>

Prices in different fiat currencies might be in different scales like for `USD` and `CNY`, 
so plotting them on the same chart with single `yaxis` might be not a good idea. For that one can plot them on the same chart but with [multiple yaxes](https://plot.ly/javascript/multiple-axes/) 
or use [subplots](https://plot.ly/javascript/subplots/).

```scala
CustomPlotlyChart(ethPricesTsRdd.toDF
                  .withColumn("currency_closing_price", $"closing_price" * $"btc_closing_price")
                  .where($"btc_currency_code" isin ("USD", "CNY")),
                  layout="""{
                    title: 'Closing price of ETH in two different fiat currencies over the last three years',
                    xaxis: {domain: [0, 0.45]},
                    yaxis: {title: 'Closing Price (ETH/USD)'},
                    xaxis2: {domain: [0.55, 1]},
                    yaxis2: {title: 'Closing Price (ETH/CNY)', anchor: 'x2'}
                  }""",
                  dataOptions="""{
                    type: 'scatter',
                    splitBy: 'btc_currency_code',
                    byTrace: {
                      'USD': {type: 'scatter'},
                      'CNY': {
                        type: 'scatter',
                        xaxis: 'x2',
                        yaxis: 'y2'
                      }
                    }
                  }""",
                  dataSources="""{
                    x: 'date',
                    y: 'currency_closing_price'
                  }""", maxPoints = 2000)
```

**[click here](https://plot.ly/~drewnoff/54.embed)** to see the interactive chart

<img src="http://telegra.ph/file/884ceb2c14b79352a5005.png" width=800>
</img>

# Total trade volume over the past week.

Another good example on using temporal joins would be calculating of total transaction volume in USD
for top all crypto currencies in the dataset over the past week.

```scala
val cryptoBTCPricesTsRdd = TimeSeriesRDD
  .fromDF(dataFrame = cryptoPricesDF.where($"volume_btc" > 0))(isSorted = false, timeUnit = SECONDS)
  
val btcUSDPricesTsRdd = TimeSeriesRDD.fromDF(dataFrame = btcUSDPricesDF)(isSorted = false, timeUnit = SECONDS)
```

An imprortant thing to note here is that when performing a join temproal join function tries to find one closest row from right table.
But in our case several rows from `crypto_prices` table corresponding to different currencies share the same timestamp 
and we want to join BTC prices for all of them.
The solution is to group all rows sharing exactly the same timestamp in `crypto_prices` table using `.groupByCycle` function.

```scala
val cryptoPricesTsRddGrouped = btcUSDPricesTsRdd.leftJoin(cryptoBTCPricesTsRdd.groupByCycle(), tolerance = "1d", leftAlias="btc")
                                             .keepRows { row: Row => row.getAs[Array[Row]]("rows")  != null }

cryptoPricesTsRddGrouped.toDF.select($"time", $"btc_closing_price", $"rows").show(5)
```

```
+-------------------|-----------------|--------------------+
|               time|btc_closing_price|                rows|
+-------------------|-----------------|--------------------+
|1378944000000000000|           139.35|[[137891520000000...|
|1379116800000000000|           136.71|[[137908800000000...|
|1379203200000000000|            138.3|[[137917440000000...|
|1379376000000000000|           139.15|[[137934720000000...|
|1379462400000000000|           140.41|[[137943360000000...|
+-------------------|-----------------|--------------------+
only showing top 5 rows
```

After this we can use `explode` function to create a new row for each element in `rows` array which contains all the rows from `crypto_prices` table
sharing extacly the same timestamp.

```scala
val cryptoBTCPricesDF = cryptoPricesTsRddGrouped.toDF
  .withColumn("currency_row", explode($"rows"))
  .drop($"rows")
  .select(($"time" / 1e9).cast(LongType).as("time"), $"btc_currency_code", $"btc_volume_currency", $"btc_closing_price", 
          $"currency_row.closing_price", $"currency_row.volume_btc", $"currency_row.currency_code")

cryptoBTCPricesDF.show(5)
```

```
+----------|-----------------|-------------------|-----------------|-------------|----------|-------------+
|      time|btc_currency_code|btc_volume_currency|btc_closing_price|closing_price|volume_btc|currency_code|
+----------|-----------------|-------------------|-----------------|-------------|----------|-------------+
|1378944000|              USD|         2886300.08|           139.35|       2.0E-8|    5.0E-4|          IFC|
|1379116800|              USD|         1237692.09|           136.71|       3.0E-8|  0.001435|          IFC|
|1379203200|              USD|          886151.41|            138.3|       3.0E-8|  0.002905|          IFC|
|1379376000|              USD|         1344189.07|           139.15|       2.0E-8|    0.0124|          IFC|
|1379462400|              USD|         1328386.12|           140.41|       2.0E-8|    1.2E-4|          IFC|
+----------|-----------------|-------------------|-----------------|-------------|----------|-------------+
only showing top 5 rows
```

```scala
btcUSDPricesDF.show(5)
```

```
+--------------------|-------------|-------------|------------|-------------|----------|---------------|-------------|----------|----------+
|            datetime|opening_price|highest_price|lowest_price|closing_price|volume_btc|volume_currency|currency_code|      time|      date|
+--------------------|-------------|-------------|------------|-------------|----------|---------------|-------------|----------|----------+
|2010-07-16 20:00:...|      0.04951|      0.04951|     0.04951|      0.04951|      20.0|         0.9902|          USD|1279324800|2010-07-17|
|2010-07-17 20:00:...|      0.04951|      0.08585|     0.05941|      0.08584|     75.01|           5.09|          USD|1279411200|2010-07-18|
|2010-07-18 20:00:...|      0.08584|      0.09307|     0.07723|       0.0808|     574.0|          49.66|          USD|1279497600|2010-07-19|
|2010-07-19 20:00:...|       0.0808|      0.08181|     0.07426|      0.07474|     262.0|          20.59|          USD|1279584000|2010-07-20|
|2010-07-20 20:00:...|      0.07474|      0.07921|     0.06634|      0.07921|     575.0|          42.26|          USD|1279670400|2010-07-21|
+--------------------|-------------|-------------|------------|-------------|----------|---------------|-------------|----------|----------+
only showing top 5 rows
```

```scala
cryptoBTCPricesDF.createOrReplaceTempView("crypto_btc_usd_prices")
```

Now we can perform required aggregation on both `crypto_btc_usd_prices` and `btc_usd_prices` tables and `UNION` the results.

```scala
val weekSeconds = Duration(7, DAYS).toSeconds.toInt
```

```scala
val cryptoUSDWeekTransactionVolumeDF = spark.sql(s"""
-- crypto currencies by total transaction volume (in usd) over the last month

SELECT 'BTC' as currency_code,
       sum(b.volume_currency) as total_volume_in_usd
FROM btc_usd_prices b
WHERE $maxTs - b.time < $weekSeconds

UNION

SELECT c.currency_code as currency_code,
       sum(c.volume_btc) * avg(c.btc_closing_price) as total_volume_in_usd
FROM crypto_btc_usd_prices c
WHERE $maxTs - c.time < $weekSeconds
GROUP BY c.currency_code

ORDER BY total_volume_in_usd DESC
""")

cryptoUSDWeekTransactionVolumeDF.show(10)
```

```
+-------------|--------------------+
|currency_code| total_volume_in_usd|
+-------------|--------------------+
|          BTC|     1.74084501296E9|
|          ETH| 1.582369332000229E9|
|          LTC| 2.813862314621429E8|
|          XRP| 2.635843285063857E8|
|          ETC|1.6212477438077143E8|
|          ANS|    1.236407701113E8|
|           SC|1.0920701628107144E8|
|         DASH| 8.499491466012858E7|
|          ZEC| 7.712978239088571E7|
|          BTS| 6.795514801418571E7|
+-------------|--------------------+
only showing top 10 rows
```

```scala
CustomPlotlyChart(cryptoUSDWeekTransactionVolumeDF.limit(10),
                  layout="""{
                    title: 'Total transaction volume in USD for top 10 currencies over the past week (ranked by volume)',
                    yaxis: {title: 'Total Volume (currency/USD)'}}""",
                  dataOptions="{type: 'bar', splitBy: 'currency_code'}",
                  dataSources="""{
                    x: 'currency_code',
                    y: 'total_volume_in_usd'
                  }""")
```

**[click here](https://plot.ly/~drewnoff/56.embed)** to see the interactive chart

<img src="http://telegra.ph/file/59b0887e76a6586761712.png" width=800>
</img>

## Conclusion

I hope this notebook has given you some ideas on how can you use these great tools like Apache Spark, Flint library, 
Spark Notebook and Plotly scientific graphing library for time serires data analysis. ALso given that Apache Spark is a fast and general engine for big data processing you can use all these tools on much larger datasets in cluster computing environment.

*And again many thanks to [@sarahpan](https://blog.timescale.com/@sarahpan) for [sharing](https://blog.timescale.com/analyzing-ethereum-bitcoin-and-1200-cryptocurrencies-using-postgresql-3958b3662e51) her ideas on analysis of given dataset*.
