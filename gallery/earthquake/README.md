This notebook is highly inspired by [@jlcoto](https://github.com/jlcoto) [assignment](https://github.com/jlcoto/Udacity/tree/master/earthquake_project) for the Udacity Data Analytics Nanodegree project.

In this notebook we'll see how we can use Spark Notebook to merge and clean data from different sources
to visualize and get insites on significant earthquake historical data.
We will use Spark DataFrame API, 
[Magellan](https://github.com/harsha2010/magellan) spark package for geospatial analytics
and build in support (`CustomPlotlyChart`) for [Plotly javascript API](https://plot.ly/javascript/) to visualize the data. 
For more examples on usage of `CustomPlotlyChart` refer to notebooks from `notebooks/viz` dir which comes with Spark Notebook distribution.
Resulting Plotly charts are interactive so fill free to zoom and hover over data directly from the Notebook.
Additionally I've exported the resulting charts to [plotly cloud](https://plot.ly ) and added links to this README so one could navigate from the README to fully interactive resulting charts.

## Roadmap

1. First we will use **[The Significant Earthquake Database](https://www.ngdc.noaa.gov/nndc/struts/form?t=101650&s=1&d=1)**
which contains information on destructive earthquakes from 2150 B.C. to the present that meet at least one of the following criteria: 
 - Moderate damage (approximately \$1 million or more);
 - 10 or more deaths;
 - Magnitude 7.5 or greater;
 - Modified Mercalli Intensity X or greater;
 - the earthquake generated a tsunami.

    We will perform some clean up steps on this data and will try to plot earthquake data on a map.
 
2. Next we will use **[World Borders Dataset](http://thematicmapping.org/downloads/world_borders.php)** to enrich our data
with country shapes and supplementary country codes (like ISO3). In order to do that, we will use a spatial merge with a help of [Magellan](https://github.com/harsha2010/magellan) spark package.

3. Finally we will merge our data with **[Penn World Table](http://www.rug.nl/ggdc/productivity/pwt/)** using ISO3 country codes obtained on the previous step. The Penn World tables is a database with information on relative levels of income, output, input and productivity, covering 182 countries between 1950 and 2014.

```scala
val spark = sparkSession
import spark.implicits._
import org.apache.spark.sql.functions._
```

## The Significant Earthquake Database

Here is direct [download](https://www.ngdc.noaa.gov/nndc/struts/results?type_0=Exact&query_0=$ID&t=101650&s=13&d=189&dfn=signif.txt) of data file in tab-delimited format and [Event Variable Definitions](https://www.ngdc.noaa.gov/nndc/struts/results?&t=101650&s=225&d=225).

For importing this dataset we can use `csv` format with tab-delimited separator.

```scala
val earthquakeDF = spark.read.format("csv")
                        .option("header", "true")  
                        .option("sep", "\t") 
                        .load("notebooks/spark-notebooks-gallery/gallery/earthquake/data/signif.tsv")
```

Let's have a look at the number of registered earthquakes per century.

```scala
import org.apache.spark.sql.types.IntegerType

val earthquakesByCentury = earthquakeDF
                            .filter(!isnull($"YEAR"))
                            .withColumn("Century", (($"YEAR".cast(IntegerType)) / 100).cast(IntegerType) + 1)
                            .select("Century")
                            .groupBy("Century")
                            .count
```

```scala
CustomPlotlyChart(earthquakesByCentury,
                  layout="{title: 'Registered earthquakes by century', xaxis: {title: 'Century'}}",
                  dataOptions="{type: 'bar'}",
                  dataSources="{x: 'Century', y: 'count'}")
```

**[click here](https://plot.ly/~drewnoff/7.embed)** to see the interactive chart

<img src="http://telegra.ph/file/e8c13366fa4826d12a808.png" width=800>
</img>

We are going to observe only earthquakes that happened from 1900 onwards. Also, we want our earthquake to have complete registries in terms of year, days and months in which they occurred.

```scala
val earthquakeData = earthquakeDF.na.drop(Seq("YEAR", "MONTH", "DAY"))
            .filter(($"YEAR").cast(IntegerType) >= 1900)
            .withColumn("Date", concat($"YEAR", lit("-"), $"MONTH", lit("-"), $"DAY"))
            .withColumn("Date", to_date($"Date"))
            .withColumn("DEATHS", trim($"DEATHS").cast(IntegerType))
```

It's interesting to look at the total number of deaths caused by earthquakes in observed period of time per country.

```scala
val totalDeathsByCountry = earthquakeData.filter(!isnull($"DEATHS"))
              .groupBy("Country")
              .agg(sum($"DEATHS").alias("TOTAL_DEATHS"))
              .orderBy(-$"TOTAL_DEATHS")

totalDeathsByCountry.show(5)
```

```
+--------+------------+
| Country|TOTAL_DEATHS|
+--------+------------+
|   CHINA|      650543|
|   HAITI|      316006|
|    IRAN|      189652|
|   JAPAN|      165728|
|PAKISTAN|      146864|
+--------+------------+
only showing top 5 rows
```

```scala
CustomPlotlyChart(totalDeathsByCountry,
                  layout="{title: 'Total deaths by country since 1900', xaxis: {title: 'Country'}}",
                  dataOptions="{type: 'bar'}",
                  dataSources="{x: 'Country', y: 'TOTAL_DEATHS'}")
```

**[click here](https://plot.ly/~drewnoff/9.embed)** to see the interactive chart

<img src="http://telegra.ph/file/4e01dda225650c862dfa4.png" width=800></img>

### Significant earthquakes around the world

We can use provided geo data to point out where every significant earthquake had taken place.

```scala
val earthquakeVizData = earthquakeData
        .select("LATITUDE", "LONGITUDE", "LOCATION_NAME", "DEATHS", "DEATHS_DESCRIPTION", "YEAR")
        .withColumn("text", concat($"LOCATION_NAME", 
                                   lit(", "), $"YEAR", lit(". Deaths: "),
                                   when(isnull($"DEATHS"), "No data").otherwise($"DEATHS")))
        .withColumn("earthquakeSize", when(isnull($"DEATHS"), 5).otherwise( log(2, $"DEATHS") + 5) )
        .withColumn("color", $"earthquakeSize")
        .drop("DEATHS_DESCRIPTION")
```

```scala
CustomPlotlyChart(
  earthquakeVizData, 
  layout="""{
        title: 'Significant Earthquakes around the World since 1900',
        height: 800,
        geo: {
            projection: {
                type: 'robinson'
            },
            showlakes: true,
            lakecolor: 'lightblue',
            showland: true,
            landcolor: 'rgb(237, 237, 237)',
            showocean: true,
            oceancolor: 'lightblue',
            showcountries: true,
            countrywidth: 0.2
        }
    }""",
  dataOptions="""{
    type: 'scattergeo',
    locationmode: 'country names',
    hoverinfo: 'text',
    marker: {
        line: {
            width: 0
        }}}""",
  dataSources="{lat: 'LATITUDE', lon: 'LONGITUDE', text: 'text', marker: {size: 'earthquakeSize', color: 'color'}}",
  maxPoints=4000)
```

**[click here](https://plot.ly/~drewnoff/3.embed)** to see the interactive chart

<img src="http://telegra.ph/file/71e249ff09ea703e4e206.png" width=800></img>

## Merging earthquake data with World Borders data

[World Borders Dataset](http://thematicmapping.org/downloads/world_borders.php) is stored in [shapefile](https://en.wikipedia.org/wiki/Shapefile) format and contains country borders as polygons.

To work with shapefiles we will use third party spark package called [Magellan](https://github.com/harsha2010/magellan).

```scala
val worldShapeDF = spark.read
  .format("magellan")
  .load("notebooks/spark-notebooks-gallery/gallery/earthquake/data/TM_WORLD_BORDERS-0.3")
  .select("polygon", "metadata")
  
worldShapeDF.show(5)
```

```
+--------------------+--------------------+
|             polygon|            metadata|
+--------------------+--------------------+
|magellan.Polygon@...|Map(UN ->  68, FI...|
|magellan.Polygon@...|Map(UN -> 704, FI...|
|magellan.Polygon@...|Map(UN -> 784, FI...|
|magellan.Polygon@...|Map(UN -> 586, FI...|
|magellan.Polygon@...|Map(UN -> 608, FI...|
+--------------------+--------------------+
only showing top 5 rows
```

Let's extract `ISO3` country codes from `metadata` column.

```scala

val countryShapeDF = worldShapeDF.select($"polygon", explode($"metadata").as(Seq("k", "v")))
            .filter($"k" === "ISO3")
            .drop("k")
            .withColumnRenamed("v", "ISO3")
countryShapeDF.show(5)
```

```
+--------------------+----+
|             polygon|ISO3|
+--------------------+----+
|magellan.Polygon@...| BOL|
|magellan.Polygon@...| VNM|
|magellan.Polygon@...| ARE|
|magellan.Polygon@...| PAK|
|magellan.Polygon@...| PHL|
+--------------------+----+
only showing top 5 rows
```

The earthquake data has latitude and longitude coordinates for the earthquakes' location.
To perform spatial join one should look if the latitude and longitude fall within the boundaries of a country geometry and, if so, merge it appropriately.
In order to perform this we should convert `LONGITUDE` and `LATITUDE` pairs from the earthquake data to magellan `Point` type and use `within` predicate from `magellan` dsl.

```scala
import org.apache.spark.sql.magellan.dsl.expressions._
import org.apache.spark.sql.types._

import org.apache.spark.sql.types.DoubleType

val earthquakePoints = earthquakeData
  .withColumn("LONGITUDE", col("LONGITUDE").cast(DoubleType))
  .withColumn("LATITUDE", col("LATITUDE").cast(DoubleType))
  .na.drop(Seq("LONGITUDE", "LATITUDE"))
  .withColumn("Point", point($"LONGITUDE", $"LATITUDE"))
```

```scala
val spatialJoined = earthquakePoints
                .join(countryShapeDF)
                .where($"Point" within $"polygon")
                .cache()
spatialJoined.count
```
```
res37: Long = 2197
```