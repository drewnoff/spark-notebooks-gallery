This notebook is highly inspired by [@jlcoto](https://github.com/jlcoto) [assignment](https://github.com/jlcoto/Udacity/tree/master/earthquake_project) for the Udacity Data Analytics Nanodegree project.

In this notebook we'll see how we can use Spark Notebook to merge and clean data from different sources
to visualize and get insites on significant earthquake historical data.
We will use Spark DataFrame API, 
[Magellan](https://github.com/harsha2010/magellan) spark package for geospatial analytics
and build in support (`CustomPlotlyChart`) for [Plotly javascript API](https://plot.ly/javascript/) to visualize the data. For more examples on usage of `CustomPlotlyChart` refer to notebooks from `notebooks/viz` dir which comes with Spark Notebook distribution.

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

**[click here](https://plot.ly/~drewnoff/7.embed)** to see interactive chart

<img src="http://telegra.ph/file/e8c13366fa4826d12a808.png" width=800>
</img>
