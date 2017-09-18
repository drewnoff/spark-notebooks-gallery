# Obtaining physical activity monitor data

To conduct our analysis of physical activity monitor measurements we will use [NHANES](https://wwwn.cdc.gov/Nchs/Nhanes/) public dataset.
We're interested in demographic and physical activity monitor files from the 2003-2004 and 2005-2006 cycles:

 - [NHANES 2003-2004 Demographics Data](https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&CycleBeginYear=2003)
 - [NHANES 2003-2004 Examination Data](https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&CycleBeginYear=2003) (Physical Activity Monitor codebook and data file)
 - [NHANES 2005-2006 Demographics Data](https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Demographics&CycleBeginYear=2005) (Physical Activity Monitor codebook and data file)
 - [NHANES 2005-2006 Examination Data](https://wwwn.cdc.gov/nchs/nhanes/search/datapage.aspx?Component=Examination&CycleBeginYear=2005)
 
 
Data files are store in SAS Transport Files format.

To convert from sas transport file format with extension`.xpt` to CSV we can use `xport` module from python PyPI packages:

```bash
pip install xport
```
and use the `xport` module as a command-line tool to convert an XPT file to CSV file:

```bash
python -m xport paxraw_d.xpt > paxraw_d.csv
```

This might take some time for physical activity monitor data files.

### Note on `PAXINTEN` vs `PAXSTEP`

According to supplied codebook the physical activity monitors (PAMs) used in NHANES collected objective information
on the intensity and duration of common locomotion activities such as walking and jogging.
The device is programmed to detect and record the magnitude of acceleration or “intensity” of movement; acceleration data are stored in memory according to a specified time interval. A one minute time interval or “epoch” was used in NHANES. Intensity readings were summed over each 1-minute epoch and stored in `PAXINTEN` variable.

In addition to intensity value there is also `PAXSTEP` variable which stores the step count recorded by the physical activity monitor 2005-2006 examination data. Since these two variables are correlated and `PAXSTEP` is not available for 2003-2004 cycle then we will use only `PAXINTEN` variable to unite datasets from two cycles.

## Preparing the data

```Scala
import org.apache.spark.sql.types._
import org.apache.spark.sql.functions._

val spark = sparkSession
```

### Physical activity monitor data

```Scala
val PaxSchema = StructType(
    StructField("SEQN", FloatType, false) ::
    StructField("PAXSTAT", FloatType, false) ::
    StructField("PAXCAL", FloatType, false) ::
    StructField("PAXDAYSAS", FloatType, false) ::
    StructField("PAXN", FloatType, false) ::
    StructField("PAXHOUR", FloatType, false) ::
    StructField("PAXMINUT", FloatType, false) ::
    StructField("PAXINTEN", FloatType, false) ::
    Nil
)

def readPaxData(path: String) = {
  spark.read
  .format("csv")
  .schema(PaxSchema)
  .option("header", true)
  .load(path)
  .select($"SEQN".cast(IntegerType),
          $"PAXSTAT".cast(IntegerType),
          $"PAXCAL".cast(IntegerType),
          $"PAXDAYSAS".cast(IntegerType),
          $"PAXN".cast(IntegerType),
          $"PAXHOUR".cast(IntegerType),
          $"PAXMINUT".cast(IntegerType),
          $"PAXINTEN".cast(IntegerType) 
         )
}
```

```Scala
val PaxDF_c = readPaxData("./notebooks/spark-notebooks-gallery/gallery/physical-activity-monitor/data/paxraw_c.csv")
val PaxDF_d = readPaxData("./notebooks/spark-notebooks-gallery/gallery/physical-activity-monitor/data/paxraw_d.csv")

val PaxDF = PaxDF_c.union(PaxDF_d)

PaxDF.limit(10).show
```

```
+-----+-------+------+---------+----+-------+--------+--------+
| SEQN|PAXSTAT|PAXCAL|PAXDAYSAS|PAXN|PAXHOUR|PAXMINUT|PAXINTEN|
+-----+-------+------+---------+----+-------+--------+--------+
|21005|      1|     1|        1|   1|      0|       0|       0|
|21005|      1|     1|        1|   2|      0|       1|       0|
|21005|      1|     1|        1|   3|      0|       2|       0|
|21005|      1|     1|        1|   4|      0|       3|       0|
|21005|      1|     1|        1|   5|      0|       4|       0|
|21005|      1|     1|        1|   6|      0|       5|       0|
|21005|      1|     1|        1|   7|      0|       6|       0|
|21005|      1|     1|        1|   8|      0|       7|       0|
|21005|      1|     1|        1|   9|      0|       8|       0|
|21005|      1|     1|        1|  10|      0|       9|       0|
+-----+-------+------+---------+----+-------+--------+--------+
```

```Scala
PaxDF.write
  .format("parquet")
  .mode("overwrite")
  .save("./notebooks/spark-notebooks-gallery/gallery/physical-activity-monitor/data/paxraw.parquet")
```

### Demographics Data

```Scala
def readDemoDF(path: String) = {
   spark.read
  .format("csv")
  .option("header", true)
  .load(path)
  .select($"SEQN".cast(IntegerType), $"RIDAGEYR".cast(IntegerType))
}

val DemoDF_c = readDemoDF("./notebooks/spark-notebooks-gallery/gallery/physical-activity-monitor/data/DEMO_C.csv")
val DemoDF_d = readDemoDF("./notebooks/spark-notebooks-gallery/gallery/physical-activity-monitor/data/DEMO_D.csv")

val DemoDF = DemoDF_c.union(DemoDF_d)

DemoDF.limit(3).show
```

```
+-----+--------+
| SEQN|RIDAGEYR|
+-----+--------+
|21005|      19|
|21006|      16|
|21007|      14|
+-----+--------+
```

```Scala
DemoDF.write
  .format("parquet")
  .mode("overwrite")
  .save("./notebooks/spark-notebooks-gallery/gallery/physical-activity-monitor/data/demo.parquet")
```