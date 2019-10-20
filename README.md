# Big Data: Bootstrapping with Spark

[Bootstrapping](https://www.thoughtco.com/what-is-bootstrapping-in-statistics-3126172) is a basic statistical technique to determine parameters (mean, variance...) of a population by averaging many re-samplings (with replacement)  of a random sample of that population.

## Dataset

[SOCR Data - 1035 Records of Heights (in) and Weights (lbs) of Major League Baseball Players](http://wiki.stat.ucla.edu/socr/index.php/SOCR_Data_MLB_HeightsWeights)

|      Name       | Team |   Position    | Height(inches) | Weight(pounds) |  Age  |
| :-------------: | :--: | :-----------: | :------------: | :------------: | :---: |
|  Adam_Donachie  | BAL  |    Catcher    |       74       |      180       | 22.99 |
|    Paul_Bako    | BAL  |    Catcher    |       74       |      215       | 34.69 |
| Ramon_Hernandez | BAL  |    Catcher    |       72       |      210       | 30.78 |
|  Kevin_Millar   | BAL  | First_Baseman |       72       |      210       | 35.43 |
|       ...       | ...  |      ...      |      ...       |      ...       |  ...  |

## Problem

To use bootstrapping to find (mean, variance) of `Height(inches)` for each `Position`

- sampling 25% of the population without replacement
- bootstrapping the above sample with e.g. 1000 times, compute average of (mean, variance) 

Using Spark:

- Input is (Key=Position, Value=Height)

|    Position    | Height(inches) |
| :------------: | :------------: |
|    Catcher     |       74       |
|    Catcher     |       74       |
| Second_Baseman |       72       |
| First_Baseman  |       72       |
| First_Baseman  |       73       |
|      ...       |       ..       |

- Output (Key=Position, Value=(average, variance))

|     Position      | Height(inches)_mean | Height(inches)_variance |
| :---------------: | :-----------------: | :---------------------: |
|      Catcher      |      71.305937      |        1.679255         |
|  Second_Baseman   |      71.696590      |        3.561578         |
|   First_Baseman   |      72.571514      |        2.065369         |
| Designated_Hitter |      72.645777      |        4.344335         |
| Starting_Pitcher  |      73.134299      |        4.184805         |
|        ...        |         ..          |           ...           |

- Compute absolute error of the estimation from bootstrapping for different sampling percentages

| Percentage | Catcher_Height_mean_error | Catcher_Height_variance_error | ...  |
| :--------: | :-----------------------: | :---------------------------: | :--: |
|     25     |                           |                               |      |
|     50     |                           |                               |      |
|     75     |                           |                               |      |

## How to run

`bootstrapping.scala` script bootstrap the data file `SOCR_Data_MLB_HeightsWeights.csv` output the result to console, then save the absolute error at `hdfs:///user/cloudera/csv`:

```bash
Usage:
spark-shell -i bootstrapping.scala --conf spark.driver.args="times=<times> percentages=<percentages>"
	- times: number of bootstrapping iterations
    - percentages: sampling percetage, comma separated(element1,...)

Example:
spark-shell -i bootstrapping.scala --conf spark.driver.args=\"times=20 percentages=(0.4,0.55,0.7,0.85)\"")
```

 `create_table.sql` maps `hdfs:///user/cloudera/csv` to Hive table for other Hive queries and visualization purpose:

```bash
hive -f create_table.sql
```

## Result

### Population distribution

```tex
            Position    Height(inches) Mean     Variance
      Second_Baseman    71.362069               2.817182
           Shortstop    71.903846               3.163831
             Catcher    72.723684               3.068386
          Outfielder    73.010309               4.041131
       Third_Baseman    73.044444               4.442469
       First_Baseman    74.000000               3.745455
   Designated_Hitter    74.222222               3.061728
      Relief_Pitcher    74.374603               4.824752
    Starting_Pitcher    74.719457               5.043468
```

### 25% bootstrapping (100 times)

```tex
            Position    Height(inches) Mean     Variance
      Second_Baseman    71.305937               1.679255
           Shortstop    71.696590               3.561578
             Catcher    72.571514               2.065369
          Outfielder    72.645777               4.344335
       Third_Baseman    73.134299               4.184805
   Designated_Hitter    73.803454               0.861009
      Relief_Pitcher    74.255059               4.188984
    Starting_Pitcher    74.510943               3.656192
       First_Baseman    74.514773               2.807699
```

### Absolute errors (100 times)

![absolute_error_100_times](https://github.com/thongnguyen2410/BD_Spark_Bootstrapping/blob/master/absolute_error_100_times.jpg)