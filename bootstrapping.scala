import org.apache.spark.rdd.RDD
import org.apache.hadoop.fs.{FileSystem, Path}

/*
 * step 0: read input parameters
 * */
// spark-shell -i your_script.scala --conf spark.driver.args="times=20 percentages=(0.2,0.4,0.6)"
var times = 10
var percentages = Array(0.5, .75)
try {
    val args = sc.getConf.get("spark.driver.args").split("\\s+")
    //val args="times=100 percentages=(0.7, 0.9)"

    for (arg <- args) {
        val p = arg.split("=")
        if(p(0)=="times") times = p(1).toInt
        if(p(0)=="percentages")
            percentages = p(1).slice(1, p(1).length() - 1).split(",").map(_.trim).map(_.toDouble)
    }
    println("\n*** Input parameters:")
} catch {
    case x: NoSuchElementException => {
        println("Usage: spark-shell -i your_script.scala --conf spark.driver.args=\"times=<times> percentages=<percentages>\"")
        println("           times: number of bootstrapping iterations")
        println("           percentages: sampling percetage, comma separated(element1,...)")
        println("Example: spark-shell -i bootstrapping.scala --conf spark.driver.args=\"times=20 percentages=(0.2,0.4,0.6)\"")
        println("\n*** No input parameters, use default:")
    }
}
println("times=" + times)
println("percentages=(" + percentages.mkString(", ") + ")")

/*
 * step 1: read data file
 * */
var lines = sc.textFile("file://" + System.getProperty("user.dir") + "/SOCR_Data_MLB_HeightsWeights.csv")

/*
 * step 2: create pairRDD population
 * */
val header = lines.first()
val headerArr = header.split("\t")
lines = lines.filter(row => row != header)
val population = lines.map {
  line =>
    val col = line.split("\t")
    (col(2), col(3).toInt)
}

/*
 * step 3: compute population average and variance
 * */
// compute Mean and Variance of a pairRDD
def computeDist(x : RDD[(String, Int)]) : RDD[(String, (Double, Double))] = {
    x.
        mapValues(x=>(x, x*x, 1)).
        reduceByKey((x,y)=>(x._1 + y._1, x._2 + y._2, x._3 + y._3)).
        mapValues {
            x=>
                val avg = x._1*1.0/x._3
                val variance = x._2*1.0/x._3 - avg*avg
                (avg, variance)
        }.
        cache()
}

val pDist = computeDist(population)

// dump distibution (Mean, Variance)
val dump = (xDist : RDD[(String, (Double, Double))]) => {
    println(f"${headerArr(2)}%20s\t${headerArr(3)} Mean\tVariance")
    xDist.
        sortBy(_._2._1).collect.
        foreach(x=>println(f"${x._1}%20s\t${x._2._1}%f\t\t${x._2._2}%f"))
}

dump(pDist)


/*
 * step 4: Create the sample for bootstrapping.
 * */
val per = 0.25
val sample_ = population.sample(false, per)

/*
 * Step 5. Do 1000 times
 * */    
// implement bootstrapping for the sample pairRDD with given times
val bootstrapping = (sample_ : RDD[(String, Int)], times: Int) => {
    var sDist = computeDist(sample_)
    for(i <- 0 until times) {
        val resampledData = sample_.sample(true, 1)
        val rsDist = computeDist(resampledData)
        sDist = sDist.union(rsDist).groupByKey().mapValues{
            list=>
                var first = 0.0
                var second = 0.0
                for(p <- list) {
                    first += p._1
                    second += p._2
                }
                (first, second)
        }
    }
    sDist.
        mapValues(x=>(x._1/(times + 1), x._2/(times + 1))).
        sortBy(_._2._1).
        cache()
}

//val times = 9
val bs = bootstrapping(sample_, times)
dump(bs)


/*
 * Step 6 Determine the absolute error percentage
 * */

//val percentage = List(0.5, 0.75)

val absDiff = (x: ((Double, Double), (Double, Double))) => {
    val estimate = x._1
    val actual = x._2
    val dmean = math.abs((actual._1 - estimate._1))*100.0/actual._1
    val dvariance = math.abs((actual._2 - estimate._2))*100.0/actual._2
    (dmean, dvariance)
}

val computeError = (percentage: Double) => {
    val sample_ = population.sample(false, percentage)
    val bs = bootstrapping(sample_, times)
    //(k, ((e1,e2),(a1,a2)))
    bs.join(pDist).mapValues(absDiff)
}

val error1 = bs.join(pDist).mapValues(absDiff)
val csv_header = ("Percentage"+:error1.sortBy(_._1).map(x=>x._1).flatMap(x=>List(x+"_mErr", x+"_vErr")).collect()).mkString(",")
// Percentage,Catcher_mErr,Catcher_vErr,Designated_Hitter_mErr,Designated_Hitter_vErr,First_Baseman_mErr,First_Baseman_vErr,Outfielder_mErr,Outfielder_vErr,Relief_Pitcher_mErr,Relief_Pitcher_vErr,Second_Baseman_mErr,Second_Baseman_vErr,Shortstop_mErr,Shortstop_vErr,Starting_Pitcher_mErr,Starting_Pitcher_vErr,Third_Baseman_mErr,Third_Baseman_vErr
val csv_1 = (25+:error1.sortBy(_._1).map(x=>x._2).flatMap(x=>List(x._1, x._2)).collect()).mkString(",")
var csv_ = List(csv_header)
csv_ = csv_ :+ csv_1

for (p <- percentages) {
    //println(p)
    val error = computeError(p)
    val csv_p = (p*100 +: error.sortBy(_._1).map(x=>x._2).flatMap(x=>List(x._1, x._2)).collect()).mkString(",")
    csv_ = csv_ :+ csv_p
}

val fs = FileSystem.get(sc.hadoopConfiguration)
val outPutPath = new Path("/user/cloudera/csv")
if (fs.exists(outPutPath)) fs.delete(outPutPath, true)
sc.parallelize(csv_).repartition(1).saveAsTextFile("hdfs://" + outPutPath)
