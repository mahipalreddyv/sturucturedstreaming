package Batch28_SparkCore1.Batch28_SparkCore1
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._;
import org.apache.spark.sql.SparkSession
object StructStreaming {  
  def main(args: Array[String]) = {     
    val conf = new SparkConf().setAppName("mysparkbatch28").setMaster("local[*]");
    val spark =  SparkSession.builder().appName("SomeAppName").config("spark.master", "local").getOrCreate();

    val readschema="D:\\jsonfiles\\schema28.json"
    val readFile="D:\\jsonfiles\\"
     val checkpoint="D:\\checkpoint\\"
     val output="D:\\output\\"
    var jsonSchema=spark.read.json(readschema).schema   
    var readJsonDF=spark.readStream.schema(jsonSchema).json(readFile).where("events.beaconType=\"pageAdRequested\"")
    val adperfExpression= """events.client AS client
        events.beaconType AS beaconType
        events.beaconVersion AS beaconVersion
        events.data.milestones.amazonA9Requested AS amazonRequested
        events.data.milestones.indexExchangeRequested AS indexExchange
        events.data.milestones.amazonA9BidsRequested AS BidsRequested""".split("\n")
         
  def getSelectExprByBeaconType(): Array[String] = {  
    return adperfExpression 
}
   var selectDF=readJsonDF.selectExpr(getSelectExprByBeaconType():_*) 
   selectDF.writeStream.format("csv").option("checkpointLocation", checkpoint).option("path", output).start()
   val query=selectDF.writeStream.outputMode("Append").format("console").start()
    query.awaitTermination()
}
}