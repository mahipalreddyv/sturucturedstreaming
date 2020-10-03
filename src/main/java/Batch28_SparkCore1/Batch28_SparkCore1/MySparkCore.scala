package Batch28_SparkCore1.Batch28_SparkCore1
import org.apache.spark.sql.SQLContext
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.{SparkConf, SparkContext}
import org.apache.spark.sql.types._;

object MySparkCore {
  def main(args: Array[String]) = {     
    val conf = new SparkConf().setAppName("mysparkbatch28").setMaster("local[*]");
    val sc = new SparkContext(conf)    
    val sqlContext = new SQLContext(sc) 
     import sqlContext.implicits._
     
    val readschema=args(0)
    val readFile=args(1)
    val outputdir=args(2)
    
    var jsonSchema=sqlContext.read.json(readschema).schema   
    var readJsonDF=sqlContext.read.schema(jsonSchema).json(readFile).where("events.beaconType=\"pageAdRequested\"")
   
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
   selectDF.coalesce(1).write.mode("append").format("csv").saveAsTable("batch28_02102020")
   selectDF.show
   
   
  }
}