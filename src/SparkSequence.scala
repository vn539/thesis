import java.util

import org.apache.hadoop.io.{IntWritable, BytesWritable}
import org.apache.spark.{SparkContext, SparkConf}
import scala.collection.mutable.ListBuffer

/**
  * Created by vinu on 12/5/15.
  */
class SparkSequence {

  def  getImageSplits() : java.util.List[ImageSplits] =  {

    val conf = new SparkConf().setAppName("Nidan ImageSplitter")
    val sc = new SparkContext(conf)
    val file = sc.sequenceFile("hdfs://localhost:9000/outimage.jpg", classOf[IntWritable],classOf[BytesWritable])
    val rddData = file.map{case (x,y) => (x.get(), y.copyBytes())}
    val sortedData = rddData.sortByKey(true).collect().toList
    //val sortedData = rddData.collect().toList
    println("sortedData.length")
    println(sortedData.length)

    //var imageSplits = new java.util.List[ImageSplits]()
    //var imageSplits = new ListBuffer[ImageSplits]()
   val imageSplits = new java.util.ArrayList[ImageSplits]()

    // Print all the array elements
    for ( x <- sortedData )
    {
      println( x._1 )
      println( x._2.length )

      var imageSplit = new ImageSplits()
      imageSplit.zcoord = x._1
      imageSplit.splitImage = x._2

      //imageSplits += imageSplit
      imageSplits.add(imageSplit)
    }

    return imageSplits
  }
}
