import java.util

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.RangePartitioner
import scala.collection.mutable._

/**
  * Created by vinu on 12/5/15.
  */
class SparkPartitioner {

  def saveImage(imageSplits: java.util.List[ImageSplits])
  {
    println("Hello (class)")

    var imageList = new ListBuffer[Tuple2[Int, Array[Byte]]]()
    var i = 0
    //loop through each item in imageSplits, create a Tuple2 and add to scala list
    while (i < imageSplits.size())
    {
      var zcoord: Int = imageSplits.get(i).zcoord
      var imageChuck: Array[Byte] = imageSplits.get(i).splitImage

      //println("imageChuck length")
      //println(imageChuck.length)

      imageList += (new Tuple2(zcoord, imageChuck))
      i += 1
      //println(zcoord)
    }

    val pairList = imageList.toList
    println("pairList length")
    println(pairList.length)

    val conf = new SparkConf().setAppName("Nidan ImageSplitter")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(imageList)

    //call map
    val input = data.map{case (x,y) => (x.get(),y.getBytes)}
    val tunedPartitioner = new RangePartitioner(5, input)
    val partitioner = input.partitionBy(tunedPartitioner)
    partitioner.saveAsSequenceFile("hdfs://localhost:9000/outimage.jpg")

  }
}
