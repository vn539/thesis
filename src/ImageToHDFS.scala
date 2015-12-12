import java.io.{ByteArrayOutputStream, File}
import javax.imageio.ImageIO
import java.awt.image.BufferedImage

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.RangePartitioner
import scala.collection.mutable._

/**
  * Created by vinu on 12/8/15.
  */
object ImageToHDFS {

  def main(args: Array[String]) {

    // args(0) = split image directory
    // args(1) = HDFS full path and file name to store
    // args(2) = user partition - Y/N
    if (args.length < 2)
    {
      System.err.println("Usage: HDFSToLocal {local input image dir} {hdfs output filename} {optional: use partition=Y/N}")
      System.exit(1)
    }

    // get list of files from the specified directory passed as args(0)
    val fileList = getListOfFiles(args(0))
    val hdfsFileName = args(1)
    var usePartition: String = "N" // default is to not use Partition

    println("args(0) = " + args(0))   // split image directory
    println("args(1) = " + args(1))   // HDFS full path and file name to store

    if (args.length == 3)
    {
      println("args(2) = " + args(2))   // use partition = Y/N
      usePartition = args(2)
    }

    val imageList = new ListBuffer[Tuple2[Int, Array[Byte]]]()

    // for each file
    for (file <- fileList)
    {
      val image = ImageIO.read(file)
      val baos: ByteArrayOutputStream = new ByteArrayOutputStream
      ImageIO.write(image, "jpg", baos)
      baos.flush

      print(file.getName() + "    ")
      println("zcoord:  " + file.getName().substring(0, file.getName().indexOf("_")).toInt)

      // get zcoord from file name 0_img.jpg
      var zcoord: Int = file.getName().substring(0, file.getName().indexOf("_")).toInt
      var imageChuck: Array[Byte] = baos.toByteArray
      baos.close

      imageList += (new Tuple2(zcoord, imageChuck))
    }

    println("imageList count: " + imageList.length)

    val conf = new SparkConf().setAppName("Nidan ImageSplitter")
    val sc = new SparkContext(conf)
    val data = sc.parallelize(imageList)

    //call map
    val input = data.map{case (x,y) => (x.get(),y.getBytes)}

    if (usePartition.equalsIgnoreCase("Y"))
    {
      println("using range partition to store")
      val tunedPartitioner = new RangePartitioner(5, input)
      val partitioner = input.partitionBy(tunedPartitioner)
      partitioner.saveAsSequenceFile(hdfsFileName)
    }
    else
    {
      println("not using range partition to store")
      input.saveAsSequenceFile(hdfsFileName)
    }

  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    if (d.exists && d.isDirectory) {
      //d.listFiles.filter(_.isFile).toList
      d.listFiles.filter(f => f.isFile && f.getName().endsWith(".jpg")).toList
    } else {
      List[File]()
    }
  }
}
