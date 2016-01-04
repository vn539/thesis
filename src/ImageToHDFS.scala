import java.io.{ByteArrayOutputStream, File}
import javax.imageio.ImageIO
import java.awt.image.BufferedImage
import java.util.Calendar

import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.hadoop.io.IntWritable
import org.apache.hadoop.io.BytesWritable
import org.apache.spark.RangePartitioner
import org.apache.spark.api.java.StorageLevels
import org.apache.spark.rdd.RDD
import scala.collection.mutable._

/**
  * Created by vinu on 12/8/15.
  */
object ImageToHDFS {

  def main(args: Array[String]) {

    // args(0) = split image directory
    // args(1) = HDFS full path and file name to store
    // args(2) = user partition - Y/N
    // args(3) = number of partition
    println("Start-Time = " + Calendar.getInstance().getTime())
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

    if (args.length == 4)
    {
      println("args(2) = " + args(2))   // use partition = Y/N
      usePartition = args(2)
    }

    //val imageList = new ListBuffer[Tuple2[Int, Array[Byte]]]()

    println("begining of Initialize spark Context")
    val conf = new SparkConf().setAppName("Nidan ImageSplitter")
    val sc = new SparkContext(conf)
    println("After spark conf")
    val imageList = new ListBuffer[Tuple2[Int, Array[Byte]]]()
    var unionData: RDD[(Int, Array[Byte])] = null
    var i = 0

    // for each file
    try {
      for (file <- fileList) {
        val imageList = new ListBuffer[Tuple2[Int, Array[Byte]]]()

        print(file.getName() + "    ")
        //println("zcoord:  " + file.getName().substring(0, file.getName().indexOf("_")).toInt)

        println("ImageIO.read(file)")
        val image = ImageIO.read(file)

        println("ByteArrayOutputStream = new ByteArrayOutputStream")
        val baos: ByteArrayOutputStream = new ByteArrayOutputStream

        println("ImageIO.write")
        ImageIO.write(image, "jpg", baos)
        println("baos.flush")
        baos.flush

        // get zcoord from file name 0_img.jpg
        var zcoord: Int = file.getName().substring(0, file.getName().indexOf("_")).toInt
        println("baos.toByteArray")
        var imageChuck: Array[Byte] = baos.toByteArray
        baos.close

        println("adding Tuple2 to list")
        imageList += (new Tuple2(zcoord, imageChuck))

        println("initialize parallelize ")
        val data = sc.parallelize(imageList)

        if (i == 0) {
          println("union first RDD")
          unionData = data
        }
        else {
          println("union non-first RDD")
          unionData = unionData.union(data)
        }

        i += 1

      }
    }
    catch{

      case unknown: Throwable => println("Got this unknown exception : " + unknown.printStackTrace())
    }

    //println("imageList count: " + imageList.length)

//    println("begining of Initialize spark Context")
//    val conf = new SparkConf().setAppName("Nidan ImageSplitter")
//    println("After spark conf")
//    val sc = new SparkContext(conf)
//    println("sc spark context ")
//    println("didnt initialize parallelize ")
//    val data = sc.parallelize(imageList)
//    println("initialize parallelize ")

    //call map
    val input = unionData.map{case (x,y) => (x.get(),y.getBytes)}
    print( " Input Length " + input.count())
    input.persist(StorageLevels.MEMORY_AND_DISK)

    if (usePartition.equalsIgnoreCase("Y"))
    {
      println("using range partition to store")
      val tunedPartitioner = new RangePartitioner(args(3).toInt, input)
      val partitioner = input.partitionBy(tunedPartitioner).persist(StorageLevels.MEMORY_AND_DISK)
      partitioner.saveAsSequenceFile(hdfsFileName)
    }
    else
    {
      println("not using range partition to store")
      input.saveAsSequenceFile(hdfsFileName)
    }

    println("End-Time = " + Calendar.getInstance().getTime())

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
