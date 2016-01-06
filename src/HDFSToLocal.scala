import java.util.Calendar
import javax.imageio.ImageIO
import java.io._

import com.ning.compress.lzf.LZFDecoder
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import org.apache.spark.{RangePartitioner, SparkContext, SparkConf}

/**
  * Created by vinu on 12/12/15.
  */
object HDFSToLocal {

  def main(args: Array[String]) {

    // args[0] = hdfs full path and file name to read
    // args[1] = output full path and file name for merge image, default jpg

    if (args.length < 2)
    {
      System.err.println("Usage: HDFSToLocal {hdfs filename} {local output path}")
      System.exit(1)
    }

    val hdfsFileName = args(0)
    val outputFileName = args(1)

    println("Start-Time = " + Calendar.getInstance().getTime())

    val conf = new SparkConf().setAppName("Nidan ImageSplitter")
    val sc = new SparkContext(conf)
    println("Start sc.sequenceFile() = " + Calendar.getInstance().getTime())
    val file = sc.sequenceFile(hdfsFileName, classOf[IntWritable],classOf[BytesWritable])

    println("Start map() = " + Calendar.getInstance().getTime())
    val rddData = file.map{case (x,y) => (x.get(), y.copyBytes())}.cache()

    val tunedPartitioner = new RangePartitioner(12, rddData)
    println("Before caching " + sc.getPersistentRDDs.size)
    val partitioner = rddData.partitionBy(tunedPartitioner).cache()
//
//    println("Before calling lookup()" + Calendar.getInstance().getTime())
//    val llokupRDD = rddData.lookup(18)
//    println("After calling lookup() " + Calendar.getInstance().getTime())


//    println("Before calling countByKey()" + Calendar.getInstance().getTime())
//       val keyMap = rddData.countByKey()
//         println("After calling countByKey() " + Calendar.getInstance().getTime())

    println("Start foreach = " + Calendar.getInstance().getTime())
    //rddData.foreach(f => {
    partitioner.foreach(f => {
      println("Key = " + f._1.toString())
      println("Start write time = " + Calendar.getInstance().getTime())
      //      write(f._2, outputFileName + f._1.toString() + ".png")
      println("End write time = " + Calendar.getInstance().getTime())
      val binData = f._2
    })
    println("End foreach = " + Calendar.getInstance().getTime())

//    println("Start-collect time to list = " + Calendar.getInstance().getTime())
//    val sortedData = collectRDD.toList
//    println("end-collect time to list= " + Calendar.getInstance().getTime())
//
//    println("Start-write time = " + Calendar.getInstance().getTime())
//
//    // Print all the array elements
//    for ( x <- sortedData )
//    {
//    //  println( x._1 )
//     // println( x._2.length )
//
//   //   println("Begin compress time = " + Calendar.getInstance().getTime())
//   //   val uncompressed = LZFDecoder.decode(x._2);
////      println("Done compress time = " + Calendar.getInstance().getTime())
//
////      val bais: ByteArrayInputStream = new ByteArrayInputStream(x._2)
//////      val bais: ByteArrayInputStream = new ByteArrayInputStream(uncompressed)
////      val bufferedImage = ImageIO.read(bais)
////      ImageIO.write(bufferedImage, "jpg", new File(outputFileName + x._1.toString() + ".jpg"))
//
//      write(x._2, outputFileName + x._1.toString() + ".png")
//
//    }

//    println("End-write time = " + Calendar.getInstance().getTime())

    println("End-Time = " + Calendar.getInstance().getTime())
  }

  def write(aInput: Array[Byte], aOutputFileName: String) {
    try {
      var output: OutputStream = null
      try {
        output = new BufferedOutputStream(new FileOutputStream(aOutputFileName))
        output.write(aInput)
      } finally {
        output.close
      }
    }
    catch {
      case ex: FileNotFoundException => {
        ex.printStackTrace
      }
      case ex: IOException => {
        ex.printStackTrace
      }
    }
  }
}
