import java.util.Calendar
import javax.imageio.ImageIO
import java.io._

import com.ning.compress.lzf.LZFDecoder
import org.apache.hadoop.io.{BytesWritable, IntWritable}
import org.apache.spark.{SparkContext, SparkConf}

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
    val rddData = file.map{case (x,y) => (x.get(), y.copyBytes())}
   // val sortedData = rddData.sortByKey(true).collect().toList
    println("Start persistant time = " + Calendar.getInstance().getTime())
    val persistedRDD = sc.getPersistentRDDs.size
    println("RDD size " + sc.getPersistentRDDs.size)
    println("End persistant time = " + Calendar.getInstance().getTime())
    println("Start-collect time = " + Calendar.getInstance().getTime())
    val collectRDD=rddData.collect()
    println("end-collect time = " + Calendar.getInstance().getTime())
    //print("sortedData.length = ")
    //println(sortedData.length)

    println("Start-collect time to list = " + Calendar.getInstance().getTime())
    val sortedData = collectRDD.toList
    println("end-collect time to list= " + Calendar.getInstance().getTime())

    println("Start-write time = " + Calendar.getInstance().getTime())

    // Print all the array elements
    for ( x <- sortedData )
    {
    //  println( x._1 )
     // println( x._2.length )

   //   println("Begin compress time = " + Calendar.getInstance().getTime())
   //   val uncompressed = LZFDecoder.decode(x._2);
//      println("Done compress time = " + Calendar.getInstance().getTime())

//      val bais: ByteArrayInputStream = new ByteArrayInputStream(x._2)
////      val bais: ByteArrayInputStream = new ByteArrayInputStream(uncompressed)
//      val bufferedImage = ImageIO.read(bais)
//      ImageIO.write(bufferedImage, "jpg", new File(outputFileName + x._1.toString() + ".jpg"))

      write(x._2, outputFileName + x._1.toString() + ".png")

    }

    println("End-write time = " + Calendar.getInstance().getTime())
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
