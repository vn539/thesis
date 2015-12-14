import java.util.Calendar
import javax.imageio.ImageIO
import java.io.{ByteArrayInputStream, ByteArrayOutputStream, File}

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
    val file = sc.sequenceFile(hdfsFileName, classOf[IntWritable],classOf[BytesWritable])
    val rddData = file.map{case (x,y) => (x.get(), y.copyBytes())}
   // val sortedData = rddData.sortByKey(true).collect().toList
   val sortedData = rddData.collect().toList

    print("sortedData.length = ")
    println(sortedData.length)

    // Print all the array elements
    for ( x <- sortedData )
    {
      println( x._1 )
      println( x._2.length )

      val bais: ByteArrayInputStream = new ByteArrayInputStream(x._2)
      val bufferedImage = ImageIO.read(bais)
      ImageIO.write(bufferedImage, "jpg", new File(outputFileName + x._1.toString() + ".jpg"))
    }
    println("End-Time = " + Calendar.getInstance().getTime())

  }

}
