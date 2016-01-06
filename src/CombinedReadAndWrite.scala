import java.io._
import java.util.Calendar

import org.apache.hadoop.io.{BytesWritable, IntWritable}
import org.apache.spark._
import org.apache.spark.SparkContext._
import org.apache.spark.RangePartitioner
import scala.collection.mutable._

/**
  * Created by vinu on 1/1/16.
  */
object CombinedReadAndWrite {

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

    println("args(0) = " + args(0))   // split image directory
    println("args(1) = " + args(1))   // HDFS full path and file name to store

    // get list of files from the specified directory passed as args(0)
    val fileList = getListOfFiles(args(0))
    println("fileList.size = " + fileList.size)
    val hdfsFileName = args(1)
    var usePartition: String = "N" // default is to not use Partition

    if (args.length == 4)
    {
      println("args(2) = " + args(2))   // use partition = Y/N
      usePartition = args(2)
    }

    println("begining of Initialize spark Context")
    val conf = new SparkConf().setAppName("Nidan ImageSplitter")
    val sc = new SparkContext(conf)
    println("After spark conf")
    var firstRDD: String = "Y"
    val imageList = new ListBuffer[Tuple2[Int, Array[Byte]]]()

    // for each file
    try {
      for (file <- fileList) {
        print("file.getName() - " + file.getName() + "    ")
        println("zcoord:  " + file.getName().substring(0, file.getName().indexOf("_")).toInt)

        // get zcoord from file name 0_img.jpg
        var zcoord: Int = file.getName().substring(0, file.getName().indexOf("_")).toInt
        var imageChuck: Array[Byte] = read(file)

        println("adding Tuple2 to list" + Calendar.getInstance().getTime())
        imageList += (new Tuple2(zcoord, imageChuck))
      }

      println("initialize parallelize " + Calendar.getInstance().getTime())
      val data = sc.parallelize(imageList)
      println("done parallelize " + Calendar.getInstance().getTime())
      val input = data.map{case (x,y) => (x.get(),y.getBytes)}
      println("done map() " + Calendar.getInstance().getTime())

      if (usePartition.equalsIgnoreCase("Y"))
      {
        println("using range partition to store")
        val tunedPartitioner = new RangePartitioner(args(3).toInt, input)
        println("Before caching " + sc.getPersistentRDDs.size)
        val partitioner = input.partitionBy(tunedPartitioner).cache()
        println("After caching " + sc.getPersistentRDDs.size)
      //  println("start collect time before sequence file " + Calendar.getInstance().getTime())
       // val rangebeforesequencefile=partitioner.collectAsMap()
        //println("end collect time before sequence file " + Calendar.getInstance().getTime())
        partitioner.saveAsSequenceFile(hdfsFileName)



      }
      else
      {
        println("not using range partition to store - " + Calendar.getInstance().getTime())
       // val sortedInput = input.sortByKey(true) //.persist(StorageLevel.MEMORY_ONLY)
        println("Before caching " + sc.getPersistentRDDs.size)
        val cacheRDD = input.cache()
        println("After caching " + sc.getPersistentRDDs.size)

        cacheRDD.saveAsSequenceFile(hdfsFileName)
      }

      println("End-Time = " + Calendar.getInstance().getTime())

      // do some clean up of memory
      imageList.clear()

      // code from HDFSToLocal.scala file
      println("Start sc.sequenceFile() = " + Calendar.getInstance().getTime())
      val file = sc.sequenceFile(hdfsFileName, classOf[IntWritable],classOf[BytesWritable])
      println("Start map() = " + Calendar.getInstance().getTime())
      val rddData = file.map{case (x,y) => (x.get(), y.copyBytes())}.cache()

      val persistedRDDs = sc.getPersistentRDDs
      println("persistedRDDs size " + persistedRDDs.size)
      println("Before calling lookup()" + Calendar.getInstance().getTime())
      val llokupRDD = rddData.lookup(10)
      println("After calling lookup() " + Calendar.getInstance().getTime())

//      println("Before calling countByKey()" + Calendar.getInstance().getTime())
//      val keyMap = rddData.countByKey()
//      println("After calling countByKey() " + Calendar.getInstance().getTime())
     // println("Start-collect time = " + Calendar.getInstance().getTime())

      //val valuesRDD=rddData.values
      //println("end-collect time = " + Calendar.getInstance().getTime())

     // println("Start-value time = " + Calendar.getInstance().getTime())
      //val collectRDD=rddData.collectAsMap()

      //val valuesRDD=rddData.values

     // println("end-value time = " + Calendar.getInstance().getTime())

      println("Start foreach = " + Calendar.getInstance().getTime())
      rddData.foreach(f => {
        println("Key = " + f._1.toString())
        println("Start write time = " + Calendar.getInstance().getTime())
        //      write(f._2, outputFileName + f._1.toString() + ".png")
        println("End write time = " + Calendar.getInstance().getTime())
        val binData = f._2
      })
      println("End foreach = " + Calendar.getInstance().getTime())

//      println("Start-collect time to list = " + Calendar.getInstance().getTime())
//      val sortedData = rddData.collect().toList
//      println("end-collect time to list= " + Calendar.getInstance().getTime())
//      println("Start-write time = " + Calendar.getInstance().getTime())
//
//     //Print all the array elements
//     for ( x <- sortedData )
//      {
//        //  println( x._1 )
//        // println( x._2.length )
//        write(x._2, "/users/vn539/OutputSplits/img_" + x._1.toString() + ".png")
//      }
//
//      println("Start toLocalIterator time to list = " + Calendar.getInstance().getTime())
//      val it = rddData.toLocalIterator
//      while (it.hasNext)
//      {
//        val keyval = it.next()
//        println("keyval._1.toString" + keyval._1.toString)
////        write(keyval._2, "/users/vn539/OutputSplits/img_" + keyval._1.toString() + ".png")
//        val binData = keyval._2
//      }
//      println("End toLocalIterator time to list = " + Calendar.getInstance().getTime())

      println("End-write time = " + Calendar.getInstance().getTime())
    }
    catch {
      case unknown: Throwable => {
        println("An exception occured")
        unknown.printStackTrace()
      }
    }
  }

  def getListOfFiles(dir: String):List[File] = {
    val d = new File(dir)
    println("d.exists = " + d.exists)
    println("d.isDirectory = " + d.isDirectory)
    if (d.exists && d.isDirectory) {
      //d.listFiles.filter(_.isFile).toList
      println("get list of .png")
      d.listFiles.filter(f => f.isFile && f.getName().endsWith(".png")).toList
    } else {
      println("empty list of .png")
      List[File]()
    }
  }

  def read(file: File) : Array[Byte] = {
    //      val file: File = new File(fileName)
    val result: Array[Byte] = new Array[Byte](file.length.toInt)

    try {
      var input: InputStream = null
      try {
        var totalBytesRead: Int = 0
        input = new BufferedInputStream(new FileInputStream(file))
        while (totalBytesRead < result.length) {
          val bytesRemaining: Int = result.length - totalBytesRead
          val bytesRead: Int = input.read(result, totalBytesRead, bytesRemaining)
          if (bytesRead > 0) {
            totalBytesRead = totalBytesRead + bytesRead
          }
        }
      } finally {
        input.close
      }
    }
    catch {
      case ex: FileNotFoundException => {
        println("FileNotFoundException exception")
      }
      case ex: IOException => {
        println("IOException exception")
      }
    }
    return result
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
