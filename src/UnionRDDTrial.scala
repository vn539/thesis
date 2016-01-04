/**
  * Created by vinu on 12/16/15.
  */

import java.io._
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
import org.apache.spark.storage.StorageLevel
import scala.collection.mutable._


  /**
    * Created by vinu on 12/8/15.
    */
  object UnionRDDTrial {

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
      //var unionData: RDD[(Int, Array[Byte])] = null
      var firstRDD: String = "Y"
      val imageList = new ListBuffer[Tuple2[Int, Array[Byte]]]()

      // for each file
      try {
       for (file <- fileList) {
          print("file.getName() - " + file.getName() + "    ")
          println("zcoord:  " + file.getName().substring(0, file.getName().indexOf("_")).toInt)

//          println("ImageIO.read(file) - " + file.getName())
//          var image = ImageIO.read(file)
//
//          println("ByteArrayOutputStream = new ByteArrayOutputStream")
//          var baos: ByteArrayOutputStream = new ByteArrayOutputStream
//
//          println("ImageIO.write")
//          ImageIO.write(image, "jpg", baos)
//          println("baos.flush")
//          baos.flush

          // get zcoord from file name 0_img.jpg
          var zcoord: Int = file.getName().substring(0, file.getName().indexOf("_")).toInt
//          println("baos.toByteArray")
//          var imageChuck: Array[Byte] = baos.toByteArray
         var imageChuck: Array[Byte] = read(file)
//          baos.close

          println("adding Tuple2 to list" + Calendar.getInstance().getTime())
          imageList += (new Tuple2(zcoord, imageChuck))

//          println("initialize parallelize ")
//          val data = sc.parallelize(imageList)
        //  data.persist(org.apache.spark.storage.StorageLevel.DISK_ONLY)

//         if (firstRDD.equalsIgnoreCase("Y")) {
//           println("union first RDD")
//           unionData = data
//           firstRDD = "N"
//         }
//         else {
//           println("union non-first RDD")
//           unionData = unionData.union(data)
//         }

        }

       println("initialize parallelize " + Calendar.getInstance().getTime())
       val data = sc.parallelize(imageList)
       println("done parallelize " + Calendar.getInstance().getTime())
       val input = data.map{case (x,y) => (x.get(),y.getBytes)}
       println("done map() " + Calendar.getInstance().getTime())
       // print( " Input Length " + input.count())
      // input.sortByKey(true).persist(StorageLevel.MEMORY_ONLY)
//       input.persist()
//       println("done persist() " + Calendar.getInstance().getTime())

       if (usePartition.equalsIgnoreCase("Y"))
       {
         println("using range partition to store")
         val tunedPartitioner = new RangePartitioner(args(3).toInt, input)
         println("Before caching " + sc.getPersistentRDDs.size)
         val partitioner = input.partitionBy(tunedPartitioner).cache()
         println("After caching " + sc.getPersistentRDDs.size)
         partitioner.saveAsSequenceFile(hdfsFileName)
       }
       else
       {
         println("not using range partition to store - " + Calendar.getInstance().getTime())
         val sortedInput = input.sortByKey(true) //.persist(StorageLevel.MEMORY_ONLY)
         println("Before caching " + sc.getPersistentRDDs.size)
         val cacheRDD = sortedInput.cache()
         println("After caching " + sc.getPersistentRDDs.size)
         cacheRDD.saveAsSequenceFile(hdfsFileName)
       }

       println("End-Time = " + Calendar.getInstance().getTime())

      }
      catch {
        case unknown: Throwable => {
          println("An exception occured")
          unknown.printStackTrace()
        }
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
  }



