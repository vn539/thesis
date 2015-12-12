import org.apache.spark.SparkConf;
import java.util.ArrayList;
import java.util.List;
import scala.Tuple2;
import org.apache.spark.api.java.JavaPairRDD;
import org.apache.spark.api.java.JavaSparkContext;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.spark.api.java.function.PairFunction;
import org.apache.hadoop.mapred.SequenceFileOutputFormat;
import scala.collection.Iterator;

import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStream;
import javax.imageio.ImageIO;

/**
 * Created by vinu on 12/5/15.
 */
public class ImageMerge {

    //convert the seq file contents to byte[] for reading the image
    /*
    public static class ConvertToNativeTypes implements
            PairFunction<Tuple2<IntWritable, BytesWritable>, Integer, byte[]>{

        public Tuple2<Integer, byte[]> call(Tuple2<IntWritable, BytesWritable> record){
            return new Tuple2(record._1.get(), record._2.copyBytes());
        }
    }
    */

    public static void main(String[] args) throws Exception
    {
        /*
        SparkConf conf = new SparkConf().setAppName("SampleSequenceFile Application");
        JavaSparkContext sc = new JavaSparkContext(conf);

        String outfileName = "hdfs://localhost:9000/outimage.jpg";
        List<Tuple2<Integer, byte[]>> input = new ArrayList();

        //-----------------------------------------------------------------------------
        //Reading the saved hadoop file
        JavaPairRDD<IntWritable, BytesWritable> seqinput = sc.sequenceFile(outfileName, IntWritable.class, BytesWritable.class);
        JavaPairRDD<Integer, byte[]> seqresult= seqinput.mapToPair(new ConvertToNativeTypes());

        List<Tuple2<Integer, byte[]>> sortedRDD = seqresult.sortByKey(true).collect();
        System.out.println(".......................................sortedRDD.size() : " + sortedRDD.size());


        // loop through each item in sortedRDD and concatenate the byte[] values
        ByteArrayOutputStream outputStream = new ByteArrayOutputStream();
        for(int k=0;k<sortedRDD.size();k++)
        {
            System.out.println(".......................................Sorted key : " + sortedRDD.get(k)._1().intValue());
            System.out.println(".......................................Value length : " + sortedRDD.get(k)._2().length);
            //outputStream.write(sortedRDD.get(k)._2());
            write(sortedRDD.get(k)._2(), "/home/vinu/Desktop/finaloutmsg_" + sortedRDD.get(k)._1().intValue() + ".jpg");
        }
        */


        /**/
        // args[0] = hdfs full path and file name to read
        // args[1] = output full path and file name for merge image, default jpg
        String fileName = args[0];
        String outputFileName = args[1];
        SparkSequence ss = new SparkSequence();
        List<ImageSplits> imageSplits =  ss.getImageSplits(fileName);
        System.out.println("imageSplits.size() =  " + imageSplits.size());

        for (ImageSplits imageSplit : imageSplits)
        {
            write(imageSplit.splitImage, outputFileName + imageSplit.zcoord + ".jpg");
        }
        /**/

    }

    /**
     Write a byte array to the given file.
     Writing binary data is significantly simpler than reading it.
     */
    public static void write(byte[] aInput, String aOutputFileName){
        try {
            OutputStream output = null;
            try {
                output = new BufferedOutputStream(new FileOutputStream(aOutputFileName));
                output.write(aInput);
            }
            finally {
                output.close();
            }
        }
        catch(FileNotFoundException ex){
            ex.printStackTrace();
        }
        catch(IOException ex){
            ex.printStackTrace();
        }
    }

}
