
import java.util.ArrayList;
import java.util.List;
import java.io.BufferedInputStream;
import java.io.BufferedOutputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.*;
import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;
import java.awt.*;

public class ImageSplitter {


    private static String interleave(String icoord, String jcoord)
    {
        char x[] = icoord.toCharArray();
        char y[] = jcoord.toCharArray();

        StringBuilder sb = new StringBuilder();

        for (int i=0; i<x.length; i++)
        {
            sb.append(x[i]);
            sb.append(y[i]);
        }

        return sb.toString();
    }

    public static void main(String[] args) throws Exception {
            // args[0] = rows
            // args[1] = cols
            // args[2] = input full path and file name
            // args[3] = hdfs full path and file name
            File file = new File(args[2]); // Splitting an image
            FileInputStream fis = new FileInputStream(file);
            BufferedImage image = ImageIO.read(fis); //reading the image file

            int rows = Integer.parseInt(args[0]); //You should decide the values for rows and cols variables
            int cols = Integer.parseInt(args[1]);
            int chunks = rows * cols;

            int chunkWidth = image.getWidth() / cols; // determines the chunk width and height
            int chunkHeight = image.getHeight() / rows;
            int count = 0;
            BufferedImage imgs[] = new BufferedImage[chunks]; //Image array to hold image chunks
            List<ImageSplits> imageSplits = new ArrayList<ImageSplits>();

            for (int x = 0; x < rows; x++) {
                for (int y = 0; y < cols; y++) {
                    //Initialize the image array with image chunks
                    imgs[count] = new BufferedImage(chunkWidth, chunkHeight, image.getType());

                    // draws the image chunk
                    Graphics2D gr = imgs[count].createGraphics();
                    gr.drawImage(image, 0, 0, chunkWidth, chunkHeight, chunkWidth * y, chunkHeight * x, chunkWidth * y + chunkWidth, chunkHeight * x + chunkHeight, null);
                    gr.dispose();

                    String icoord = String.format("%04d", Integer.parseInt(Integer.toBinaryString(x)));
                    String jcoord = String.format("%04d", Integer.parseInt(Integer.toBinaryString(y)));
                    String interleavevalue = interleave(icoord, jcoord);
                    ImageSplits imageSplit = new ImageSplits();

                    //Assign zcoord and splitimage, the value of image
                    imageSplit.zcoord = new Integer(Integer.parseInt(interleavevalue, 2)).intValue();
                    // convert BufferedImage to byte arrary
                    ByteArrayOutputStream baos = new ByteArrayOutputStream();
                    ImageIO.write(imgs[count], "jpg", baos);
                    baos.flush();
                    imageSplit.splitImage = baos.toByteArray();
                    baos.close();

                    //System.out.println("imageSplit byte[] length....   " + imageSplit.splitImage.length);

                    imageSplits.add(imageSplit);
                    count++;

                    //System.out.println(icoord + "," + jcoord);
                }
            }

            System.out.println("Splitting done");

            SparkPartitioner sp = new SparkPartitioner();
            sp.saveImage(imageSplits, args[3]);
    }
}
