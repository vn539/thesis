import com.ning.compress.lzf.LZFDecoder;
import com.ning.compress.lzf.LZFEncoder;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * Created by vinu on 12/27/15.
 */
public class TestLZFUncompress
{

    public static void main(String[] args) throws Exception
    {
        // args[0] = compressed input full path and file name
        // args[1] = output full path and file name
        System.out.println("args[0] =  " + args[0]);
        System.out.println("args[1] =  " + args[1]);

//        File file = new File(args[0]); // compressed image
//        FileInputStream fis = new FileInputStream(file);
//        BufferedImage image = ImageIO.read(fis); //reading the compressed image file
//
//        ByteArrayOutputStream baos = new ByteArrayOutputStream();
//        ImageIO.write(image, "jpg", baos);
//        baos.flush();
//
//        byte[] uncompressed = LZFDecoder.decode(baos.toByteArray());

        byte[] uncompressed = LZFDecoder.decode(read(args[0]));

        ByteArrayInputStream bais = new ByteArrayInputStream(uncompressed);
        BufferedImage bufferedImage = ImageIO.read(bais);
        ImageIO.write(bufferedImage, "jpg", new File(args[1]));

//        baos.close();

    }

    /** Read the given binary file, and return its contents as a byte array.*/
    private static byte[] read(String aInputFileName){
//        log("Reading in binary file named : " + aInputFileName);
        File file = new File(aInputFileName);
//        log("File size: " + file.length());
        byte[] result = new byte[(int)file.length()];
        try {
            InputStream input = null;
            try {
                int totalBytesRead = 0;
                input = new BufferedInputStream(new FileInputStream(file));
                while(totalBytesRead < result.length){
                    int bytesRemaining = result.length - totalBytesRead;
                    //input.read() returns -1, 0, or more :
                    int bytesRead = input.read(result, totalBytesRead, bytesRemaining);
                    if (bytesRead > 0){
                        totalBytesRead = totalBytesRead + bytesRead;
                    }
                }
        /*
         the above style is a bit tricky: it places bytes into the 'result' array;
         'result' is an output parameter;
         the while loop usually has a single iteration only.
        */
//                log("Num bytes read: " + totalBytesRead);
            }
            finally {
//                log("Closing input stream.");
                input.close();
            }
        }
        catch (FileNotFoundException ex) {
//            log("File not found.");
        }
        catch (IOException ex) {
//            log(ex);
        }
        return result;
    }
}
