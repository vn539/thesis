import com.ning.compress.lzf.LZFEncoder;

import javax.imageio.ImageIO;
import java.awt.image.BufferedImage;
import java.io.*;

/**
 * Created by vinu on 12/27/15.
 */
public class TestLZFCompress
{
    public static void main(String[] args) throws Exception
    {
        // args[0] = input full path and file name
        // args[1] = output full path and file name
        System.out.println("args[0] =  " + args[0]);
        System.out.println("args[1] =  " + args[1]);

        File file = new File(args[0]); // image to compress
        FileInputStream fis = new FileInputStream(file);
        BufferedImage image = ImageIO.read(fis); //reading the image file

        ByteArrayOutputStream baos = new ByteArrayOutputStream();
        ImageIO.write(image, "jpg", baos);
        baos.flush();

        byte[] compressed = LZFEncoder.encode(baos.toByteArray());
        baos.close();

//        ByteArrayInputStream bais = new ByteArrayInputStream(compressed);
//        BufferedImage bufferedImage = ImageIO.read(bais);
//        ImageIO.write(bufferedImage, "jpg", new File(args[1]));

        write(compressed, args[1]);

    }

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
