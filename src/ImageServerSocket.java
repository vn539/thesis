import java.io.*;
import java.net.ServerSocket;
import java.net.Socket;
import java.util.ArrayList;
import java.util.List;

/**
 * Created by vinu on 1/7/16.
 */
public class ImageServerSocket
{
    public static void main(String[] args) throws IOException {

        ServerSocket serverSocket = null;
        List<ImageSplits> imageSplits = new ArrayList<ImageSplits>();

        try {
            serverSocket = new ServerSocket(5432);
            System.out.println("Server started at port: " + serverSocket.toString());

            while (true)
            {
                Socket clientSocket = serverSocket.accept();
                System.out.println("Got connection from " + clientSocket.getInetAddress() + " at port: " + clientSocket.getPort());

                byte[] bytes = new byte[18];

                InputStream in = clientSocket.getInputStream();
                in.read(bytes);
                String requestType = new String(bytes);
                System.out.println("first read = " + requestType);

                bytes = new byte[16*1024];

                if (requestType.startsWith("Store for key"))
                {
                    //OutputStream out = new FileOutputStream("/users/vn539/OutputSplits/img_test.png");
                    ByteArrayOutputStream out = new ByteArrayOutputStream();
                    int count;
                    while ((count = in.read(bytes)) > 0) {
                        out.write(bytes, 0, count);
                    }

                    ImageSplits imageSplit = new ImageSplits();
                    imageSplit.splitImage = out.toByteArray();
                    System.out.println("imageSplit.splitImage.length = " + imageSplit.splitImage.length);

                    String zcoord = requestType.substring(requestType.indexOf("= ") + 2);
                    System.out.println("zcoord = " + zcoord);
                    imageSplit.zcoord = Integer.parseInt(zcoord);

                    imageSplits.add(imageSplit);
                    out.close();
                }
                else if(requestType.startsWith("Get for key"))
                {
//                    System.out.println("imageSplitData.length = " + imageSplitData.length);
//                    OutputStream out = new BufferedOutputStream(clientSocket.getOutputStream());
//                    out.write(imageSplitData);
//                    out.close();

                    System.out.println("Sending sample-png.png from server to client");
                    OutputStream out = clientSocket.getOutputStream();
                    out.write(read("/users/vn539/Test2/0_img.png"));
                    out.close();

                }

                in.close();
                clientSocket.close();
            }


        } catch (Exception ex) {
            System.out.println("An exception occured in ImageServerSocket class.");
            ex.printStackTrace();
        }
        finally {

            serverSocket.close();
        }
    }

    /** Read the given binary file, and return its contents as a byte array.*/
    private static byte[] read(String aInputFileName){
        File file = new File(aInputFileName);
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
            }
            finally {
                input.close();
            }
        }
        catch (FileNotFoundException ex) {
        }
        catch (IOException ex) {
        }
        return result;
    }
}
