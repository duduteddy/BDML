import java.io.BufferedWriter;
import java.io.File;
import java.io.FileOutputStream;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.util.Random;

public class CalculateNumbers {
    public static void main(String[] args) throws IOException {

        File file = new File("/Users/Eddy/nyu/Big data and ML Systems/java/calculatenumbers/MatrixNumbers.txt");
        if (!file.exists()) {
            file.createNewFile();
        }
        FileOutputStream fos = new FileOutputStream(file);
        OutputStreamWriter osw = new OutputStreamWriter(fos, "utf-8");
        BufferedWriter out = new BufferedWriter(osw);
        Random r = new Random();


        for (int i = 1; i<= 10; i++){
           for( int j =1;j<= 10 ; j++) {
               out.write("A");
               out.write(" ");
               out.write(Integer.toString(i));
               out.write(" ");
               out.write(Integer.toString(j));
               out.write(" ");
               out.write(Integer.toString(r.nextInt(20)));
               out.newLine();
               if (i % 20 == 0) {
                   out.flush();
               }
           }
        }

        for (int i = 1; i<= 10; i++){
            for( int j =1;j<= 10 ; j++) {
                out.write("B");
                out.write(" ");
                out.write(Integer.toString(i));
                out.write(" ");
                out.write(Integer.toString(j));
                out.write(" ");
                out.write(Integer.toString(r.nextInt(20)));
                out.newLine();
                if (i % 20 == 0) {
                    out.flush();
                }
            }
        }
        out.close();
    }
}
