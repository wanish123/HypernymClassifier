import java.io.BufferedReader;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.IOException;
import java.util.LinkedList;
import java.util.List;
import java.util.StringTokenizer;

/**
 * Created by talwanich on 25/07/2016.
 */
public class LocalMain {
    public static void main(String[] args) {

        BufferedReader br = null;
        List<NounPair> nounPairList = new LinkedList<NounPair>();
        int sum = 0;

        try {

            String sCurrentLine;

            br = new BufferedReader(new FileReader("annotated_set.txt"));


            while ((sCurrentLine = br.readLine()) != null) {
                String[] parts = sCurrentLine.split("\\t");
                if(parts[2].equals("True")) {
                    sum += 1;
                    NounPair nounPair = new NounPair(parts[0], parts[1]);
                    nounPairList.add(nounPair);
                }
            }



        } catch (IOException e) {
            e.printStackTrace();
        } finally {
            try {
                if (br != null)br.close();
            } catch (IOException ex) {
                ex.printStackTrace();
            }
        }


//        for(NounPair pair : nounPairList){
//            System.out.print(pair + " ,");
//        }


        System.out.println("Sum: " + sum);


    }
}
