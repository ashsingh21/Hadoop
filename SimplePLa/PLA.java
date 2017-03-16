import java.util.concurrent.ThreadLocalRandom;

/**
 * Created by ashu on 2/4/2017.
 */
public class PLA {

    public static void main(String... args) {

        // Training Data
        float[][] data = {{-10,-2}, {9, 15}, {15,1}, {-1,-1}, {-1,-5}, {5, -6}, {-3,9}, {1,26}, {-1,15},{2,-6},{-1,-10},{0,0},
                {3, 5}, {5, 6}, {8,9},  {-1,-2}, {1,1},{5, 6}, {8,9}, {1,8},};
        int[] d = {-1,1,1,-1,-1,1,-1,1,-1,-1,-1,-1,1,1,1,-1,-1,1,1,1,};

        // calculated weights
        float[] w = learnPLA(data,d,1,3,50);

        // Test data
        float[][] testData = {{-1,15},{-20,-2},{2,9},{8,6},{-8,-6},{9,2},{-8,2},{-9,-1},{6,1},{-9,-12}};
        int[] testD = {-1,-1,1,1,-1,-1,1,-1,1,-1};

        // Test PLA on test data
         testPLA(w,testData,testD);

    }


    static void testPLA(float[] calculatedWeights, float[][] testData, int[] label){

           double error = 0;

           for(int i = 0; i < testData.length; i++){
               double D = calculateD(calculatedWeights,testData[i]);
               if(sign(D,label[i]) == 0) error++;
           }

           System.out.println("Error Rate on test data: " + error/testData.length);
    }


    static float[] learnPLA(float[][] data,int[] d, float k, float c, int numberOfIterations) {
        int weightUpdates = 0;
        float[] weights = {2,8,1};

        double errorRate = 0;
        int i = 0;
        int index = 0;
        double errorCount = 0;

        while (i <= numberOfIterations) {

            index = index % data.length;
            double D = calculateD(weights, data[index]);
            if (sign(D, d[index]) == 0) {
                errorCount++;
                weightUpdates++;
                updateWeights(weights, data[index], k, c, d[index]);
            }

            if (index == data.length - 1) {
                errorRate = errorCount / data.length;
                if (errorRate <= 0.2) break;
                else errorCount = 0;
            }

            index++;
            i++;
        }

        String equation = createEquation(weights);

        System.out.println("Equation: " + equation + ", Error Rate: " + errorRate +
              "\n"  + "Number of times weights updated: " + weightUpdates);
        System.out.println("Number of iterations: " + i);

        return weights;
    }


    /******** helper methods ************/


    static void updateWeights(float[] w, float[] x, float k, float c, float d) {
        checkLengths(w, x);
        w[0] += c * d * k;
        for (int i = 0; i < x.length; i++) {
            w[i + 1] += x[i] * c * d;
        }
    }

    static double calculateD(float[] w, float[] x) {
        checkLengths(w, x);
        float D = 0;
        for (int i = 0; i < x.length; i++) {
            D += w[i + 1] * x[i];
        }
        return w[0] + D;
    }

    static void checkLengths(float[] w, float[] x) {
        if (w.length != x.length + 1) throw new IllegalArgumentException("arrays length mismatch");
    }


    static int sign(double a, float b) {
        if ((a >= 0 && b >= 0) || (a < 0 && b < 0)) return 1;
        else return 0;
    }

    static String createEquation(float[] w) {
        String s = "";
        for (int i = 1; i < w.length; i++) {
            s += w[i] + "x" + i + " + ";
        }
        s = w[0] + " + " + s.substring(0, s.length() - 2);
        return s;
    }

    static void printArr(String s, float[] arr) {
        System.out.print(s);
        for (float a : arr) {
            System.out.print(a + " ");
        }
        System.out.println();
    }
}
