import java.util.Random;

/**
 * Created by ashu on 3/18/2017.
 *
 * Goal here is to minimize the value of f(X) where X = x1^2 + x2^2.....+ xn^2 where min <= xi <= max
 * for i = 1,2,3,4...n
 *
 */

public class SimulatedAnnealing {

    private double Temp;
    private double cooling;
    private int max, min;
    private int[] variables;

    private Random random;

    SimulatedAnnealing(double Temp, double cooling, int min, int max, int length) {
        this.Temp = Temp;
        this.cooling = cooling;
        random = new Random(100);
        this.max = max;
        this.min = min;
        variables = new int[length];
    }

    SimulatedAnnealing(int min, int max) {
        this(10000, 0.1, min, max, 3);
    }

    private int generateRandom(int min, int max) {
        int rand = random.nextInt((max - min) + 1) + min;
        return rand;
    }

    // initialize the array with random variables
    private void initialize() {
        for (int i = 0; i < variables.length; i++) {
            variables[i] = generateRandom(min, max);
        }
    }

    public int[] anneal() {
        initialize();
        int oldCost = calulateVal(variables);
        while (Temp > 1) {
            for (int i = 0; i < 500; i++) {
                int[] newSolution = neighbors(variables);
                int newCost = calulateVal(newSolution);
                // update old sol if new solution is better or acceptance probability is greater than random number generated
                // random number generated here is between 0 and 1
                if(newCost < oldCost || random.nextDouble() < acceptProbability(oldCost,newCost,Temp) ){
                    oldCost = newCost;
                    variables = newSolution;
                }
            }

            Temp = (1 - cooling) * Temp;
        }
        printArray(variables);
        return variables;

    }

    // calculate neighbors (naive way)
    private int[] neighbors(int[] solution) {
        int[] newSolution = new int[solution.length];
        for (int i = 0; i < solution.length; i++) {
            newSolution[i] = solution[i] + generateRandom(min, max);
            //clip value so that it doesn't exceed bounds
            if (newSolution[i] > 10 || newSolution[i] < -10) newSolution[i] = generateRandom(min,max);
        }
        return newSolution;
    }

    // acceptance probability
    private double acceptProbability(int oldSol, int newSol, double temp) {
        return Math.exp(-(newSol - oldSol) / temp);
    }

    private void printArray(int[] arr) {
        for (int a : arr) {
            System.out.print(a + " ");
        }
        System.out.println();
    }

    private int calulateVal(int[] arr) {
        int val = 0;
        for (int var : arr) {
            val += Math.pow(var, 2);
        }
        return val;
    }

    public static void main(String... args) {
        SimulatedAnnealing annealing = new SimulatedAnnealing(-10, 10);
        annealing.anneal();
    }
}
