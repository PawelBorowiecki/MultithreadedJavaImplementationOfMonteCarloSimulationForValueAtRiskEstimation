package parallel1;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.SplittableRandom;
import java.util.concurrent.CountDownLatch;

public class PathSimulationThread implements Runnable{
    private final BigDecimal[][] prices;
    private final BigDecimal[] portfolioValues, weights, annualDrifts, annualVolatilities, losses;
    private final BigDecimal portfolioInitialValue;
    private final double[][] decomposedMatrix;
    private final double timeHorizon;
    private final int assetQuantity, startIndex, endIndex;
    private final SplittableRandom random;
    private final CountDownLatch latch;
    private final MathContext mathContext;

    public PathSimulationThread(BigDecimal[][] prices, BigDecimal[] portfolioValues, BigDecimal[] weights, BigDecimal[] annualDrifts, BigDecimal[] annualVolatilities, BigDecimal[] losses, BigDecimal portfolioInitialValue, double[][] decomposedMatrix, double timeHorizon, int assetQuantity, int startIndex, int endIndex, SplittableRandom random, CountDownLatch latch, MathContext mathContext) {
        this.prices = prices;
        this.portfolioValues = portfolioValues;
        this.weights = weights;
        this.annualDrifts = annualDrifts;
        this.annualVolatilities = annualVolatilities;
        this.losses = losses;
        this.portfolioInitialValue = portfolioInitialValue;
        this.decomposedMatrix = decomposedMatrix;
        this.timeHorizon = timeHorizon;
        this.assetQuantity = assetQuantity;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.random = random;
        this.latch = latch;
        this.mathContext = mathContext;
    }

    @Override
    public void run() {
        for(int j = startIndex; j <= endIndex; j++){
            BigDecimal portfolioValue = BigDecimal.ZERO;
            double[] independentStandardNormalVariables = createIndependentStandardNormalVariables(random, assetQuantity);
            double[] correlatedMatrix = multiplyMatrixByVector(decomposedMatrix, independentStandardNormalVariables);
            for(int i = 0; i < assetQuantity; i++){
                double drift = (annualDrifts[i].doubleValue() - 0.5 * annualVolatilities[i].doubleValue() * annualVolatilities[i].doubleValue()) * timeHorizon;
                double diffusion = annualVolatilities[i].doubleValue() * Math.sqrt(timeHorizon) * correlatedMatrix[i];
                double simulatedFuturePrice = prices[i][prices[i].length-1].doubleValue() * Math.exp(drift + diffusion);
                portfolioValue = portfolioValue.add(BigDecimal.valueOf(simulatedFuturePrice).multiply(weights[i], mathContext));
            }
            portfolioValues[j] = portfolioValue;
            losses[j] = portfolioInitialValue.subtract(portfolioValue);
        }
        latch.countDown();
    }

    private double[] createIndependentStandardNormalVariables(SplittableRandom random, int assetQuantity){
        double[] result = new double[assetQuantity];
        for(int i = 0; i < assetQuantity; i++){
            result[i] = boxMullerTransformation(random);
        }
        return result;
    }

    private double boxMullerTransformation(SplittableRandom random){
        double u1 = random.nextDouble();
        double u2 = random.nextDouble();
        return Math.sqrt(-2.0 * Math.log(u1)) * Math.cos(2.0 * Math.PI * u2);
    }

    private double[] multiplyMatrixByVector(double[][] matrix, double[] vector){
        int size = vector.length;
        double[] result = new double[size];
        for(int i = 0; i < size; i++){
            double sum = 0.0;
            for(int j = 0; j < size; j++){
                sum += matrix[i][j] * vector[j];
            }
            result[i] = sum;
        }
        return result;
    }
}
