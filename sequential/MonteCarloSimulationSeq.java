package sequential;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.Arrays;
import java.util.Random;

public class MonteCarloSimulationSeq {
    private final MathContext mathContext = new MathContext(20, RoundingMode.HALF_UP);

    public BigDecimal runSimulation(BigDecimal[][] prices, BigDecimal[] weights, double confidenceLevel, double days, int simulationQuantity){
        Random random = new Random();
        double merchantDaysPerYear = 252.0;
        BigDecimal epsilon = BigDecimal.valueOf(1e-6);
        BigDecimal[] portfolioValues = new BigDecimal[simulationQuantity];
        double timeHorizon = days / merchantDaysPerYear;
        BigDecimal portfolioInitialValue = computePortfolioInitialValue(prices, weights);
        BigDecimal[][] logReturns = computeLogReturns(prices);
        BigDecimal[] empiricMeans = computeEmpiricMeans(logReturns);
        BigDecimal[] standardDeviations = computeStandardDeviations(logReturns, empiricMeans);
        BigDecimal[] annualVolatilities = computeAnnualVolatilities(logReturns, standardDeviations, merchantDaysPerYear);
        BigDecimal[] annualDrifts = computeAnnualDrifts(logReturns, empiricMeans, merchantDaysPerYear);
        BigDecimal[][] covarianceMatrix = createCovarianceMatrix(logReturns, empiricMeans);
        makePositiveDefiniteMatrix(covarianceMatrix, epsilon);
        double[][] decomposedMatrix = choleskyDecompose(covarianceMatrix);

        for(int i = 0; i < simulationQuantity; i++){
            portfolioValues[i] = simulatePath(random, prices.length, timeHorizon, prices, weights, decomposedMatrix, annualDrifts, annualVolatilities);
        }
        BigDecimal[] losses = computeLosses(portfolioValues, portfolioInitialValue, simulationQuantity);
        Arrays.sort(losses);
        return computeValueAtRisk(losses, confidenceLevel, simulationQuantity);
    }

    private BigDecimal computePortfolioInitialValue(BigDecimal[][] prices, BigDecimal[] weights){
        BigDecimal result = BigDecimal.ZERO;
        for(int i = 0; i < prices.length; i++){
            result = result.add(prices[i][prices[i].length-1].multiply(weights[i]));
        }
        return result;
    }

    private BigDecimal[][] computeLogReturns(BigDecimal[][] prices){
        BigDecimal[][] logReturns = new BigDecimal[prices.length][prices[0].length-1];
        for(int i = 0; i < prices.length; i++){
            for(int j = 1; j < prices[i].length; j++){
                logReturns[i][j-1] = BigDecimal.valueOf(Math.log(prices[i][j].divide(prices[i][j-1], mathContext).doubleValue()));
            }
        }
        return logReturns;
    }

    private BigDecimal[] computeEmpiricMeans(BigDecimal[][] logReturns){
        int empiricMeansSize = logReturns.length;
        BigDecimal[] empiricMeans = new BigDecimal[empiricMeansSize];
        for(int i = 0; i < empiricMeansSize; i++){
            empiricMeans[i] = mean(logReturns[i]);
        }
        return empiricMeans;
    }

    private BigDecimal mean(BigDecimal[] array){
        BigDecimal sum = BigDecimal.ZERO;
        for(BigDecimal element : array){
            sum = sum.add(element);
        }
        return sum.divide(BigDecimal.valueOf(array.length), mathContext);
    }

    private BigDecimal[] computeStandardDeviations(BigDecimal[][] logReturns, BigDecimal[] empiricMeans){
        int standardDeviationsSize = logReturns.length;
        BigDecimal[] standardDeviations = new BigDecimal[standardDeviationsSize];
        for(int i = 0; i < standardDeviationsSize; i++){
            standardDeviations[i] = standardDeviation(logReturns[i], empiricMeans[i]);
        }
        return standardDeviations;
    }

    private BigDecimal standardDeviation(BigDecimal[] array, BigDecimal mean){
        BigDecimal sum = BigDecimal.ZERO;
        for(BigDecimal element : array){
            sum = sum.add(element.subtract(mean).multiply(element.subtract(mean), mathContext));
        }
        return BigDecimal.valueOf(Math.sqrt(sum.divide(BigDecimal.valueOf(array.length-1), mathContext).doubleValue()));
    }

    private BigDecimal[] computeAnnualVolatilities(BigDecimal[][] logReturns, BigDecimal[] standardDeviations, double merchantDaysPerYear){
        int annualVolatilitiesSize = logReturns.length;
        BigDecimal[] annualVolatilities = new BigDecimal[annualVolatilitiesSize];
        for(int i = 0; i < annualVolatilitiesSize; i++){
            annualVolatilities[i] = standardDeviations[i].multiply(BigDecimal.valueOf(Math.sqrt(merchantDaysPerYear)));
        }
        return annualVolatilities;
    }

    private BigDecimal[] computeAnnualDrifts(BigDecimal[][] logReturns, BigDecimal[] empiricMeans, double merchantDaysPerYear){
        int annualDriftsSize = logReturns.length;
        BigDecimal[] annualDrifts = new BigDecimal[annualDriftsSize];
        for(int i = 0; i < annualDriftsSize; i++){
            annualDrifts[i] = empiricMeans[i].multiply(BigDecimal.valueOf(merchantDaysPerYear), mathContext);
        }
        return annualDrifts;
    }

    private BigDecimal[][] createCovarianceMatrix(BigDecimal[][] logReturns, BigDecimal[] empiricMeans){
        int assetQuantity = logReturns.length;
        int surveyQuantity = logReturns[0].length;
        BigDecimal[][] covarianceMatrix = new BigDecimal[assetQuantity][assetQuantity];

        for(int i = 0; i < assetQuantity; i++){
            for(int j = i; j < assetQuantity; j++){
                BigDecimal sum = BigDecimal.ZERO;
                for(int t = 0; t < surveyQuantity; t++){
                    sum = sum.add((logReturns[i][t].subtract(empiricMeans[i])).multiply(logReturns[j][t].subtract(empiricMeans[j])));
                }
                BigDecimal ratio = sum.divide(BigDecimal.valueOf(surveyQuantity - 1), mathContext);
                covarianceMatrix[i][j] = ratio;
                covarianceMatrix[j][i] = ratio;
            }
        }
        return covarianceMatrix;
    }

    private void makePositiveDefiniteMatrix(BigDecimal[][] covarianceMatrix, BigDecimal epsilon) {
        for(int i = 0; i < covarianceMatrix.length; i++){
            covarianceMatrix[i][i] = covarianceMatrix[i][i].add(epsilon);
        }
    }

    private double[][] choleskyDecompose(BigDecimal[][] matrix){
        int n = matrix.length;
        double[][] resultMatrix = new double[n][n];

        for(int i = 0; i < n; i++){
            for(int j = 0; j <= i; j++){
                double sum = 0.0;
                for(int k = 0; k < j; k++){
                    sum += resultMatrix[i][k] * resultMatrix[j][k];
                }
                if(i == j){
                    resultMatrix[i][j] = Math.sqrt(matrix[i][i].doubleValue() - sum);
                }else{
                    resultMatrix[i][j] = (matrix[i][j].doubleValue() - sum) / resultMatrix[j][j];
                }
            }
        }
        return resultMatrix;
    }

    private BigDecimal simulatePath(Random random, int assetQuantity, double timeHorizon, BigDecimal[][] prices, BigDecimal[] weights, double[][] decomposedMatrix, BigDecimal[] annualDrifts, BigDecimal[] annualVolatilities){
        BigDecimal portfolioValue = BigDecimal.ZERO;
        double[] independentStandardNormalVariables = createIndependentStandardNormalVariables(random, assetQuantity);
        double[] correlatedMatrix = multiplyMatrixByVector(decomposedMatrix, independentStandardNormalVariables);
        for(int i = 0; i < assetQuantity; i++){
            double drift = (annualDrifts[i].doubleValue() - 0.5 * annualVolatilities[i].doubleValue() * annualVolatilities[i].doubleValue()) * timeHorizon;
            double diffusion = annualVolatilities[i].doubleValue() * Math.sqrt(timeHorizon) * correlatedMatrix[i];
            double simulatedFuturePrice = prices[i][prices[i].length-1].doubleValue() * Math.exp(drift + diffusion);
            portfolioValue = portfolioValue.add(BigDecimal.valueOf(simulatedFuturePrice).multiply(weights[i], mathContext));
        }
        return portfolioValue;
    }

    private double[] createIndependentStandardNormalVariables(Random random, int assetQuantity){
        double[] result = new double[assetQuantity];
        for(int i = 0; i < assetQuantity; i++){
            result[i] = random.nextGaussian();
        }
        return result;
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

    private BigDecimal[] computeLosses(BigDecimal[] portfolioValues, BigDecimal portfolioInitialValue, int simulationQuantity){
        BigDecimal[] result = new BigDecimal[simulationQuantity];
        for(int i = 0; i < simulationQuantity; i++){
            result[i] = portfolioInitialValue.subtract(portfolioValues[i]);
        }
        return result;
    }

    private BigDecimal computeValueAtRisk(BigDecimal[] losses, double confidenceLevel, int simulationQuantity){
        int valueAtRiskIndex = (int) Math.floor((1-confidenceLevel) * simulationQuantity);
        return losses[valueAtRiskIndex].setScale(2, RoundingMode.HALF_UP);
    }
}
