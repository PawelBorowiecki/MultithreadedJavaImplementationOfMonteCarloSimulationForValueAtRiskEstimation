package parallel1;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.*;

public class MonteCarloSimulationMultithreaded {
    private final MathContext mathContext = new MathContext(20, RoundingMode.HALF_UP);

    public BigDecimal runSimulation(BigDecimal[][] prices, BigDecimal[] weights, double confidenceLevel, double days, int simulationQuantity, int threadsQuantity){
        SplittableRandom random = new SplittableRandom();
        double merchantDaysPerYear = 252.0;
        BigDecimal epsilon = BigDecimal.valueOf(1e-6);
        BigDecimal[] portfolioValues = new BigDecimal[simulationQuantity];
        BigDecimal[] losses = new BigDecimal[simulationQuantity];
        double timeHorizon = days / merchantDaysPerYear;
        BigDecimal portfolioInitialValue = computePortfolioInitialValue(prices, weights, threadsQuantity);
        BigDecimal[][] logReturns = computeLogReturns(prices, threadsQuantity);
        BigDecimal[][] covarianceMatrix = new BigDecimal[logReturns.length][logReturns.length];
        BigDecimal[] empiricMeans = computeEmpiricMeans(logReturns, threadsQuantity);
        BigDecimal[] standardDeviations = computeStandardDeviations(logReturns, empiricMeans, threadsQuantity);
        BigDecimal[] annualVolatilities = computeAnnualVolatilities(logReturns, standardDeviations, merchantDaysPerYear, threadsQuantity);
        BigDecimal[] annualDrifts = computeAnnualDrifts(logReturns, empiricMeans, merchantDaysPerYear, threadsQuantity);
        createCovarianceMatrix(covarianceMatrix, logReturns, empiricMeans, threadsQuantity);
        makePositiveDefiniteMatrix(covarianceMatrix, epsilon);
        double[][] decomposedMatrix = choleskyDecompose(covarianceMatrix);
        simulatePaths(prices, decomposedMatrix, portfolioValues, weights, annualDrifts, annualVolatilities, losses, portfolioInitialValue, timeHorizon, simulationQuantity, threadsQuantity, random);
        Arrays.parallelSort(losses);
        return computeValueAtRisk(losses, confidenceLevel, simulationQuantity);
    }

    private BigDecimal computePortfolioInitialValue(BigDecimal[][] prices, BigDecimal[] weights,  int threadsQuantityForPrerequisition){
        BigDecimal result = BigDecimal.ZERO;
        try(ExecutorService executorServiceForPortfolioInitialValue = Executors.newFixedThreadPool(threadsQuantityForPrerequisition)) {
            List<Future<BigDecimal>> portfolioValuesComponentsFuture = new ArrayList<>();
            int chunk = prices.length / threadsQuantityForPrerequisition;
            for(int i = 0; i < threadsQuantityForPrerequisition; i++){
                int startIndex = i * chunk;
                int endIndex = startIndex + chunk - 1;
                if(i == (threadsQuantityForPrerequisition - 1)){
                    endIndex = prices.length - 1;
                }
                Callable<BigDecimal> task = new PortfolioInitialValueComputationThread(prices, weights, startIndex, endIndex);
                Future<BigDecimal> portfolioValuesComponentFuture = executorServiceForPortfolioInitialValue.submit(task);
                portfolioValuesComponentsFuture.add(portfolioValuesComponentFuture);
            }
            for(Future<BigDecimal> portfolioValuesComponentFuture : portfolioValuesComponentsFuture){
                result = result.add(portfolioValuesComponentFuture.get());
            }
        }catch(ExecutionException | InterruptedException e) {
            throw new RuntimeException(e);
        }
        return result;
    }

    private BigDecimal[][] computeLogReturns(BigDecimal[][] prices, int threadsQuantity){
        CountDownLatch countDownLatchForLogReturns = new CountDownLatch(threadsQuantity);
        BigDecimal[][] logReturns = new BigDecimal[prices.length][prices[0].length-1];
        int chunk = prices.length / threadsQuantity;
        for(int i = 0; i < threadsQuantity; i++){
            int startIndex = i * chunk;
            int endIndex = startIndex + chunk - 1;
            if(i == (threadsQuantity - 1)){
                endIndex = prices.length - 1;
            }
            Thread logReturnsComputationThread = new Thread(new LogReturnsComputationThread(prices, logReturns, startIndex, endIndex, mathContext, countDownLatchForLogReturns));
            logReturnsComputationThread.start();
        }
        try{
            countDownLatchForLogReturns.await();
        }catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
        return logReturns;
    }

    private BigDecimal[] computeEmpiricMeans(BigDecimal[][] logReturns, int threadsQuantity){
        CountDownLatch countDownLatchForEmpiricMeans = new CountDownLatch(threadsQuantity);
        int empiricMeansSize = logReturns.length;
        BigDecimal[] empiricMeans = new BigDecimal[empiricMeansSize];
        int chunk = empiricMeansSize / threadsQuantity;
        for(int i = 0; i < threadsQuantity; i++){
            int startIndex = i * chunk;
            int endIndex = startIndex + chunk - 1;
            if(i == (threadsQuantity - 1)){
                endIndex = empiricMeansSize - 1;
            }
            Thread empiricMeansComputationThread = new Thread(new EmpiricMeansComputationThread(logReturns, empiricMeans, startIndex, endIndex, mathContext, countDownLatchForEmpiricMeans));
            empiricMeansComputationThread.start();
        }
        try{
            countDownLatchForEmpiricMeans.await();
        }catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
        return empiricMeans;
    }

    private BigDecimal[] computeStandardDeviations(BigDecimal[][] logReturns, BigDecimal[] empiricMeans, int threadsQuantity){
        CountDownLatch countDownLatchForStandardDeviations = new CountDownLatch(threadsQuantity);
        int standardDeviationsSize = logReturns.length;
        BigDecimal[] standardDeviations = new BigDecimal[standardDeviationsSize];
        int chunk = standardDeviationsSize / threadsQuantity;
        for(int i = 0; i < threadsQuantity; i++){
            int startIndex = i * chunk;
            int endIndex = startIndex + chunk - 1;
            if(i == (threadsQuantity - 1)){
                endIndex = standardDeviationsSize - 1;
            }
            Thread standardDeviationsComputationThread = new Thread(new StandardDeviationsComputationThread(logReturns, standardDeviations, empiricMeans, startIndex, endIndex, countDownLatchForStandardDeviations, mathContext));
            standardDeviationsComputationThread.start();
        }
        try{
            countDownLatchForStandardDeviations.await();
        }catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
        return standardDeviations;
    }

    private BigDecimal[] computeAnnualVolatilities(BigDecimal[][] logReturns, BigDecimal[] standardDeviations, double merchantDaysPerYear, int threadsQuantityForPrerequisition){
        CountDownLatch countDownLatchForAnnualVolatilities = new CountDownLatch(threadsQuantityForPrerequisition);
        int annualVolatilitiesSize = logReturns.length;
        BigDecimal[] annualVolatilities = new BigDecimal[annualVolatilitiesSize];
        int chunk = annualVolatilitiesSize / threadsQuantityForPrerequisition;
        for(int i = 0; i < threadsQuantityForPrerequisition; i++){
            int startIndex = i * chunk;
            int endIndex = startIndex + chunk - 1;
            if(i == (threadsQuantityForPrerequisition - 1)){
                endIndex = annualVolatilitiesSize - 1;
            }
            Thread annualVolatilitiesComputationThread = new Thread(new AnnualVolatilitiesComputationThread(annualVolatilities, standardDeviations, merchantDaysPerYear, startIndex, endIndex, countDownLatchForAnnualVolatilities));
            annualVolatilitiesComputationThread.start();
        }
        try{
            countDownLatchForAnnualVolatilities.await();
        }catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
        return annualVolatilities;
    }

    private BigDecimal[] computeAnnualDrifts(BigDecimal[][] logReturns, BigDecimal[] empiricMeans, double merchantDaysPerYear, int threadsQuantity){
        CountDownLatch countDownLatchForAnnualDrifts = new CountDownLatch(threadsQuantity);
        int annualDriftsSize = logReturns.length;
        BigDecimal[] annualDrifts = new BigDecimal[annualDriftsSize];
        int chunk = annualDriftsSize / threadsQuantity;
        for(int i = 0; i < threadsQuantity; i++){
            int startIndex = i * chunk;
            int endIndex = startIndex + chunk - 1;
            if(i == (threadsQuantity - 1)){
                endIndex = annualDriftsSize - 1;
            }
            Thread annualDriftsComputationThread = new Thread(new AnnualDriftsComputationThread(annualDrifts, empiricMeans, merchantDaysPerYear, startIndex, endIndex, countDownLatchForAnnualDrifts, mathContext));
            annualDriftsComputationThread.start();
        }
        try{
            countDownLatchForAnnualDrifts.await();
        }catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
        return annualDrifts;
    }

    private void createCovarianceMatrix(BigDecimal[][] covarianceMatrix, BigDecimal[][] logReturns, BigDecimal[] empiricMeans, int threadsQuantityForPrerequisition){
        CountDownLatch countDownLatchForCovarianceMatrix = new CountDownLatch(threadsQuantityForPrerequisition);
        int chunk = logReturns.length / threadsQuantityForPrerequisition;
        for(int i = 0; i < threadsQuantityForPrerequisition; i++){
            int startIndex = i * chunk;
            int endIndex = startIndex + chunk - 1;
            if(i == (threadsQuantityForPrerequisition - 1)){
                endIndex = logReturns.length - 1;
            }
            Thread covarianceMatrixCreationThread = new Thread(new CovarianceMatrixThread(covarianceMatrix, logReturns, empiricMeans, startIndex, endIndex, countDownLatchForCovarianceMatrix, mathContext));
            covarianceMatrixCreationThread.start();
        }
        try{
            countDownLatchForCovarianceMatrix.await();
        }catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
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

    private void simulatePaths(BigDecimal[][] prices, double[][] decomposedMatrix, BigDecimal[] portfolioValues, BigDecimal[] weights, BigDecimal[] annualDrifts, BigDecimal[] annualVolatilities, BigDecimal[] losses, BigDecimal portfolioInitialValue, double timeHorizon, int simulationQuantity, int threadsQuantity, SplittableRandom random){
        int threadsForOperation = Math.min(simulationQuantity, threadsQuantity);
        CountDownLatch countDownLatchForPathSimulation = new CountDownLatch(threadsForOperation);
        int chunk = simulationQuantity / threadsForOperation;
        for(int i = 0; i < threadsForOperation; i++){
            int startIndex = i * chunk;
            int endIndex = startIndex + chunk - 1;
            if(i == (threadsForOperation - 1)){
                endIndex = simulationQuantity - 1;
            }
            Thread pathSimulationThread = new Thread(new PathSimulationThread(prices, portfolioValues, weights, annualDrifts, annualVolatilities, losses, portfolioInitialValue, decomposedMatrix, timeHorizon, prices.length, startIndex, endIndex, random.split(), countDownLatchForPathSimulation, mathContext));
            pathSimulationThread.start();
        }
        try{
            countDownLatchForPathSimulation.await();
        }catch(InterruptedException e) {
            throw new RuntimeException(e);
        }
    }

    private BigDecimal computeValueAtRisk(BigDecimal[] losses, double confidenceLevel, int simulationQuantity){
        int valueAtRiskIndex = (int) Math.floor((1-confidenceLevel) * simulationQuantity);
        return losses[valueAtRiskIndex].setScale(2, RoundingMode.HALF_UP);
    }
}
