package parallel2;

import parallel1.*;

import java.math.BigDecimal;
import java.math.MathContext;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.SplittableRandom;
import java.util.concurrent.*;
import java.util.stream.IntStream;

public class MonteCarloSimulationMultithreaded2 {
    private final MathContext mathContext = new MathContext(20, RoundingMode.HALF_UP);

    public BigDecimal runSimulation(BigDecimal[][] prices, BigDecimal[] weights, double confidenceLevel, double days, int simulationQuantity, int threadsQuantity){
        SplittableRandom random = new SplittableRandom();
        int threadsQuantityForPrerequisition, threadsQuantityForCovarianceMatrixCreation;
        boolean isSequential = false;
        boolean isCovarianceMatrixParallel = false;

        if(threadsQuantity == 1){
            threadsQuantityForPrerequisition = 0;
            threadsQuantityForCovarianceMatrixCreation = 0;
            isSequential = true;
        }else if(threadsQuantity == 2){
            threadsQuantityForPrerequisition = 0;
            isSequential = true;
            isCovarianceMatrixParallel = true;
            threadsQuantityForCovarianceMatrixCreation = 0;
        }else{
            threadsQuantityForPrerequisition = threadsQuantity / 3;
            threadsQuantityForCovarianceMatrixCreation = threadsQuantityForPrerequisition + (threadsQuantity % 3);
        }

        BigDecimal[] portfolioValues = new BigDecimal[simulationQuantity];
        double merchantDaysPerYear = 252.0;
        BigDecimal epsilon = BigDecimal.valueOf(1e-6);
        BigDecimal[] losses = new BigDecimal[simulationQuantity];
        double timeHorizon = days / merchantDaysPerYear;
        BigDecimal[][] logReturns = computeLogReturns(prices, threadsQuantity);
        BigDecimal[] empiricMeans = computeEmpiricMeans(logReturns, threadsQuantity);
        BigDecimal[][] covarianceMatrix = new BigDecimal[logReturns.length][logReturns.length];
        CompletableFuture<double[][]> decomposedMatrixFuture;
        CompletableFuture<BigDecimal> portfolioInitialValueFuture;
        CompletableFuture<BigDecimal[]> annualVolatilitiesFuture;

        try(ForkJoinPool threadPool = new ForkJoinPool(threadsQuantity)){
            if(isSequential && !isCovarianceMatrixParallel){
                decomposedMatrixFuture = CompletableFuture
                        .supplyAsync(() -> createCovarianceMatrixSeq(logReturns, empiricMeans), threadPool)
                        .thenApply(covMatrix -> makePositiveDefiniteMatrix(covMatrix, epsilon))
                        .thenApply(this::choleskyDecompose);
                annualVolatilitiesFuture = CompletableFuture
                        .supplyAsync(() -> computeStandardDeviationsSeq(logReturns, empiricMeans), threadPool)
                        .thenApply(standardDeviations -> computeAnnualVolatilitiesSeq(logReturns, standardDeviations, merchantDaysPerYear));
                portfolioInitialValueFuture = CompletableFuture.supplyAsync(() -> computePortfolioInitialValueSeq(prices, weights), threadPool);
            }else if(isSequential && isCovarianceMatrixParallel){
                decomposedMatrixFuture = CompletableFuture
                        .supplyAsync(() -> createCovarianceMatrixSeq(logReturns, empiricMeans), threadPool)
                        .thenApply((covMatrix) -> makePositiveDefiniteMatrix(covMatrix, epsilon))
                        .thenApply(this::choleskyDecompose);
                annualVolatilitiesFuture = CompletableFuture
                        .supplyAsync(() -> computeStandardDeviationsSeq(logReturns, empiricMeans), threadPool)
                        .thenApply(standardDeviations -> computeAnnualVolatilitiesSeq(logReturns, standardDeviations, merchantDaysPerYear));
                portfolioInitialValueFuture = CompletableFuture.supplyAsync(() -> computePortfolioInitialValueSeq(prices, weights), threadPool);
            }else{
                decomposedMatrixFuture = CompletableFuture
                        .supplyAsync(() -> createCovarianceMatrixParallel(covarianceMatrix, logReturns, empiricMeans, threadsQuantityForCovarianceMatrixCreation))
                        .thenApply((covMatrix) -> makePositiveDefiniteMatrix(covMatrix, epsilon))
                        .thenApply(this::choleskyDecompose);
                annualVolatilitiesFuture = CompletableFuture
                        .supplyAsync(() -> computeStandardDeviationsParallel(logReturns, empiricMeans, threadsQuantityForPrerequisition))
                        .thenApply(standardDeviations -> computeAnnualVolatilitiesParallel(logReturns, standardDeviations, merchantDaysPerYear, threadsQuantityForPrerequisition));
                portfolioInitialValueFuture = CompletableFuture.supplyAsync(() -> computePortfolioInitialValueParallel(prices, weights, threadsQuantityForPrerequisition));
            }

            BigDecimal[] annualVolatilities = annualVolatilitiesFuture.join();
            double[][] decomposedMatrix = decomposedMatrixFuture.join();
            BigDecimal portfolioInitialValue = portfolioInitialValueFuture.join();
            BigDecimal[] annualDrifts = computeAnnualDrifts(logReturns, empiricMeans, merchantDaysPerYear, threadsQuantity);

            threadPool.submit(() ->
                    IntStream.range(0, simulationQuantity).parallel().forEach(i -> {
                        portfolioValues[i] = simulatePath(random.split(), prices.length, timeHorizon, prices, weights, decomposedMatrix, annualDrifts, annualVolatilities);
                        losses[i] = portfolioInitialValue.subtract(portfolioValues[i]);
                    })
            );
        }catch(Exception e){
            e.printStackTrace();
        }

        Arrays.parallelSort(losses);
        return computeValueAtRisk(losses, confidenceLevel, simulationQuantity);
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

    private BigDecimal[][] createCovarianceMatrixSeq(BigDecimal[][] logReturns, BigDecimal[] empiricMeans){
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

    private BigDecimal[][] createCovarianceMatrixParallel(BigDecimal[][] covarianceMatrix, BigDecimal[][] logReturns, BigDecimal[] empiricMeans, int threadsQuantityForPrerequisition){
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
        return covarianceMatrix;
    }

    private BigDecimal[][] makePositiveDefiniteMatrix(BigDecimal[][] covarianceMatrix, BigDecimal epsilon) {
        for(int i = 0; i < covarianceMatrix.length; i++){
            covarianceMatrix[i][i] = covarianceMatrix[i][i].add(epsilon);
        }
        return covarianceMatrix;
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

    private BigDecimal[] computeStandardDeviationsSeq(BigDecimal[][] logReturns, BigDecimal[] empiricMeans){
        int size = logReturns.length;
        BigDecimal[] result = new BigDecimal[size];
        for(int i = 0; i < size; i++){
            result[i] = standardDeviation(logReturns[i], empiricMeans[i]);
        }
        return result;
    }

    private BigDecimal[] computeStandardDeviationsParallel(BigDecimal[][] logReturns, BigDecimal[] empiricMeans, int threadsQuantity){
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

    private BigDecimal standardDeviation(BigDecimal[] array, BigDecimal mean){
        BigDecimal sum = BigDecimal.ZERO;
        for(BigDecimal element : array){
            sum = sum.add(element.subtract(mean).multiply(element.subtract(mean), mathContext));
        }
        return BigDecimal.valueOf(Math.sqrt(sum.divide(BigDecimal.valueOf(array.length-1), mathContext).doubleValue()));
    }

    private BigDecimal[] computeAnnualVolatilitiesSeq(BigDecimal[][] logReturns, BigDecimal[] standardDeviations, double merchantDaysPerYear){
        int size = logReturns.length;
        BigDecimal[] result = new BigDecimal[size];
        for(int i = 0; i < size; i++){
            result[i] = standardDeviations[i].multiply(BigDecimal.valueOf(Math.sqrt(merchantDaysPerYear)));
        }
        return result;
    }

    private BigDecimal[] computeAnnualVolatilitiesParallel(BigDecimal[][] logReturns, BigDecimal[] standardDeviations, double merchantDaysPerYear, int threadsQuantityForPrerequisition){
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

    private BigDecimal computePortfolioInitialValueSeq(BigDecimal[][] prices, BigDecimal[] weights){
        BigDecimal result = BigDecimal.ZERO;
        for(int i = 0; i < prices.length; i++){
            result = result.add(prices[i][prices[i].length-1].multiply(weights[i]));
        }
        return result;
    }

    private BigDecimal computePortfolioInitialValueParallel(BigDecimal[][] prices, BigDecimal[] weights,  int threadsQuantityForPrerequisition){
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

    private BigDecimal simulatePath(SplittableRandom random, int assetQuantity, double timeHorizon, BigDecimal[][] prices, BigDecimal[] weights, double[][] decomposedMatrix, BigDecimal[] annualDrifts, BigDecimal[] annualVolatilities){
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

    private BigDecimal computeValueAtRisk(BigDecimal[] losses, double confidenceLevel, int simulationQuantity){
        int valueAtRiskIndex = (int) Math.floor((1-confidenceLevel) * simulationQuantity);
        return losses[valueAtRiskIndex].setScale(2, RoundingMode.HALF_UP);
    }
}
