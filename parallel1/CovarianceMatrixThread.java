package parallel1;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.concurrent.CountDownLatch;

public class CovarianceMatrixThread implements Runnable{
    private final BigDecimal[][] logReturns, covarianceMatrix;
    private final BigDecimal[] empiricMeans;
    private final int startIndex, endIndex;
    private final CountDownLatch latch;
    private final MathContext mathContext;

    public CovarianceMatrixThread(BigDecimal[][] covarianceMatrix, BigDecimal[][] logReturns, BigDecimal[] empiricMeans, int startIndex, int endIndex, CountDownLatch latch, MathContext mathContext){
        this.logReturns = logReturns;
        this.empiricMeans = empiricMeans;
        this.covarianceMatrix = covarianceMatrix;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.latch = latch;
        this.mathContext = mathContext;
    }

    @Override
    public void run() {
        int surveyQuantity = logReturns[0].length;
        for(int i = startIndex; i <= endIndex; i++){
            for(int j = i; j < logReturns.length; j++){
                BigDecimal sum = BigDecimal.ZERO;
                for(int t = 0; t < surveyQuantity; t++){
                    sum = sum.add((logReturns[i][t].subtract(empiricMeans[i])).multiply(logReturns[j][t].subtract(empiricMeans[j])));
                }
                BigDecimal ratio = sum.divide(BigDecimal.valueOf(surveyQuantity - 1), mathContext);
                covarianceMatrix[i][j] = ratio;
                covarianceMatrix[j][i] = ratio;
            }
        }
        latch.countDown();
    }
}
