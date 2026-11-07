package parallel1;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.concurrent.CountDownLatch;

public class StandardDeviationsComputationThread implements Runnable{
    private final BigDecimal[][] logReturns;
    private final BigDecimal[] standardDeviations, empiricMeans;
    private final int startIndex, endIndex;
    private final CountDownLatch latch;
    private final MathContext mathContext;

    public StandardDeviationsComputationThread(BigDecimal[][] logReturns, BigDecimal[] standardDeviations, BigDecimal[] empiricMeans, int startIndex, int endIndex, CountDownLatch latch, MathContext mathContext) {
        this.logReturns = logReturns;
        this.standardDeviations = standardDeviations;
        this.empiricMeans = empiricMeans;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.latch = latch;
        this.mathContext = mathContext;
    }

    @Override
    public void run() {
        for(int i = startIndex; i <= endIndex; i++){
            standardDeviations[i] = standardDeviation(logReturns[i], empiricMeans[i]);
        }
        latch.countDown();
    }

    private BigDecimal standardDeviation(BigDecimal[] array, BigDecimal mean){
        BigDecimal sum = BigDecimal.ZERO;
        for(BigDecimal element : array){
            sum = sum.add(element.subtract(mean).multiply(element.subtract(mean), mathContext));
        }
        return BigDecimal.valueOf(Math.sqrt(sum.divide(BigDecimal.valueOf(array.length-1), mathContext).doubleValue()));
    }
}
