package parallel1;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.concurrent.CountDownLatch;

public class EmpiricMeansComputationThread implements Runnable{
    private final BigDecimal[][] logReturns;
    private final BigDecimal[] empiricMeans;
    private final int startIndex, endIndex;
    private final MathContext mathContext;
    private final CountDownLatch latch;

    public EmpiricMeansComputationThread(BigDecimal[][] logReturns, BigDecimal[] empiricMeans, int startIndex, int endIndex, MathContext mathContext, CountDownLatch latch) {
        this.logReturns = logReturns;
        this.empiricMeans = empiricMeans;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.mathContext = mathContext;
        this.latch = latch;
    }

    @Override
    public void run() {
        for(int i = startIndex; i <= endIndex; i++){
            empiricMeans[i] = mean(logReturns[i]);
        }
        latch.countDown();
    }

    private BigDecimal mean(BigDecimal[] array){
        BigDecimal sum = BigDecimal.ZERO;
        for(BigDecimal element : array){
            sum = sum.add(element);
        }
        return sum.divide(BigDecimal.valueOf(array.length), mathContext);
    }
}
