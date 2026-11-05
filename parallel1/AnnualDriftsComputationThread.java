package parallel1;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.concurrent.CountDownLatch;

public class AnnualDriftsComputationThread implements Runnable{
    private final BigDecimal[] annualDrifts, empiricMeans;
    private final double merchantDaysPerYear;
    private final int startIndex, endIndex;
    private final CountDownLatch latch;
    private final MathContext mathContext;

    public AnnualDriftsComputationThread(BigDecimal[] annualDrifts, BigDecimal[] empiricMeans, double merchantDaysPerYear, int startIndex, int endIndex, CountDownLatch latch, MathContext mathContext) {
        this.annualDrifts = annualDrifts;
        this.empiricMeans = empiricMeans;
        this.merchantDaysPerYear = merchantDaysPerYear;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.latch = latch;
        this.mathContext = mathContext;
    }

    @Override
    public void run() {
        for(int i = startIndex; i <= endIndex; i++){
            annualDrifts[i] = empiricMeans[i].multiply(BigDecimal.valueOf(merchantDaysPerYear), mathContext);
        }
        latch.countDown();
    }
}
