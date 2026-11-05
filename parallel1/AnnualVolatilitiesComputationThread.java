package parallel1;

import java.math.BigDecimal;
import java.util.concurrent.CountDownLatch;

public class AnnualVolatilitiesComputationThread implements Runnable {
    private final BigDecimal[] annualVolatilities;
    private final BigDecimal[] standardDeviations;
    private final double merchantDaysPerYear;
    private final int startIndex, endIndex;
    private final CountDownLatch latch;

    public AnnualVolatilitiesComputationThread(BigDecimal[] annualVolatilities, BigDecimal[] standardDeviations, double merchantDaysPerYear, int startIndex, int endIndex, CountDownLatch latch) {
        this.annualVolatilities = annualVolatilities;
        this.standardDeviations = standardDeviations;
        this.merchantDaysPerYear = merchantDaysPerYear;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.latch = latch;
    }

    @Override
    public void run() {
        for(int i = startIndex; i <= endIndex; i++){
            annualVolatilities[i] = standardDeviations[i].multiply(BigDecimal.valueOf(Math.sqrt(merchantDaysPerYear)));
        }
        latch.countDown();
    }
}
