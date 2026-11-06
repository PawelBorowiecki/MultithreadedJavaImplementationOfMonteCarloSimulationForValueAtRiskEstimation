package parallel1;

import java.math.BigDecimal;
import java.math.MathContext;
import java.util.concurrent.CountDownLatch;

public class LogReturnsComputationThread implements Runnable{
    private final BigDecimal[][] prices, logReturns;
    private final int startIndex, endIndex;
    private final MathContext mathContext;
    private final CountDownLatch latch;

    public LogReturnsComputationThread(BigDecimal[][] prices, BigDecimal[][] logReturns, int startIndex, int endIndex, MathContext mathContext, CountDownLatch latch) {
        this.prices = prices;
        this.logReturns = logReturns;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
        this.mathContext = mathContext;
        this.latch = latch;
    }

    @Override
    public void run() {
        for(int i = startIndex; i <= endIndex; i++){
            for(int j = 1; j < prices[i].length; j++){
                logReturns[i][j-1] = BigDecimal.valueOf(Math.log(prices[i][j].divide(prices[i][j-1], mathContext).doubleValue()));
            }
        }
        latch.countDown();
    }
}
