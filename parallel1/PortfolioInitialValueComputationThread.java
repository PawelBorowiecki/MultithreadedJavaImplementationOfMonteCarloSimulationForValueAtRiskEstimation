package parallel1;

import java.math.BigDecimal;
import java.util.concurrent.Callable;

public class PortfolioInitialValueComputationThread implements Callable<BigDecimal> {
    private final BigDecimal[][] prices;
    private final BigDecimal[] weights;
    private final int startIndex, endIndex;

    public PortfolioInitialValueComputationThread(BigDecimal[][] prices, BigDecimal[] weights, int startIndex, int endIndex) {
        this.prices = prices;
        this.weights = weights;
        this.startIndex = startIndex;
        this.endIndex = endIndex;
    }

    @Override
    public BigDecimal call() {
        BigDecimal result = BigDecimal.ZERO;
        for(int i = startIndex; i <= endIndex; i++){
            result = result.add(prices[i][prices[i].length-1].multiply(weights[i]));
        }
        return result;
    }
}
