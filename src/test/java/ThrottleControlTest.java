import com.marketdata.model.MarketData;
import com.marketdata.throttlecontrol.MarketDataProcessor;
import org.junit.jupiter.api.Assertions;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.math.BigDecimal;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.List;
import java.util.concurrent.TimeUnit;


public class ThrottleControlTest {
    private final int MAX_FEED_PER_WINDOW = 100;
    private final long WINDOW_LENGTH_IN_MILLIS = 1000;
    private final String[] symbols = {"Symbol_A", "Symbol_B", "Symbol_C"};
    private MarketDataProcessor processor;

    @BeforeEach
    void setUp() throws Exception {
        processor = new MarketDataProcessor(MAX_FEED_PER_WINDOW, WINDOW_LENGTH_IN_MILLIS);
    }

    @Test
    void _1_testWindowInitiated() {
        Assertions.assertNotNull(processor.getWindow());
        Assertions.assertEquals(MAX_FEED_PER_WINDOW, processor.getWindow().getWindowMaxData());
        Assertions.assertEquals(WINDOW_LENGTH_IN_MILLIS, processor.getWindow().getWindowLengthInMillis());
    }

    @Test
    void _2_testSimplePublishSymbolsWithinSameWindow() throws Exception {
        MarketData symbolA = createMarketData(symbols[0]);
        MarketData symbolB = createMarketData(symbols[1]);
        MarketData symbolC = createMarketData(symbols[2]);

        processor.onMessage(symbolA);
        processor.onMessage(symbolB);
        processor.onMessage(symbolC);

        Assertions.assertEquals(3, processor.getWindow().getLatestDataMap().size());
    }

    @Test
    void _3_testUpdateLatestDataMap() throws Exception {
        MarketData symbolA = createMarketData(symbols[0], BigDecimal.valueOf(1.11));
        MarketData symbolA2 = createMarketData(symbols[0], BigDecimal.valueOf(2.22));
        MarketData symbolB = createMarketData(symbols[1]);
        MarketData symbolC = createMarketData(symbols[2]);

        processor.onMessage(symbolA);
        processor.onMessage(symbolB);
        processor.onMessage(symbolC);
        processor.onMessage(symbolA2);

        Assertions.assertEquals(symbolA2.getPrice(), ((MarketData) processor.getWindow().getLatestDataMap().get(symbolA.getSymbol())).getPrice());
    }

    @Test
    void _4_testAtMostUpdateOncePerWindow() throws Exception {
        MarketData symbolA = createMarketData(symbols[0], BigDecimal.valueOf(1.11));
        MarketData symbolA2 = createMarketData(symbols[0], BigDecimal.valueOf(2.22));
        MarketData symbolB = createMarketData(symbols[1]);
        MarketData symbolC = createMarketData(symbols[2]);

        processor.onMessage(symbolA);
        processor.onMessage(symbolB);
        processor.onMessage(symbolC);
        processor.onMessage(symbolA2);

        Assertions.assertEquals(symbolA.getPrice(), ((MarketData) processor.getWindow().getPublishedInWindowDataMap().get(symbolA.getSymbol())).getPrice());
    }

    @Test
    void _5_testPublishDataOutOfWindow() throws Exception {
        MarketData symbolA = createMarketData(symbols[0], BigDecimal.valueOf(1.11));
        MarketData symbolA2 = createMarketData(symbols[0], BigDecimal.valueOf(2.22));
        MarketData symbolB = createMarketData(symbols[1]);
        MarketData symbolC = createMarketData(symbols[2]);

        processor.onMessage(symbolA);
        processor.onMessage(symbolB);
        processor.onMessage(symbolC);

        TimeUnit.MILLISECONDS.sleep(WINDOW_LENGTH_IN_MILLIS * 2);
        processor.onMessage(symbolA2);

        Assertions.assertEquals(symbolA2.getPrice(), ((MarketData) processor.getWindow().getPublishedInWindowDataMap().get(symbolA2.getSymbol())).getPrice());
        Assertions.assertEquals(processor.getWindow().getWindowEndTime(), Calendar.getInstance().getTimeInMillis() + processor.getWindow().getWindowLengthInMillis());
    }

    @Test
    void _6_testNotPublishIfDataExceedMaxWithinWindow() throws Exception {
        List<MarketData> marketDataList = batchCreateMarketData(MAX_FEED_PER_WINDOW);

        MarketData symbol1 = createMarketData("symbol_1");

        for (MarketData marketData : marketDataList) {
            processor.onMessage(marketData);
        }
        processor.onMessage(symbol1);

        Assertions.assertNotEquals(symbol1.getPrice(), ((MarketData) processor.getWindow().getPublishedInWindowDataMap().get(symbol1.getSymbol())).getPrice());
    }

    private List<MarketData> batchCreateMarketData(int noOfData) {
        List<MarketData> marketDataList = new ArrayList<>();
        for (int i = 0; i < noOfData; i++) {
            marketDataList.add(createMarketData("symbol_" + i, BigDecimal.valueOf((10.01 + i))));
        }
        return marketDataList;
    }

    private MarketData createMarketData(String symbol) {
        return createMarketData(symbol, BigDecimal.valueOf(1.123));
    }

    private MarketData createMarketData(String symbol, BigDecimal price) {
        return new MarketData(symbol, price, Calendar.getInstance().getTimeInMillis());
    }
}
