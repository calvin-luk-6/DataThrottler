package com.marketdata.throttlecontrol;

import com.marketdata.model.MarketData;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.Calendar;


/*
 * Assumptions:
 * 1. Assume publishAggregatedMarketData is non-blocking and completes in very short amount of time.
 *
 * */

public class MarketDataProcessor {
    private static final Logger logger = LoggerFactory.getLogger(MarketDataProcessor.class);
    private Window window;

    public MarketDataProcessor(int maxData, long windowLengthInMillis) throws Exception {
        window = new Window(maxData, windowLengthInMillis);
    }

    public void onMessage(MarketData data) throws Exception {
        long currentTimestamp = Calendar.getInstance().getTimeInMillis();
        String symbol = data.getSymbol();

        // first market data
        if (window.getLatestDataMap().size() == 0) {

            publishData(data);

            window.putLatestDataMap(symbol, data);
            window.putPublishedInWindowDataMap(symbol, data);

            window.renewWindowEndTime(currentTimestamp);
            window.incrementWindowCount();
        } else {
            // subsequent market data
            window.putLatestDataMap(symbol, data);

            // market data out of the window
            if (currentTimestamp > window.getWindowEndTime()) {

                publishData(data);

                window.clearPublishedInWindowDataMap();
                window.putPublishedInWindowDataMap(symbol, data);

                window.renewWindowEndTime(currentTimestamp);

                window.resetWindowCount();
                window.incrementWindowCount();
            } else {
                // market data within window
                if (window.getWindowCount() < window.getWindowMaxData() && !window.isPublishedData(symbol)) {

                    publishData(data);

                    window.putPublishedInWindowDataMap(symbol, data);
                    window.incrementWindowCount();
                } else {
                    // market data ignored, do nothing
                }
            }
        }
    }

    public Window getWindow() {
        return window;
    }

    private void publishData(MarketData data) {
        logger.debug(data.toString());
        publishAggregatedMarketData(data);
    }

    // Publish aggregated and throttled market data
    public void publishAggregatedMarketData(MarketData data) {
        // Do Nothing, assume implemented.
    }
}
