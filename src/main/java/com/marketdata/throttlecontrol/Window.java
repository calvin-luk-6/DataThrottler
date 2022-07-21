package com.marketdata.throttlecontrol;


import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.HashMap;
import java.util.Map;

public class Window {
    private static final Logger logger = LoggerFactory.getLogger(Window.class);
    private Map<String, Object> latestDataMap = new HashMap<>();
    private Map<String, Object> publishedInWindowDataMap = new HashMap<>();
    private int maxData = -1;
    private long windowLengthInMillis = -1;
    private long windowEndTime = -1L;
    private int windowCount = 0;

    public Window(int maxData, long windowLengthInMillis) throws Exception {
        if (maxData <= 0 || windowLengthInMillis <= 0)
            throw new Exception("invalid initialisation parameters");

        this.maxData = maxData;
        this.windowLengthInMillis = windowLengthInMillis;
    }

    public void renewWindowEndTime(long currentTime) {
        if (currentTime <= 0) {
            logger.warn("update time <= 0, ignore renew window");
        }

        windowEndTime = currentTime + windowLengthInMillis;
    }

    public long getWindowEndTime() {
        return windowEndTime;
    }

    public void incrementWindowCount() throws Exception {
        if (windowCount == maxData) {
            throw new Exception("max data reached - fail to increment counter");
        } else {
            windowCount++;
        }
    }

    public void resetWindowCount() {
        windowCount = 0;
    }

    public int getWindowCount() {
        return windowCount;
    }

    public int getWindowMaxData() {
        return maxData;
    }

    public long getWindowLengthInMillis() {
        return windowLengthInMillis;
    }

    public void putLatestDataMap(String key, Object data) {
        latestDataMap.put(key, data);
    }

    public Map<String, Object> getLatestDataMap() {
        return latestDataMap;
    }

    public Map<String, Object> getPublishedInWindowDataMap() {
        return publishedInWindowDataMap;
    }

    public void putPublishedInWindowDataMap(String key, Object data) {
        publishedInWindowDataMap.put(key, data);
    }

    public void clearPublishedInWindowDataMap() {
        publishedInWindowDataMap.clear();
    }

    public boolean isPublishedData(String key) {
        return publishedInWindowDataMap.containsKey(key);
    }

}
