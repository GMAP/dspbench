package org.dspbench.applications.bargainindex;

import org.apache.storm.tuple.Fields;
import org.apache.storm.tuple.Tuple;
import org.apache.storm.tuple.Values;
import java.util.HashMap;
import java.util.Map;

import org.dspbench.applications.bargainindex.BargainIndexConstants;
import org.dspbench.bolt.AbstractBolt;
import org.dspbench.util.config.Configuration;
import org.joda.time.DateTime;
import org.joda.time.DateTimeComparator;

/**
 * Calculates the VWAP (Volume Weighted Average Price) throughout the day for each
 * stock symbol. When the first quote from following day appears, the old calculation
 * is thrown away.
 * 
 * @author mayconbordin
 */
public class VwapBolt extends AbstractBolt {
    private static final DateTimeComparator dateOnlyComparator = DateTimeComparator.getDateOnlyInstance();
    
    private Map<String, Vwap> stocks;
    private String period;

    @Override
    public Fields getDefaultFields() {
        return new Fields(BargainIndexConstants.Field.STOCK, BargainIndexConstants.Field.VWAP, BargainIndexConstants.Field.START_DATE, BargainIndexConstants.Field.END_DATE);
    }

    @Override
    public void initialize() {
        period = config.getString(BargainIndexConstants.Conf.VWAP_PERIOD, BargainIndexConstants.Periodicity.DAILY);
        
        stocks = new HashMap<>();
        
    }

    @Override
    public void execute(Tuple input) {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            receiveThroughput();
        }

        String stock  = input.getStringByField(BargainIndexConstants.Field.STOCK);
        double price  = (double) input.getDoubleByField(BargainIndexConstants.Field.PRICE);
        int volume    = (int) input.getIntegerByField(BargainIndexConstants.Field.VOLUME);
        DateTime date = (DateTime) input.getValueByField(BargainIndexConstants.Field.DATE);
        int inteval   = input.getIntegerByField(BargainIndexConstants.Field.INTERVAL);

        Vwap vwap = stocks.get(stock);

        if (withinPeriod(vwap, date.withTimeAtStartOfDay())) {
            vwap.update(volume, price, date.withTimeAtStartOfDay().plusSeconds(inteval));
            if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
                emittedThroughput();
            }
            collector.emit(input, new Values(stock, vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
        } else {
            if (vwap != null) {
                collector.emit(new Values(stock, vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
            }
            
            vwap = new Vwap(volume, price, date.withTimeAtStartOfDay(), date.withTimeAtStartOfDay().plusSeconds(inteval));
            stocks.put(stock, vwap);
            if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
                emittedThroughput();
            }
            collector.emit(input, new Values(stock, vwap.getVwap(), vwap.getStartDate(), vwap.getEndDate()));
        }
        
        collector.ack(input);
    }

    @Override
    public void cleanup() {
        if (!config.getBoolean(Configuration.METRICS_ONLY_SINK, false)) {
            SaveMetrics();
        }
    }
    
    private boolean withinPeriod(Vwap vwap, DateTime quoteDate) {
        if (vwap == null) return false;
        
        DateTime vwapDate  = vwap.getStartDate();
        
        switch (period) {
            case BargainIndexConstants.Periodicity.MINUTELY:
                return ((dateOnlyComparator.compare(vwapDate, quoteDate) == 0)
                        && vwapDate.getMinuteOfDay() == quoteDate.getMinuteOfDay());
            
            case BargainIndexConstants.Periodicity.HOURLY:
                return ((dateOnlyComparator.compare(vwapDate, quoteDate) == 0)
                        && vwapDate.getHourOfDay() == quoteDate.getHourOfDay());
                
            case BargainIndexConstants.Periodicity.DAILY:
                return (dateOnlyComparator.compare(vwapDate, quoteDate) == 0);
                
            case BargainIndexConstants.Periodicity.WEEKLY:
                return (vwapDate.getYear() == quoteDate.getYear() &&
                        vwapDate.getWeekOfWeekyear() == quoteDate.getWeekOfWeekyear());
                
            case BargainIndexConstants.Periodicity.MONTHLY:
                return (vwapDate.getYear() == quoteDate.getYear() &&
                        vwapDate.getMonthOfYear() == quoteDate.getMonthOfYear());
        }
        
        return false;
    }
    
    public static final class Vwap {
        private long totalShares = 0;
        private double tradedValue = 0;
        private double vwap = 0;
        private DateTime startDate;
        private DateTime endDate;

        public Vwap(long shares, double price, DateTime start, DateTime end) {
            this.startDate = start;
            this.endDate = end;
            
            update(shares, price, null);
        }
        
        public void update(long shares, double price, DateTime date) {
            totalShares += shares;
            tradedValue += (shares*price);
            vwap = tradedValue/totalShares;
            
            if (date != null) {
                endDate = date;
            }
        }

        public double getVwap() {
            return vwap;
        }

        public DateTime getStartDate() {
            return startDate;
        }

        public DateTime getEndDate() {
            return endDate;
        }
    }
    
}
