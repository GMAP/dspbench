package org.dspbench.applications.bargainindex;

import com.google.common.collect.ImmutableList;
import org.dspbench.spout.parser.Parser;
import org.dspbench.util.stream.StreamValues;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Date;
import java.util.List;

public class StockQuotesParser extends Parser {
    private static final DateTimeFormatter dtFormatter = DateTimeFormat.forPattern("yyyy-MM-dd");

    private static final int NAME      = 0;
    private static final int DATE      = 1;
    private static final int OPEN      = 2;
    private static final int HIGH      = 3;
    private static final int LOW       = 4;
    private static final int CLOSE     = 5;
    private static final int ADJ_CLOSE = 6;
    private static final int VOLUME    = 7;

    @Override
    public List<StreamValues> parse(String input) {
        String[] record = input.split(",");

        if (record.length != 8)
            return null;

        // exclude header
        if (record[DATE].toLowerCase().equals("date")) {
            return null;
        }

        String stock      = record[NAME];
        DateTime date         = dtFormatter.parseDateTime(record[DATE]).withTimeAtStartOfDay();
        double open       = Double.parseDouble(record[OPEN]);
        double high       = Double.parseDouble(record[HIGH]);
        double low        = Double.parseDouble(record[LOW]);
        double close      = Double.parseDouble(record[CLOSE]);
        Integer volume = null;

        try {
            volume = Integer.parseInt(record[VOLUME]);
        } catch (NumberFormatException e) {
            Double number = Double.parseDouble(record[VOLUME]);
            volume = number.intValue();
        }

        int interval     = 24 * 60 * 60;

        return ImmutableList.of(new StreamValues(stock, close, volume, date, interval));
    }
}