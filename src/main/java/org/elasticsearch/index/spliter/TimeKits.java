package org.elasticsearch.index.spliter;

import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

/**
 * @author xingtianyu(code4j) Created on 2018-2-27.
 */
public class TimeKits {

    public static String getCurrentDateFormatted(String pattern){
        DateTime dateTime = new DateTime();
        return dateTime.toString(pattern);
    }

    public static DateTime getDateTime(String time, String pattern){
        DateTimeFormatter format = DateTimeFormat.forPattern(pattern);
        return DateTime.parse(time, format);
    }

    /**
     * front是否在behind之后
     * @param front
     * @param behind
     * @return
     */
    public static boolean after(DateTime front,DateTime behind){
        return front.isAfter(behind.getMillis());
    }

}
