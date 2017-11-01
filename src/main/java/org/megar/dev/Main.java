package org.megar.dev;



import org.megar.dev.queries.QueriesPersistence;
import java.util.Calendar;
import java.util.Date;

public class Main {


    /**
     * Запуск  импорт данных из cassandra в oracle (за предыдущий день).
     * @param args
     */
    public static void main(final String[] args) {
        Calendar Current_Calendar = Calendar.getInstance();
        Date Current_Date = Current_Calendar.getTime();
        new QueriesPersistence(args[0], 9042, args[1])
                .queryByDay(previosDay(Current_Date).toString(), args[2], args[3], args[4]);



    }


    /**
     * Метода расчета предыдущего дня.
     * @param date
     * @return
     */
    public static Date previosDay(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DAY_OF_MONTH, -1);

        return new java.sql.Date(cal.getTime().getTime());
    }
}
