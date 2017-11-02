package org.megar.dev;



import org.megar.dev.queries.DoCassandraJob;
import org.megar.dev.queries.QueriesPersistence;
import org.megar.dev.queries.SchedulerTask;

import java.io.IOException;
import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;

public class Main {


    /**
     * Запуск  импорт данных из cassandra в oracle (за предыдущий день).
     * @param args
     */
    public static void main(final String[] args) {
        Calendar Current_Calendar = Calendar.getInstance();
        Date Current_Date = Current_Calendar.getTime();
        try {

            new DoCassandraJob().doJob(previosDay(Current_Date).toString(), args[0], args[1]);
            System.out.println("Джоб первый отработал");


        } catch (ParseException e) {

        } catch (UnsupportedEncodingException e) {

        }/*finally {
            new QueriesPersistence(args[0], 9042, args[1])
                    .queryByDay(previosDay(Current_Date).toString(),
                            args[2], args[3], args[4]);

        }
        {
            System.out.println("Джобы отработали");
        }*/
        {

            System.out.println("Инициализация второго джоба");
        }

        Timer timer = new Timer();
        try {
            Thread.sleep(10000);
            timer.schedule(new SchedulerTask(args[0], args[1],
                    previosDay(Current_Date).toString(), args[2],
                    args[3], args[4]), 0, 450000);

            for (int i = 0; i <= 1; i++){
                Thread.sleep(450000);

                if(i==1) {
                    System.out.println("Джобы отработали");
                    System.exit(0);
                }
            }
        } catch (InterruptedException e) {

        }


    }


    /**main
     * Метода расчета предыдущего дня.
     * @param date
     * @return
     */
    public static Date previosDay(Date date) {
        Calendar cal = Calendar.getInstance();
        cal.setTime(date);
        cal.add(Calendar.DAY_OF_MONTH, 0);

        return new java.sql.Date(cal.getTime().getTime());
    }
}
