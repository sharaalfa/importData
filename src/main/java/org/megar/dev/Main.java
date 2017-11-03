package org.megar.dev;

import org.apache.log4j.Logger;
import org.megar.dev.queries.QueriesPersistence;
import org.megar.dev.queries.SchedulerTask;


import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Timer;

public class Main {
    /**
     * логирование
     */
    private static Logger log = Logger.getLogger(QueriesPersistence.class);


    /**
     * Запуск  импорт данных из cassandra в oracle (за предыдущий день).
     * @param args
     */
    public static void main(final String[] args) {
        Calendar Current_Calendar = Calendar.getInstance();
        Date Current_Date = Current_Calendar.getTime();
        Timer timer = new Timer();
        Timer timer1 = new Timer();
        QueriesPersistence queriesPersistence = new QueriesPersistence(args[0],9042,args[1]);
        SchedulerTask schedulerTask = new SchedulerTask(queriesPersistence);
        try {
            log.info("старт первого джоба");
            System.out.println("start first job");
            Thread.sleep(10000);
            timer.schedule(schedulerTask, 0, 300000);
            String day = new SimpleDateFormat("yyyy-MM-dd").format(previosDay(Current_Date));
            queriesPersistence.doJob(day);
            for (int i = 0; i <= 1; i++){
                log.info("старт второго джоба");
                System.out.println("stop first and start second job");
                Thread.sleep(300000);
                try {
                    SchedulerTask schedulerTask1 = new SchedulerTask(queriesPersistence);
                    queriesPersistence.queryByDay(previosDay(Current_Date).toString(),
                            args[2],args[3],args[4]);
                    timer1.schedule(schedulerTask1, 0, 300000);


                    for (int j = 0; j <= 1; j++){
                        Thread.sleep(5000);
                        log.info("второй джоб отработал нормально");
                        System.out.println("2nd jobs worked");

                        if(j==1) {
                            log.info("первый и второй джобы отработали нормально");
                            System.out.println("1st and 2nd jobs worked");
                            System.exit(0);
                        }
                    }
                } catch (InterruptedException e) {
                    log.error("ошибка работы второго джоба");
                }
                if(i==1) {
                    log.info("первый и второй джобы отработали нормально");
                    System.out.println("1st and 2nd jobs worked");
                    System.exit(0);
                }

            }


        } catch (InterruptedException e) {
            log.error("ошибка работы первого джоба");
            System.out.println("only 1st job worked");

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
