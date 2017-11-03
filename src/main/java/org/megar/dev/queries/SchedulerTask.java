package org.megar.dev.queries;



import java.util.TimerTask;

public class SchedulerTask extends TimerTask {
    private QueriesPersistence queriesPersistence;


    public SchedulerTask(QueriesPersistence queriesPersistence) {
        this.queriesPersistence=queriesPersistence;
    }

    @Override
    public void run() {


    }
}
