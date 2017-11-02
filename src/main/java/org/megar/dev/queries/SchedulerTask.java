package org.megar.dev.queries;



import java.util.TimerTask;

public class SchedulerTask extends TimerTask {
    private String node;
    private String keyspace;
    private String date;
    private String path;
    private String user;
    private String password;

    public SchedulerTask(String node, String keyspace,
                         String date, String path,
                         String user, String password) {
        this.node = node;
        this.keyspace = keyspace;
        this.date = date;
        this.path = path;
        this.user = user;
        this.password = password;
    }

    @Override
    public void run() {
        new QueriesPersistence(node, 9042, keyspace)
                .queryByDay(date, path, user, password);

    }
}
