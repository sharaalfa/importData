package org.megar.dev.queries;


import org.junit.Assert;
import org.junit.jupiter.api.Test;
import java.util.Calendar;

class QueriesPersistenceTest {

    String date = Calendar.getInstance().getTime().toString();
    QueriesPersistence queriesPersistence = new QueriesPersistence("10.1.100.83",
            9042, "unifo2");

    @Test
    void queryByDay() {
        queriesPersistence.queryByDay(date,
                "jdbc:oracle:thin:@oracle.megar.ru:1521:oracle",
                "otchet", "otchet");

        Assert.assertNotNull(queriesPersistence);
    }

}