package org.megar.dev.connector;

import org.apache.log4j.Logger;
import org.junit.Assert;
import org.junit.jupiter.api.Test;
import java.sql.Connection;
import java.sql.SQLException;


class DBPoolTest {
    Connection connection;

    private static Logger log = Logger.getLogger(DBPoolTest.class);

    DBPool dbPool = new DBPool("jdbc:oracle:thin:@10.1.100.1:1521:oracle",
            "oracle.jdbc.OracleDriver", 4,"otchet", "otchet");
    @Test
    void retrieve() {
       try {
           connection = dbPool.retrieve();
           Assert.assertNotNull(connection.getMetaData());
       } catch (SQLException e) {
           log.error("нет соединеия с бд");
       }
    }

}