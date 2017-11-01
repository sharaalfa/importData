package org.megar.dev.connector;


import org.junit.Assert;
import org.junit.jupiter.api.Test;


class CassandraConnectorTest {
    CassandraConnector cassandraConnector = new CassandraConnector();
    @Test
    void connect() {
        cassandraConnector.connect("10.1.100.83", 9042, "unifo2");
        Assert.assertNotNull(cassandraConnector.getSession());
    }

}