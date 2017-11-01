package org.megar.dev.connector;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.Host;
import com.datastax.driver.core.Metadata;
import com.datastax.driver.core.Session;


public class CassandraConnector {
    /**
     * кластер Кассандры
     */
    private Cluster cluster;

    /**
     * сессия Кассандры
     */
    private Session session;

    public void connect(final String node, final int port, final String keyspace) {
        cluster = Cluster.builder().addContactPoint(node).withPort(port).build();
        final Metadata metadata = cluster.getMetadata();
        System.out.printf("Соединить с кластером: %s\n", metadata.getClusterName());
        for (final Host host: metadata.getAllHosts()) {
            System.out.printf("Datacenter: %s; Host: %s; Rack: %s\n",
                    host.getDatacenter(), host.getAddress(), host.getRack());
        }
        session = cluster.connect(keyspace);
    }

    public Session getSession() {
        return session;
    }

    public void close() {
        cluster.close();
    }
}
