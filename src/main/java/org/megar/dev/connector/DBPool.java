package org.megar.dev.connector;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.util.Vector;

public class DBPool {

    /**
     * Список подключений сущуствующих и и спользуемых соотвественно.
     */
    private Vector<Connection> availableConns = new Vector<Connection>();
    private Vector<Connection> usedConns = new Vector<Connection>();
    /**
     * Параметры подключний к DB Oracle
     */
    private String url;
    private String user;
    private String password;

    /**
     * Конструктор создания пула соединений с базой данных.
     * @param url
     * @param driver
     * @param initConnCnt
     * @param user
     * @param password
     */
    public DBPool(String url, String driver, int initConnCnt, String user, String password) {
        try {
            Class.forName(driver);
        } catch (Exception e) {
            e.printStackTrace();
        }
        this.url = url;
        this.user = user;
        this.password =password;
        for (int i = 0; i < initConnCnt; i++) {
            availableConns.addElement(getConnection());
        }
    }

    /**
     * Метод соединения с базой данных.
     * @return
     */
    private Connection getConnection() {
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, user, password);
        } catch (Exception e) {
            e.printStackTrace();
        }
        return conn;
    }

    /**
     * Потокобезопасные соединения с базой данных
     * @return
     * @throws SQLException
     */
    public synchronized Connection retrieve() throws SQLException {
        Connection newConn = null;
        if (availableConns.size() == 0) {
            newConn = getConnection();
        } else {
            newConn = (Connection) availableConns.lastElement();
            availableConns.removeElement(newConn);
        }
        usedConns.addElement(newConn);
        return newConn;
    }

}
