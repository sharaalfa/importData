package org.megar.dev.queries;


import com.datastax.driver.core.*;


import org.apache.log4j.Logger;
import org.megar.dev.connector.CassandraConnector;
import org.megar.dev.connector.DBPool;

import java.sql.SQLException;
import java.sql.Connection;


/**
 *  Миграция актуальных данных с кассандры на Oracle DB.
 */
public class QueriesPersistence {


    /**
     * Драйвер для соединеия с DB ORACLE.
     */
    private static final String driver = "oracle.jdbc.OracleDriver";


    /**
     * логирование
     */
    private static  Logger log = Logger.getLogger(QueriesPersistence.class);
    /**
     * кластер Кассандры
     */
    private Cluster cluster;
    private final CassandraConnector client = new CassandraConnector();

    public QueriesPersistence(final String newHost, final int newPort, final String newKeyspace)
    {
        System.out.println("Connecting to IP Address " + newHost + ":" + newPort + "..." + newKeyspace + ":" + "...");
        client.connect(newHost, newPort, newKeyspace);
    }






    /**
     * Метод запроса с кассандры и записи в DB Oracle.
     */
    public void queryByDay(String dDate, String uUrl, String uUser, String passwd)
    {

        final ResultSet queryResults = client.getSession().execute(
                "select * from sender_requests_for_days");
        final ResultSet queryResults2 = client.getSession().execute(
                "select * from originator_requests_for_days");
        final ResultSet queryResults3 = client.getSession().execute(
                "select * from export_request_parameter_count");
        for (Row row: queryResults3) {
            String[] str = row.toString().split(",");
            try {
                Connection conn = new DBPool(uUrl, driver, 4, uUser, passwd).retrieve();
                //Connection conn = DBConnector.getConnection();
                java.sql.PreparedStatement prd = conn.prepareStatement("INSERT INTO C_EXP_REQUEST_PARAM_COUNT" +
                        "(\"DAY\", PARTICIPANT_ID, ESIA, PAYID, PAYID_KBK, PAYID_KBK_INN, PAYID_KBK_INN_KPP, PAYID_TIMESLOT," +
                        "PAYID_TIMESLOT_INN, PAYID_TIMESLOT_INN_KPP, PAYID_TIMESLOT_KBK, PAYID_TIMESLOT_KBK_INN," +
                        "PAYID_TIMESLOT_KBK_INN_KPP, SBILL, SBILL_KBK, SBILL_KBK_INN,  SBILL_KBK_INN_KPP, " +
                        "SBILL_TIMESLOT, SBILL_TIMESLOT_INN, SBILL_TIMESLOT_INN_KPP, SBILL_TIMESLOT_KBK," +
                        "SBILL_TIMESLOT_KBK_INN, SBILL_TIMESLOT_KBK_INN_KPP, SYSID, SYSID_KBK, SYSID_KBK_INN," +
                        "SYSID_KBK_INN_KPP, SYSID_TIMESLOT_KBK, SYSID_TIMESLOT_KBK_INN, SYSID_TIMESLOT_KBK_INN_KPP," +
                        "TIMESLOT, TIMESLOT_INN_KPP, TIMESLOT_KBK, TIMESLOT_KBK_INN, TIMESLOT_KBK_INN_KPP, TIMESLOT_INN) " +
                        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

                if (dDate.equals(str[0].substring(4, 14))) {
                    prd.setDate(1, java.sql.Date.valueOf(str[0].substring(4, 14).trim()));
                    prd.setString(2, str[1].trim());
                    for (int i = 1; i < str.length - 1; i++) {
                        if (str[1 + i].trim().contains("NULL") || str[1 + i].replaceAll("^\\[|\\]$", "")
                                .trim().equalsIgnoreCase("0")) {
                            prd.setInt(i + 2, 0);
                        } else {
                            prd.setInt(i + 2, Integer.parseInt(str[i + 1].replaceAll("^\\[|\\]$", "").trim()));
                        }
                    }

                    prd.executeUpdate();
                    prd.close();

                }

                str = null;
            } catch (SQLException e) {
                log.error("нет соединения с Oracle DB");
            }
        }
        for (Row row: queryResults) {
            String[] str = row.toString().split(",");
            try {
                Connection conn =  new DBPool(uUrl, driver, 4, uUser, passwd).retrieve();
                //Connection conn = DBConnector.getConnection();
                java.sql.PreparedStatement prd = conn.prepareStatement("INSERT INTO C_SENDER_RQST_FOR_DAYS " +
                        "(\"DAY\", SENDER_ID, REQUEST_TYPE, ROLE_ID, FORMAT, MNEMONICS, CNT, CNT_ERROR) " +
                        " VALUES (?,?,?,?,?,?,?,?)");
                System.out.println(java.sql.Date.valueOf(str[0].substring(4,14)));
                if (dDate.equals(str[0].substring(4,14))) {
                    prd.setDate(1, java.sql.Date.valueOf(str[0].substring(4,14).trim()));
                    prd.setString(2, str[1].trim());
                    prd.setString(3, str[2].trim());
                    prd.setString(4, str[3].trim());
                    prd.setString(5, str[4].trim());
                    prd.setString(6, str[5].trim());
                    if (str[6].trim().contains("NULL") || Integer.parseInt(str[6]
                            .replaceAll("^\\[|\\]$", "").trim()) == 0)
                    {
                        prd.setInt(7, 0);
                    }else{
                        prd.setInt(7, Integer.parseInt(str[6].replaceAll("^\\[|\\]$", "").trim()));
                    }
                    if (str[7].trim().contains("NULL") || Integer.parseInt(str[7]
                            .replaceAll("^\\[|\\]$", "").trim()) == 0)
                    {
                        prd.setInt(8, 0);
                    }else{
                        prd.setInt(8, Integer.parseInt(str[7].replaceAll("^\\[|\\]$", "").trim()));
                    }
                    prd.executeUpdate();
                    prd.close();

                }
                str = null;

            } catch (SQLException e) {
                log.error("нет соединения с Oracle DB1");
            }

        }
        for (Row row: queryResults2) {
            String[] str = row.toString().split(",");
            try {

                Connection connection =  new DBPool(uUrl, driver, 4, uUser, passwd).retrieve();
                //Connection connection = DBConnector.getConnection();
                java.sql.PreparedStatement prd2 = connection.prepareStatement("INSERT INTO C_ORIGINATOR_RQST_FOR_DAYS " +
                        "(\"DAY\", SENDER_ID, ORIGINATOR_ID, REQUEST_TYPE, ROLE_ID, FORMAT, MNEMONICS, CNT, CNT_ERROR) " +
                        " VALUES (?,?,?,?,?,?,?,?,?)");
                System.out.println(java.sql.Date.valueOf(str[0].substring(4,14)));
                if (dDate.equals(str[0].substring(4,14))) {
                    prd2.setDate(1, java.sql.Date.valueOf(str[0].substring(4,14).trim()));
                    prd2.setString(2, str[1].trim());
                    prd2.setString(3, str[2].trim());
                    prd2.setString(4, str[3].trim());
                    prd2.setString(5, str[4].trim());
                    prd2.setString(6, str[5].trim());
                    prd2.setString(7, str[6].trim());
                    if (str[7].trim().contains("NULL") || Integer.parseInt(str[7]
                            .replaceAll("^\\[|\\]$", "").trim()) == 0)
                    {
                        prd2.setInt(8, 0);
                    }else{
                        prd2.setInt(8, Integer.parseInt(str[7].replaceAll("^\\[|\\]$", "").trim()));
                    }
                    if (str[8].trim().contains("NULL") || Integer.parseInt(str[8]
                            .replaceAll("^\\[|\\]$", "").trim()) == 0)
                    {
                        prd2.setInt(9, 0);
                    }else{
                        prd2.setInt(9, Integer.parseInt(str[8].replaceAll("^\\[|\\]$", "").trim()));
                    }
                    prd2.executeUpdate();
                    prd2.close();

                }
                str = null;

            } catch (SQLException e) {
                log.error("нет соединения с Oracle DB2");
            }

        }
        close();

    }
    public void updateDB(String uUrl, String uUser, String passwd) {
        final ResultSet queryResults = client.getSession().execute(
                "select * from sender_requests_for_days");
        final ResultSet queryResults2 = client.getSession().execute(
                "select * from originator_requests_for_days");
        final ResultSet queryResults3 = client.getSession().execute(
                "select * from export_request_parameter_count");
        for (Row row: queryResults) {
            String[] str = row.toString().split(",");
            try {
                Connection connection = new DBPool(uUrl, driver, 4, uUser, passwd).retrieve();
                java.sql.PreparedStatement prd2 = connection.prepareStatement("select * from C_ORIGINATOR_RQST_FOR_DAYS");
                java.sql.ResultSet resultSet = prd2.executeQuery();
                int count = 0;
                while (resultSet.next()) {
                    if (resultSet.getString(1).substring(0, 11).trim().contains(str[0].substring(4, 14).trim()))
                        System.out.println(resultSet.getString(2) + ", " + count++);
                }
                prd2.close();


            } catch (SQLException e) {
                e.printStackTrace();
            }
        }
        close();

    }




    /**
     * Закрытие соединения с кассандрой.
     */
    private void close()
    {
        client.close();
    }
}
