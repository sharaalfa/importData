package org.megar.dev.queries;


import com.datastax.driver.core.*;


import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;
import org.apache.log4j.Logger;
import org.megar.dev.connector.CassandraConnector;
import org.megar.dev.connector.DBPool;

import java.sql.SQLException;
import java.sql.Connection;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.TimeZone;
import java.util.UUID;


/**
 *  Миграция актуальных данных с кассандры на Oracle DB.
 */
public class QueriesPersistence {
    /**
     * получение необходимого формата даты для первого джоба
     */

    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public static SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static TimeZone tz = TimeZone.getDefault();
    private static String year = "2017";


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

    private final CassandraConnector client = new CassandraConnector();

    public QueriesPersistence(final String newHost, final int newPort, final String newKeyspace)
    {
        System.out.println("Connecting to IP Address " + newHost + ":" + newPort + "..." + newKeyspace + ":" + "...");
        client.connect(newHost, newPort, newKeyspace);
    }




    private void updateTables(Row rowDetail, String responseCode ){
        long mdt = rowDetail.getTimestamp("message_datetime").getTime()-tz.getRawOffset();
        String messageDateTime = dateTimeFormat.format(mdt);
        String messageDate = simpleDateFormat.format(mdt);
        int isSender = rowDetail.getInt("is_sender");
        String participantId = rowDetail.getString("participant_id");
        String senderId = rowDetail.getString("sender_id");
        String mnemonics = rowDetail.getString("mnemonics");
        String requestType = rowDetail.getString("request_type");
        String roleId = rowDetail.getString("role_id");
        String format = rowDetail.getString("format");
        UUID messageId = rowDetail.getUUID("message_id");
        String entityId = rowDetail.getString("entity_id");

        try {
            if (isSender == 1) {
                client.getSession().execute("update sender_requests_for_days set cnt_error=cnt_error+1 where day='" + messageDate + "' and sender_id='" + participantId + "' and request_type='" + requestType + "' and role_id='" + roleId + "' and format='" + format + "' and mnemonics='" + mnemonics + "'");
                client.getSession().execute("update request_count_for_day set cnt_error=cnt_error+1 where day='" + messageDate + "' and request_type='" + requestType + "'");

            } else {
                client.getSession().execute("update originator_requests_for_days set cnt_error=cnt_error+1 where day='" + messageDate + "' and sender_id='" + senderId + "' and originator_id='" + participantId + "' and request_type='" + requestType + "' and role_id='" + roleId + "' and format='" + format + "' and mnemonics='" + mnemonics + "'");
            }
            String insertErrorParticipant;
            insertErrorParticipant = "insert into error_requests_by_participant (participant_id,  is_sender,   year,  message_datetime,  error_code,  message_id, entity_id) values (";
            insertErrorParticipant += "'" + participantId + "'";
            insertErrorParticipant += "," + isSender;
            insertErrorParticipant += "," + year;
            insertErrorParticipant += ",'" + messageDateTime + "'";
            insertErrorParticipant += ",'" + responseCode + "'";
            insertErrorParticipant += "," + messageId.toString();
            insertErrorParticipant += ",'" + entityId + "')";
            client.getSession().execute(insertErrorParticipant);

            String insertErrorCode = "insert into error_requests_by_error_code (participant_id, is_sender, year, message_datetime, error_code, message_id, entity_id) values (";
            insertErrorCode += "'" + participantId + "'";
            insertErrorCode += "," + isSender;
            insertErrorCode += "," + year;
            insertErrorCode += ",'" + messageDateTime + "'";
            insertErrorCode += ",'" + responseCode + "'";
            insertErrorCode += "," + messageId.toString();
            insertErrorCode += ",'" + entityId + "')";
            client.getSession().execute(insertErrorCode);

        } catch (NoHostAvailableException e1) {
            System.out.println(e1.getMessage());
        } catch (QueryExecutionException e2) {
            System.out.println(e2.getMessage());
        } catch (QueryValidationException e3) {
            System.out.println(e3.getMessage());
        } catch (UnsupportedFeatureException e4) {
            System.out.println(e4.getMessage());
        }
    }

    private void doProcess(String day) {

        String selectResponses = "select message_id, is_error, entity_id, response_code from responses_for_days where day ='" + day + "' and is_error = 1";
        System.out.println("selectResponse=" + selectResponses);
        try {
            ResultSet rs = client.getSession().execute(selectResponses);
            System.out.println("rs.getAvailableWithoutFetching()=" + rs.getAvailableWithoutFetching());
            if (rs.getAvailableWithoutFetching() == 0) {
                return;
            }
            for (Row row : rs) {
                UUID messageId = row.getUUID("message_id");
                String entityId = row.getString("entity_id");
                String responseCode = row.getString("response_code");
                if (entityId.equals(messageId.toString())) {
                    //System.out.println("messageId=" + messageId.toString() + " entityId=" + entityId);
                    String selectDetails = "select * from request_details where message_id = " + messageId + " and entity_id = '" + entityId + "' and is_sender = 1";
                    Row requestDetails = client.getSession().execute(selectDetails).one();
                    if (requestDetails != null) {
                        updateTables(requestDetails, responseCode);
                    } else {
                        System.out.println("Cannot find messageId = " + messageId + " and entityId = '" + entityId + "' in request_details. ");
                    }
                    selectDetails = "select * from request_details where message_id = " + messageId + " and entity_id = '" + entityId + "' and is_sender = 0";
                    requestDetails = client.getSession().execute(selectDetails).one();
                    if (requestDetails != null) {
                        updateTables(requestDetails, responseCode);
                    }
                } else {
                    //  messageId !=entityId
                    // ищем заголовок для пакетной обработки
                    String selectDetails = "select * from request_details where message_id = " + messageId + " and entity_id = '" + messageId.toString() + "'";
                    Row requestDetails2 = client.getSession().execute(selectDetails).one();
                    if (requestDetails2 != null) {
                        String packageId = requestDetails2.getString("package_id");
                        Row packageMessages = client.getSession().execute("select * from package_messages where package_id='" + packageId + "'").one();
                        if (packageMessages != null && messageId.equals(packageMessages.getUUID("message_id_request"))) {
                            UUID messageIdResponse = packageMessages.getUUID("message_id_response");
                            Row requestDetails1;
                            if (messageIdResponse == null) {
                                requestDetails1 = null;
                            } else {
                                selectDetails = "select * from request_details where message_id = " + messageIdResponse + " and entity_id = '" + entityId + "'";
                                requestDetails1 = client.getSession().execute(selectDetails).one();
                            }
                            if (requestDetails1 != null) {
                                // проверка обработки пакета для определения ошибок по сущностям
                                Row packageStatusRequest = client.getSession().execute("select * from package_status_by_entity where package_id='" + packageId + "' and message_id = " + messageId + " and entity_id='" + entityId + "'").one();
                                if (packageStatusRequest == null) {
                                    updateTables(requestDetails1, responseCode);
                                    client.getSession().execute("insert into package_status_by_entity(package_id, message_id, entity_id) values('" + packageId + "', " + messageId + ", '" + entityId + "')");
                                } else {
                                    System.out.println("After package_messages cannot find  message_id = " + messageIdResponse + " and entity_id = '" + entityId + "' in request_details. So cannot treat the original message_id = " + messageId);
                                }
                            } else {
                                if (packageMessages == null) {
                                    System.out.println("Cannot find packageId = '" + packageId + "' in package_messages. So cannot treat the original message_id = " + messageId);
                                } else if (!messageId.equals(packageMessages.getUUID("message_id_request"))) {
                                    System.out.println("In package_messages for packageId = '" + packageId + "' the message_id_request = " + packageMessages.getUUID("message_id_request") + " and it is not equal to the requested message_id = " + messageId + ". So cannot treat the original message_id = " + messageId);
                                } else if (messageIdResponse == null) {
                                    System.out.println("In package_messages for packageId = '" + packageId + "' the message_id_response is null");
                                } else {
                                    System.out.println("Program error #L-114");
                                }
                            }
                        }
                    } else {
                        System.out.println("Cannot find messageId = " + messageId + " and entityId = '" + entityId + "' in request_details. ");
                    }
                }
            }

        } catch (Exception e) {
            System.out.println(e.getMessage());
        }
    }
    public  void doJob(String date){
        doProcess(date);
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
                        "SYSID_KBK_INN_KPP, SYSID_TIMESLOT, SYSID_TIMESLOT_KBK, SYSID_TIMESLOT_KBK_INN, SYSID_TIMESLOT_KBK_INN_KPP," +
                        "TIMESLOT, TIMESLOT_INN_KPP, TIMESLOT_KBK, TIMESLOT_KBK_INN, TIMESLOT_KBK_INN_KPP, TIMESLOT_OKTMO, ZERO_UIN, TIMESLOT_INN) " +
                        " VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)");

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




    /**
     * Закрытие соединения с кассандрой.
     */
    private void close()
    {
        client.close();
    }
}
