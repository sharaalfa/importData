package org.megar.dev.queries;

import com.datastax.driver.core.Cluster;
import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.datastax.driver.core.Session;
import com.datastax.driver.core.exceptions.NoHostAvailableException;
import com.datastax.driver.core.exceptions.QueryExecutionException;
import com.datastax.driver.core.exceptions.QueryValidationException;
import com.datastax.driver.core.exceptions.UnsupportedFeatureException;

import java.io.UnsupportedEncodingException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.TimeZone;
import java.util.UUID;

public class DoCassandraJob {
    public static SimpleDateFormat simpleDateFormat = new SimpleDateFormat("yyyy-MM-dd");
    public static SimpleDateFormat dateTimeFormat = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss");
    public static TimeZone tz = TimeZone.getDefault();
    private static String year = "2017";

    private  void updateSenderCnt(Session session, String day) {
        ResultSet results = session.execute("SELECT * FROM sender_requests_for_days where day='" + day + "'");
        for (Row row : results) {
            long cntError = row.getLong("cnt_error");
            if (cntError > 0) {
                String sender_id = row.getString("sender_id");
                String request_type = row.getString("request_type");
                String role_id = row.getString("role_id");
                String format = row.getString("format");
                String mnemonics = row.getString("mnemonics");
                String executeString = "update sender_requests_for_days set cnt_error = cnt_error-" + String.valueOf(cntError) + " where day='" + day + "' and sender_id='" + sender_id + "' and request_type='" + request_type + "' and role_id='" + role_id + "' and format='" + format + "' and mnemonics='" + mnemonics + "'";
                session.execute(executeString);
                //System.out.println(row.getString("sender_id") + "   " + row.getString("request_type"));
                //System.out.println(executeString);
                //break;
            }
        }
    }

    private  void updateOriginatorCnt(Session session, String day) {
        ResultSet results = session.execute("SELECT * FROM originator_requests_for_days where day='" + day + "'");
        for (Row row : results) {
            long cntError = row.getLong("cnt_error");
            if (cntError > 0) {
                String sender_id = row.getString("sender_id");
                String originator_id = row.getString("originator_id");
                String request_type = row.getString("request_type");
                String role_id = row.getString("role_id");
                String format = row.getString("format");
                String mnemonics = row.getString("mnemonics");
                String executeString = "update originator_requests_for_days set cnt_error = cnt_error-" + String.valueOf(cntError) + " where day='" + day + "' and sender_id='" + sender_id + "' and originator_id='" + originator_id + "' and request_type='" + request_type + "' and role_id='" + role_id + "' and format='" + format + "' and mnemonics='" + mnemonics + "'";
                session.execute(executeString);
                //System.out.println(row.getString("sender_id") + "   " + row.getString("request_type"));
                //System.out.println(executeString);
                //break;
            }
        }
    }

    private  void updateRequestCnt(Session session, String day) {
        ResultSet results = session.execute("SELECT * FROM request_count_for_day where day='" + day + "'");
        for (Row row : results) {
            long cntError = row.getLong("cnt_error");
            if (cntError > 0) {
                String request_type = row.getString("request_type");
                String executeString = "update request_count_for_day set cnt_error = cnt_error-" + String.valueOf(cntError) + " where day='" + day + "' and request_type='" + request_type + "'";
                session.execute(executeString);
                //System.out.println(row.getString("sender_id") + "   " + row.getString("request_type"));
                //System.out.println(executeString);
                //break;
            }
        }
    }

    private  void updateTables(Session session, Row rowDetail, String responseCode ){
        long mdt = rowDetail.getTimestamp("message_datetime").getTime()-tz.getRawOffset();
        String messageDateTime = dateTimeFormat.format(mdt);
        //String messageDateTime = dateTimeFormat.format(rowDetail.getTimestamp("message_datetime").getTime()-);
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
                session.execute("update sender_requests_for_days set cnt_error=cnt_error+1 where day='" + messageDate + "' and sender_id='" + participantId + "' and request_type='" + requestType + "' and role_id='" + roleId + "' and format='" + format + "' and mnemonics='" + mnemonics + "'");
                session.execute("update request_count_for_day set cnt_error=cnt_error+1 where day='" + messageDate + "' and request_type='" + requestType + "'");

            } else {
                session.execute("update originator_requests_for_days set cnt_error=cnt_error+1 where day='" + messageDate + "' and sender_id='" + senderId + "' and originator_id='" + participantId + "' and request_type='" + requestType + "' and role_id='" + roleId + "' and format='" + format + "' and mnemonics='" + mnemonics + "'");
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
            session.execute(insertErrorParticipant);

            String insertErrorCode = "insert into error_requests_by_error_code (participant_id, is_sender, year, message_datetime, error_code, message_id, entity_id) values (";
            insertErrorCode += "'" + participantId + "'";
            insertErrorCode += "," + isSender;
            insertErrorCode += "," + year;
            insertErrorCode += ",'" + messageDateTime + "'";
            insertErrorCode += ",'" + responseCode + "'";
            insertErrorCode += "," + messageId.toString();
            insertErrorCode += ",'" + entityId + "')";
            session.execute(insertErrorCode);

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

    private  void doProcess(Session session, String day) {

        String selectResponses = "select message_id, is_error, entity_id, response_code from responses_for_days where day ='" + day + "' and is_error = 1";
        System.out.println("selectResponse=" + selectResponses);
        try {
            ResultSet rs = session.execute(selectResponses);
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
                    Row requestDetails = session.execute(selectDetails).one();
                    if (requestDetails != null) {
                        updateTables(session, requestDetails, responseCode);
                    } else {
                        System.out.println("Cannot find messageId = " + messageId + " and entityId = '" + entityId + "' in request_details. ");
                    }
                    selectDetails = "select * from request_details where message_id = " + messageId + " and entity_id = '" + entityId + "' and is_sender = 0";
                    requestDetails = session.execute(selectDetails).one();
                    if (requestDetails != null) {
                        updateTables(session, requestDetails, responseCode);
                    }
                } else {
                    //  messageId !=entityId
                    // ищем заголовок для пакетной обработки
                    String selectDetails = "select * from request_details where message_id = " + messageId + " and entity_id = '" + messageId.toString() + "'";
                    Row requestDetails2 = session.execute(selectDetails).one();
                    if (requestDetails2 != null) {
                        String packageId = requestDetails2.getString("package_id");
                        Row packageMessages = session.execute("select * from package_messages where package_id='" + packageId + "'").one();
                        if (packageMessages != null && packageMessages.getUUID("message_id_request").equals(messageId)) {
                            UUID messageIdResponse = packageMessages.getUUID("message_id_response");
                            selectDetails = "select * from request_details where message_id = " + messageIdResponse + " and entity_id = '" + entityId + "'";
                            Row requestDetails1 = session.execute(selectDetails).one();
                            if (requestDetails1 != null) {
                                // проверка обработки пакета для определения ошибок по сущностям
                                String originatorId = requestDetails1.getString("participant_id");
                                Row packageStatusRequest = session.execute("select * from package_status_by_entity where package_id='" + packageId + "' and message_id = " + messageId + " and entity_id='" + entityId + "'").one();
                                if (packageStatusRequest == null) {
                                    updateTables(session, requestDetails1, responseCode);
                                    session.execute("insert into package_status_by_entity(package_id, message_id, entity_id) values('" + packageId + "', " + messageId + ", '" + entityId + "')");
                                } else {
                                    System.out.println("After package_messages cannot find  message_id = " + messageIdResponse + " and entity_id = '" + entityId + "' in request_details. So cannot treat the original message_id = " + messageId);
                                }
                            } else {
                                if (packageMessages == null) {
                                    System.out.println("Cannot find packageId = '" + packageId + "' in package_messages. So cannot treat the original message_id = " + messageId);
                                } else if (!packageMessages.getUUID("message_id_request").equals(messageId)) {
                                    System.out.println("In package_messages for packageId = '" + packageId + "' the message_id_request = " + packageMessages.getUUID("message_id_request") + " and it is not equal to the requested message_id = " + messageId + ". So cannot treat the original message_id = " + messageId);
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
    public  void doJob(String date, String node, String kayspace) throws ParseException, UnsupportedEncodingException {
        Cluster cluster = Cluster.builder().addContactPoint(node).withPort(9042).build();
        Session session = cluster.connect(kayspace);
        String day = String.valueOf(date);
        System.out.println("dfddddddddddddddddddddddddddddddddddddddddddddddddура");
//        updateSenderCnt(session, day);
//        updateOriginatorCnt(session, day);
//        updateRequestCnt(session, day);
        doProcess(session, day);
        session.close();
        cluster.close();
    }

}
