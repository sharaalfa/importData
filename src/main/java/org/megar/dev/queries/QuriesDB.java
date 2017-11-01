package org.megar.dev.queries;

import org.megar.dev.connector.DBPool;

import java.sql.Connection;
import java.sql.SQLException;


public class QuriesDB {
    private static final String driver = "oracle.jdbc.OracleDriver";

    public void updateDB(String uUrl, String uUser, String passwd) {
        try {
            Connection connection = new DBPool(uUrl, driver, 4, uUser, passwd).retrieve();
            java.sql.PreparedStatement prd2 = connection.prepareStatement("select * from C_ORIGINATOR_RQST_FOR_DAYS");
            java.sql.ResultSet resultSet = prd2.executeQuery();
            String[] str = null;
            while (resultSet.next()){

                System.out.println(resultSet.getString(1).substring(0,11));
            }
            prd2.close();


        } catch (SQLException e) {
            e.printStackTrace();
        }
    }
}
