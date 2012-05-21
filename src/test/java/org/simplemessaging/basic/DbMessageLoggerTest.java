package org.simplemessaging.basic;

import org.h2.jdbcx.JdbcDataSource;
import org.junit.Before;
import org.junit.BeforeClass;
import org.junit.Test;

import java.sql.SQLException;
import java.util.Arrays;
import java.util.List;

import static org.junit.Assert.*;

/**
 */
public class DbMessageLoggerTest {
    static DbMessageLogger dbLogger;
    static String listener = "testListener2";
    static JdbcDataSource ds = new JdbcDataSource();

    @BeforeClass
    public static void before() {
        ds = new JdbcDataSource();
        ds.setURL("jdbc:h2:~/h2dbtest1");
        ds.setUser("sa");
        ds.setPassword("sa");
        dbLogger = new DbMessageLogger();
        dbLogger.setDataSource(ds);
        dbLogger.register(listener);
    }

    @Before
    public void beforeTest() {
        try {
            ds.getConnection().createStatement().execute("delete from \"LOG_" + listener + "\"");
        } catch (SQLException e) {
            e.printStackTrace();
            fail();
        }
    }



}
