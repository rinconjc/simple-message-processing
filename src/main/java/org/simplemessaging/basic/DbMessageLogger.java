package org.simplemessaging.basic;

import com.esotericsoftware.kryo.Kryo;
import com.esotericsoftware.kryo.Serializer;
import de.javakaffee.kryoserializers.ArraysAsListSerializer;
import de.javakaffee.kryoserializers.SubListSerializer;
import org.apache.log4j.Logger;
import org.simplemessaging.Message;
import org.simplemessaging.MessageLogger;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.nio.ByteBuffer;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;

/**
 * MessageLogger implementations that persists notifications in a database
 * @param <T>
 */
public class DbMessageLogger<T extends Message> implements MessageLogger<T> {

    private final static Logger LOGGER = Logger.getLogger(DbMessageLogger.class);
    private static final int CAPACITY = 10000;

    private int bufferCapacity = 10*1024;

    private enum Status{PROCESSING, FAILED, INVALID}

    private DataSource dataSource;

    private String tablePrefix="LOG_";

    private List<String> listeners = new ArrayList<String>();

    transient Kryo kryo ;

    public DbMessageLogger() {
        kryo = new Kryo(){
            public Serializer newSerializer(Class aClass) {
                if(SubListSerializer.canSerialize(aClass))
                    return new SubListSerializer(this);
                return super.newSerializer(aClass);
            }
        };
        kryo.register(Arrays.asList("").getClass(), new ArraysAsListSerializer(kryo));
        kryo.setRegistrationOptional(true);
    }

    public void logProcessing(String listenerName, Message message) {
        insertOrUpdateNotification(listenerName, message, Status.PROCESSING);
    }

    public void logCompleted(String listenerName, Message message) {
        removeNotification(listenerName, message);
    }

    public void logFailed(String listenerName, Message message, Exception e) {
        updateNotification(listenerName, message, Status.FAILED);
    }

    public void logInvalid(String listenerName, Message message, Exception e) {
        updateNotification(listenerName, message, Status.INVALID);
    }

    public List<T> getFailedMessages(String listenerName, int max){
        return loadNotifications(listenerName, Status.FAILED, max, 0);
    }

    public List<T> getFailedMessages(String listenerName, int max, int maxFailCount){
        return loadNotifications(listenerName, Status.FAILED, max, maxFailCount);
    }

    public List<T> getInProcessMessages(String listenerName, int max) {
        return loadNotifications(listenerName, Status.PROCESSING, max, 0);
    }
    
    public T findNotification(String listenerName, int id){
        String sql = "SELECT notification, failCount FROM \"" + tablePrefix + listenerName
                + "\" WHERE id=? ";

        List<Object> list = new JdbcTemplate(dataSource).query(sql, new RowMapper<Object>() {
            public Object mapRow(ResultSet resultSet, int i) throws SQLException {
                T notification = deserialise(resultSet.getBytes(1));
                notification.setFailCount(resultSet.getInt(2));
                return notification;
            }
        }, id);

        return list.isEmpty()? null: (T) list.get(0);
    }
    
    /**
     * Registers a notification listener for logging and marks any stalled notification as failed
     * @param listenerName
     */
    public void register(String listenerName){
        LOGGER.info("Registering Message logger for :" + listenerName);
        listeners.add(listenerName);
        Connection connection = null;
        try {
            connection = dataSource.getConnection();
            String tableName = tablePrefix + listenerName;
            ResultSet tables = connection.getMetaData().getTables(null, null, tableName, null);
            if(!tables.next()){
                LOGGER.info("Logging table not found for listener " + listenerName+ "; A new table will be created");
                String sql = "CREATE TABLE \"" + tableName + "\" (id int not null, notification VARBINARY(100000)" +
                        ", status VARCHAR(15), PRIMARY KEY(id), failCount INT DEFAULT 0)";
                LOGGER.debug("Executing :" + sql);
                boolean result = connection.createStatement().execute(sql);
                LOGGER.info("Logging table creation success:" + result);
            }else{
                LOGGER.info("Logging table already defined in database for " + listenerName);
            }
            markStalledAsFailed(listenerName);
        } catch (SQLException e) {
            LOGGER.error("Failed registering notification logger for " + listenerName, e);
        } finally {
            try {
                if(connection!=null)connection.close();
            } catch (SQLException e) {
                LOGGER.error(e);
            }
        }
    }

    private void insertOrUpdateNotification(String listenerName, Message message, Status status){
        String idSql = "SELECT id, status FROM \"" + tablePrefix + listenerName + "\" WHERE id=?";
        String sql = "INSERT INTO \"" + tablePrefix + listenerName + "\" (id, message, status) values(?,?,?)";
        Connection con = null;
        int id = message.getId();
        try {
            con = dataSource.getConnection();
            PreparedStatement idstmt = con.prepareStatement(idSql);
            idstmt.setInt(1, id);
            ResultSet rs = idstmt.executeQuery();

            if(rs.next()){
                LOGGER.debug("Message already logged: " + message);
                if(!status.name().equals(rs.getString(2))){
                    String updateSql = "UPDATE \"" + tablePrefix + listenerName + "\" SET status=? WHERE id=?";
                    PreparedStatement pstmt = con.prepareStatement(updateSql);
                    pstmt.setString(1, status.name());
                    pstmt.setInt(2, id);
                    pstmt.executeUpdate();
                }
                return;
            }
            
            PreparedStatement pstmt = con.prepareStatement(sql);
            pstmt.setInt(1, id);
            pstmt.setBytes(2, serialise(message));
            pstmt.setString(3, status.name());
            int count = pstmt.executeUpdate();
        } catch (SQLException e) {
            LOGGER.error("Failed inserting message log " + message, e);
        } finally {
            try{if(con!=null) con.close();}
            catch (Exception e){LOGGER.error(e);}
        }
    }

    private void removeNotification(String listenerName, Message message){
        String sql = "DELETE \"" + tablePrefix + listenerName + "\" WHERE id=?";
        Connection con = null;
        try {
            con = dataSource.getConnection();
            PreparedStatement pstmt = con.prepareStatement(sql);
            pstmt.setInt(1, message.getId());
            int count = pstmt.executeUpdate();
        }catch (SQLException e){
            LOGGER.error("Failed deleting message log", e);
        } finally {
            if(con!=null){
                try{con.close();}
                catch (Exception e){LOGGER.error(e);}
            }
        }
    }

    private void updateNotification(String listenerName, Message message, Status status){
        String sql = "UPDATE \"" + tablePrefix + listenerName
                + "\" SET " + (status== Status.FAILED?"failCount=failCount+1," :"") //increase failCount if it's FAILED
                + " status=? WHERE id=?";

        Connection con = null;
        try {
            con = dataSource.getConnection();
            PreparedStatement pstmt = con.prepareStatement(sql);
            pstmt.setString(1, status.toString());
            pstmt.setInt(2, message.getId());
            int count = pstmt.executeUpdate();
            if(count==0)
                LOGGER.warn("No rows affected by update of:" + message + ", " + status);
        }catch (SQLException e){
            LOGGER.error("Failed updating message log", e);
        } finally {
            if(con!=null){
                try{con.close();}
                catch (Exception e){LOGGER.error(e);}
            }
        }
    }

    public List<T> loadNotifications(String listenerName, Status status, int max, int maxFailCount){
        String sql = "SELECT notification, failCount FROM \"" + tablePrefix + listenerName
                + "\" WHERE status=? "
                + (maxFailCount<=0?"": " and failCount <= ?"); //conditional criterion

        Connection con = null;
        List<T> notifications = new ArrayList<T>();
        try {
            con = dataSource.getConnection();
            PreparedStatement pstmt = con.prepareStatement(sql);
            pstmt.setString(1, status.toString());
            if(maxFailCount>0)
                pstmt.setInt(2, maxFailCount);
            pstmt.setFetchSize(max);
            ResultSet resultSet = pstmt.executeQuery();
            int count=0;
            while (resultSet.next() && ++count<=max){
                try{
                    T notification = deserialise(resultSet.getBytes(1));
                    notification.setFailCount(resultSet.getInt(2));
                    notifications.add(notification);
                    count++;
                }catch (Exception e){
                    LOGGER.warn("Failed deserialising notification" , e);
                }
            }
        }catch (SQLException e){
            LOGGER.error("Failed updating notification log", e);
        } finally {
            if(con!=null){
                try{con.close();}
                catch (Exception e){LOGGER.error(e);}
            }
        }
        return notifications;
    }

    private void markStalledAsFailed(String listenerName){
        String sql = "UPDATE \"" + tablePrefix + listenerName + "\" SET status=? WHERE status=?";
        Connection con = null;
        try {
            con = dataSource.getConnection();
            PreparedStatement pstmt = con.prepareStatement(sql);
            pstmt.setString(1, Status.FAILED.name());
            pstmt.setString(2, Status.PROCESSING.name());
            pstmt.executeUpdate();
        }catch (SQLException e){
            LOGGER.error("Failed marking stalled notifications as failed", e);
        } finally {
            if(con!=null){
                try{con.close();}
                catch (Exception e){LOGGER.error(e);}
            }
        }
    }

    private byte[] serialise(Message message){
        //kryo.register(message.getClass());
        ByteBuffer buffer = ByteBuffer.allocate(bufferCapacity);
        kryo.writeClassAndObject(buffer, message);
        byte[] result = new byte[buffer.flip().limit()];
        buffer.get(result);
        return result;
    }

    private T deserialise(byte[] data){
        ByteBuffer buffer = ByteBuffer.wrap(data);
        T notification = (T) kryo.readClassAndObject(buffer);
        return notification;
    }

    public DataSource getDataSource() {
        return dataSource;
    }

    public void setDataSource(DataSource dataSource) {
        this.dataSource = dataSource;
    }

    public String getTablePrefix() {
        return tablePrefix;
    }

    public void setTablePrefix(String tablePrefix) {
        this.tablePrefix = tablePrefix;
    }

    public void setBufferCapacity(int bufferCapacity) {
        this.bufferCapacity = bufferCapacity;
    }
}
