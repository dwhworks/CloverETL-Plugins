package org.dwhworks.component.util;

import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

/**
 * Helper for processing database connection.
 *
 * @author Nikita Skotnikov
 * @since 02.04.2018
 */
public class JDBCHelper {

  private DatabaseMetaData metadata;

  /**
   * Constructor.
   *
   * @param metadata database info
   */
  public JDBCHelper(DatabaseMetaData metadata) {
    this.metadata = metadata;
  }

  public void setMetadata(DatabaseMetaData metadata) {
    this.metadata = metadata;
  }

  private final Map<String, Map<String, Integer[]>> tableFields = new HashMap<>();

  /**
   * @param schema    schema name
   * @param tableName table name
   * @return <code>true</code> if specified table is exist in specified schema
   * @throws SQLException if errors
   */
  public boolean isTableExist(String schema, String tableName) throws SQLException {
    if (tableFields.get(schema + '.' + tableName) != null) return true;

    try (ResultSet rs = metadata.getTables(null, schema, tableName, null)) {
      if (rs.next()) {
        tableFields.put(schema + '.' + tableName, new HashMap<>());
        return true;
      }
    }

    return false;
  }

  /**
   * @param schema    schema name
   * @param tableName table name
   * @return specified table's fields (name, type and size)
   * @throws SQLException if errors
   */
  public Map<String, Integer[]> getTableFields(String schema, String tableName) throws SQLException {
    if (!isTableExist(schema, tableName)) return null;

    return tableFields.get(schema + '.' + tableName);
  }

  /**
   * @param schema schema name
   * @param table  table name
   * @param field  field name
   * @return <code>true</code> is field exist in specified table
   * @throws SQLException if errors
   */
  public boolean hasField(String schema, String table, String field) throws SQLException {
    if (!isTableExist(schema, table)) return false;

    Map<String, Integer[]> fields = tableFields.get(schema + '.' + table);
    if (fields.size() == 0) {
      ResultSet rs = metadata.getColumns(null, schema, table, null);
      while (rs.next())
        fields.put(rs.getString("COLUMN_NAME"), new Integer[]{rs.getInt("DATA_TYPE"),
            rs.getInt("COLUMN_SIZE")});
    }

    return fields.get(field) != null;
  }

  /**
   * Commit connection.
   *
   * @param connection connection
   * @throws SQLException if errors
   */
  public void commit(Connection connection) throws SQLException {
    if (connection != null && !connection.getAutoCommit())
      connection.commit();
  }

  /**
   * Rollback connection.
   *
   * @param connection connection
   * @throws SQLException if errors
   */
  public void rollback(Connection connection) throws SQLException {
    if (connection != null) connection.rollback();
  }

  /**
   * Close connection.
   *
   * @param connection connection
   * @throws SQLException if errors
   */
  public void close(Connection connection) throws SQLException {
    if (connection != null) connection.close();
  }


}
