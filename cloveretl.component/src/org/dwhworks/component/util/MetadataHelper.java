package org.dwhworks.component.util;

import org.apache.log4j.Logger;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.primitive.Decimal;
import org.jetel.metadata.DataFieldMetadata;
import org.jetel.metadata.DataFieldType;
import org.jetel.metadata.DataRecordMetadata;

import java.text.DecimalFormat;
import java.text.Format;
import java.text.SimpleDateFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;

/**
 * Helper for processing CloverETL graph's metadata.
 *
 * @author Nikita Skotnikov
 * @since 27.03.2018
 */
public class MetadataHelper {

  private static MetadataHelper instance;

  /**
   * Singleton
   *
   * @return instance of MetadataHelper
   */
  public static synchronized MetadataHelper getInstance() {
    if (instance == null) instance = new MetadataHelper();
    return instance;
  }

  private MetadataHelper() {

  }

  private Map<String, Format> fieldsFormats;

  /**
   * @param metadata a metadata for exploring
   * @param log      logger
   */
  public void checkFieldsFormats(DataRecordMetadata metadata, Logger log) {
    fieldsFormats = new HashMap<>();
    DataFieldMetadata[] fieldsMetadata = metadata.getFields();

    for (DataFieldMetadata fieldMetadata : fieldsMetadata) {
      String fieldName = fieldMetadata.getName();
      DataFieldType fieldType = fieldMetadata.getDataType();
      String fieldFormat = fieldMetadata.getFormat();
      boolean hasFieldFormat = fieldFormat != null && !fieldFormat.isEmpty();

      if (isDate(fieldType)) {
        if (hasFieldFormat) fieldsFormats.put(fieldName, new SimpleDateFormat(fieldFormat));
        else {
          fieldsFormats.put(fieldName, new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));

          if (log != null)
            log.warn("No format for field " + fieldName + ":" + fieldType
                + ". Default format will be used 'yyyy-MM-dd HH:mm:ss'.");
        }
      } else if (isDecimal(fieldType)) {
        DecimalFormat df = (DecimalFormat) DecimalFormat.getNumberInstance(Locale.ENGLISH);

        if (hasFieldFormat) df.applyPattern(fieldFormat);
        else {
          StringBuilder sb = new StringBuilder(32);
          sb.append("#################0.");
          int scale = Integer.valueOf(fieldMetadata.getProperty("scale"));
          for (int i = 0; i < scale; i++) sb.append('0');

          df.applyPattern(sb.toString());

          if (log != null)
            log.warn("No format for field " + fieldName + ":" + fieldType
                + ". Format will be used '" + sb + "'.");
        }

        fieldsFormats.put(fieldName, df);
      } else if (isInteger(fieldType)) {
        if (hasFieldFormat) fieldsFormats.put(fieldName, new DecimalFormat(fieldFormat));
        else {
          fieldsFormats.put(fieldName, new DecimalFormat("#################0"));

          if (log != null)
            log.warn("No format for field " + fieldName + ":" + fieldType
                + ". Default format will be used '#################0'.");
        }
      }
    }
  }

  /**
   * @param metadata a metadata for exploring
   * @return an string array of field names in specified metadata
   */
  public String[] getFieldNames(DataRecordMetadata metadata) {
    if (metadata == null) return null;
    return metadata.getFieldNamesArray();
  }

  /**
   * @param metadata   a metadata for exploring
   * @param dataRecord a record that contains data
   * @param fields     list of fields to format
   * @param log        logger
   * @return formatted field values that specified in fields list
   */
  public Map<String, String> getFormattedFieldValues(DataRecordMetadata metadata, DataRecord dataRecord,
                                                     List<String> fields, Logger log) {
    if (metadata == null || dataRecord == null) return null;

    Map<String, String> fieldValues = new HashMap<>();

    for (String fieldName : fields) {
      DataFieldMetadata fieldMetadata = metadata.getField(fieldName);
      if (fieldMetadata == null) {
        if (log != null) log.error("Metadata for field \"" + fieldName + "\" not found");
        continue;
      }

      DataField field = dataRecord.getField(fieldName);
      Object fieldValue = field.getValue();

      if (fieldValue == null) {
        fieldValues.put(fieldName, "");
        continue;
      }

      final String formattedValue = formatField(fieldMetadata, fieldValue, log);
      fieldValues.put(fieldName, formattedValue);
    }

    return fieldValues;
  }

  private String formatField(DataFieldMetadata fieldMetadata, Object fieldValue,
                             Logger log) throws IllegalArgumentException {
    Format fieldFormat = fieldsFormats.get(fieldMetadata.getName());

    if (fieldFormat != null)
      try {
        DataFieldType fieldType = fieldMetadata.getDataType();

        if (isDate(fieldType) || isInteger(fieldType))
          return fieldFormat.format(fieldValue);

        else if (isDecimal(fieldType))
          return fieldFormat.format(((Decimal) fieldValue).getDouble());

        else {
          if (log != null)
            log.warn("Unsupported field type \"" + fieldType.getName()
                + "\". Supported field types for formatting: date, integer, decimal");
        }
      } catch (IllegalArgumentException e) {
        if (log != null)
          log.error("Format problems on field \"" + fieldMetadata.getName()
              + "\" [value=" + fieldValue + ";format=" + fieldFormat + ']');
        throw e;
      }

    return fieldValue == null ? "" : String.valueOf(fieldValue);
  }

  /**
   * @param metadata   a metadata for exploring
   * @param dataRecord a record that contains data
   * @param log        logger
   * @return field values of specified data record
   */
  public Map<String, Object> getFieldValues(DataRecordMetadata metadata, DataRecord dataRecord, Logger log) {
    if (metadata == null || dataRecord == null) return null;

    String[] fieldNames = metadata.getFieldNamesArray();
    Map<String, Object> fieldValues = new HashMap<>();

    for (String fieldName : fieldNames) {
      DataFieldMetadata fieldMetadata = metadata.getField(fieldName);
      if (fieldMetadata == null) {
        if (log != null) log.error("Metadata for field \"" + fieldName + "\" not found");
        continue;
      }

      DataField field = dataRecord.getField(fieldName);
      fieldValues.put(fieldName, field.getValue());
    }

    return fieldValues;
  }

  /**
   * @param metadata  a metadata for exploring
   * @param fieldName field name
   * @return type of specified field
   */
  public DataFieldType getFieldType(DataRecordMetadata metadata, String fieldName) {
    if (metadata == null) return null;
    DataFieldMetadata field = metadata.getField(fieldName);
    if (field == null) return null;
    return field.getDataType();
  }

  /**
   * @param metadata  a metadata for exploring
   * @param fieldName field name
   * @return true if field is exist in specified metadata
   */
  public boolean isFieldExist(DataRecordMetadata metadata, String fieldName) {
    try {
      return metadata.getFieldPosition(fieldName) >= 0;
    } catch (Exception e) {
      return false;
    }
  }

  /**
   * @param type field type
   * @return true if specified field type is DATE
   */
  public boolean isDate(DataFieldType type) {
    return DataFieldType.DATE.equals(type);
  }

  /**
   * @param type field type
   * @return true if specified field type is STRING
   */
  public boolean isString(DataFieldType type) {
    return DataFieldType.STRING.equals(type);
  }

  /**
   * @param type field type
   * @return true if specified field type is INTEGER
   */
  public boolean isInteger(DataFieldType type) {
    return DataFieldType.INTEGER.equals(type);
  }

  /**
   * @param type field type
   * @return true if specified field type is LONG
   */
  public boolean isLong(DataFieldType type) {
    return DataFieldType.LONG.equals(type);
  }

  /**
   * @param type field type
   * @return true if specified field type is DECIMAL
   */
  public boolean isDecimal(DataFieldType type) {
    return DataFieldType.DECIMAL.equals(type);
  }


}
