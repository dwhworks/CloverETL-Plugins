package org.dwhworks.component;

import org.dwhworks.component.util.MetadataHelper;
import org.dwhworks.component.util.Utils;
import org.apache.log4j.Logger;
import org.jetel.data.DataField;
import org.jetel.data.DataRecord;
import org.jetel.data.DataRecordFactory;
import org.jetel.exception.ComponentNotReadyException;
import org.jetel.exception.ConfigurationStatus;
import org.jetel.exception.XMLConfigurationException;
import org.jetel.graph.Node;
import org.jetel.graph.Result;
import org.jetel.graph.TransformationGraph;
import org.jetel.metadata.DataRecordMetadata;
import org.jetel.util.SynchronizeUtils;
import org.jetel.util.property.ComponentXMLAttributes;
import org.jetel.util.property.RefResFlag;
import org.w3c.dom.Element;

import java.util.*;

/**
 * <h3>Hash Calculation Component</h3>
 *
 * <!-- Takes any record as input, calculates `KEY_HASH` and `MEASURE_HASH` and outputs the original record with two additional fields as strings.-->
 *
 * <table border="1">
 * <th>Component:</th>
 * <tr><td><h4><i>Name:</i></h4></td>
 * <td>Hash Calculation</td></tr>
 * <tr><td><h4><i>Category:</i></h4></td>
 * <td>Custom</td></tr>
 * <tr><td><h4><i>Description:</i></h4></td>
 * <td>Receives data through connected input port and calculates
          two 'hash' values for a record: the 'KEY_HASH' and 'MEASURE_HASH'.
          KEY_HASH is commonly used as a records's primary key if the natural key contains more than one column.

          MEASURE_HASH is essentially a checksum of all non-key values for the record.
          It is used for determining later if the record was really changed without comparing
          all field values, thus increasing the speed of such comparison and saving on manual labour.
          A hash may be a plain concatenation of field values, or an MD5 sum of such concatenation.</td></tr>
 * </table>
 * <br>
 * <table border="1">
 * <th>XML attributes:</th>
 * <tr><td><b>id</b></td><td>component identification</td>
 * <tr><td><b>type</b></td><td>"HASH_CALC"</td></tr>
 * <tr><td><b>keyHashFields</b></td><td>Fields used for KEY_HASH calculation, separated by semicolon.</td></tr>
 * 
 * <tr>
 *   <td><b>measureHashFields</b></td>
 *   <td>Fields used for MEASURE_HASH calculation, separated by semicolon. If this attribute is not specified,
 *   all fields, not included into keyHashFields and ignoreFields will be used.
 *   In this case the sequence of values will be the same as the sequence of fields declared in record metadata.
 *   This default behaviour may be handy if we want the same graph to process records with
 *   different structure.
 *   </td>
 * </tr>
 * 
 * <tr><td><b>ignoreFields</b></td><td>Fields to be ignored in hash calculations.</td></tr>
 * <tr><td><b>hashFunction</b></td><td>'md5' or 'raw' (by default md5 will be used). Raw means all field values will be concatenated using '-' (hyphen) as a separator and returned without actual hashing</td></tr>
 * <tr><td><b>keyHashFieldName</b></td><td>Field name to be used for storing KEY_HASH</td></tr>
 * <tr><td><b>measureHashFieldName</b></td><td>Field name to be used for storing MEASURE_HASH</td></tr>
 * <tr><td><b>printDebugInfo</b></td><td>Print debug info on DEBUG logging level. Prints hash values for each record</td></tr>
 * </table>
 *
 * <h4>Example:</h4>
 * <pre>&lt;Node id="HASH_CALCULATION" type="HASH_CALC" keyHashFields="mfr_name;mfr_inn;mfr_kpp" measureHashFieldsFields="mfr_address" hashFunction="md5" keyHashFieldName="key_hash" measureHashFieldName="measure_hash"/&gt;</pre>
 *
 * <p>Output record must contain two fields for key_hash and measure_hash values. The names of these fields
 * are specified in keyHashFieldName and measureHashFieldName attributes.
 * <code>md5</code> hash is returned as a 32-character string in lowercase.
 * <code>raw</code> hash is a string, containing of all field values concatenated together using hyphen "-" as a separator.
 * When md5 is specified as hashFunction, first the raw hash is calculated and then the resulting value
 * is md5-hashed.
 *
 * NULLs are interpreted as empty strings.
 * Date and time are converted to string according to format specified in incoming metadata.
 * Numeric values are converted to string according to format specified in incoming metadata.
 * </p>
 *
 * @author Nikita Skotnikov
 * @since 23.03.2018
 */
public class HashCalc extends AbstractComponent {
  public final static String COMPONENT_TYPE = "HASH_CALC";

  private static final String XML_KEY_HASH_FIELDS_ATTRIBUTE = "keyHashFields";
  private static final String XML_MEASURE_HASH_FIELDS_ATTRIBUTE = "measureHashFields";
  private static final String XML_IGNORE_FIELDS_ATTRIBUTE = "ignoreFields";
  private static final String XML_HASH_FUNCTION_ATTRIBUTE = "hashFunction";
  private static final String XML_KEY_HASH_FIELD_NAME_ATTRIBUTE = "keyHashFieldName";
  private static final String XML_MEASURE_HASH_FIELD_NAME_ATTRIBUTE = "measureHashFieldName";
  private static final String XML_PRINT_DEBUG_INFO_ATTRIBUTE = "printDebugInfo";

  private static final String HASH_FUNCTION_MD5 = "md5";
  private static final String HASH_FUNCTION_RAW = "raw";
  private static final String DEFAULT_HASH_FUNCTION = HASH_FUNCTION_MD5;

  private static final String ATTR_VALUES_DELIMITER = ";";
  private static final String RAW_VALUES_DELIMITER = "-";

  private static final int READ_FROM_PORT = 0;
  private static final int WRITE_TO_PORT = 0;

  private static final Logger LOG = Logger.getLogger(HashCalc.class);

  private String attrKeyHashFields, attrMeasureHashFields, attrIgnoreFields, attrHashFunction, attrKeyHashFieldName,
      attrMeasureHashFieldName;
  private boolean attrPrintDebugInfo;

  /**
   * Constructor
   *
   * @param id                   component id in the graph
   * @param keyHashFields        list of key hash fields separated by semicolon
   * @param measureHashFields    list of measure hash fields separated by semicolon
   * @param ignoreFields         list of fields to ignore when calculating hashes
   * @param hashFunction         md5 or raw (by default md5 will be used)
   * @param keyHashFieldName     field name for key hash
   * @param measureHashFieldName field name for measure hash
   */
  public HashCalc(String id, String keyHashFields, String measureHashFields, String ignoreFields,
                  String hashFunction, String keyHashFieldName, String measureHashFieldName,
                  boolean printDebugInfo) {
    super(id);

    attrKeyHashFields = keyHashFields;
    attrMeasureHashFields = measureHashFields;
    attrIgnoreFields = ignoreFields;
    attrHashFunction = hashFunction;
    attrKeyHashFieldName = keyHashFieldName;
    attrMeasureHashFieldName = measureHashFieldName;
    attrPrintDebugInfo = printDebugInfo;
  }

  public HashCalc(String id, String keyHashFields, String measureHashFields,
                  String ignoreFields, String keyHashFieldName, String measureHashFieldName, boolean printDebugInfo) {
    this(id, keyHashFields, measureHashFields, ignoreFields,
        DEFAULT_HASH_FUNCTION, keyHashFieldName, measureHashFieldName, printDebugInfo);
  }

  @Override
  public String getType() {
    return COMPONENT_TYPE;
  }

  @Override
  public ConfigurationStatus checkConfig(ConfigurationStatus status) {
    super.checkConfig(status);

    if (getInPorts().size() < 1 || getOutPorts().size() < 1) {
      status.addError(this, null, "Both input and output port must be connected!");
      return status;
    }

    DataRecordMetadata inMetadata = getInputPort(0).getMetadata(),
        outMetadata = getOutputPort(0).getMetadata();
    if (inMetadata == null || outMetadata == null)
      status.addError(this, null, "Metadata on input or output port not specified!");

    return status;
  }

  private MetadataHelper metadataHelper;
  private DataRecordMetadata inMetadata, outMetadata;
  private List<String> keyHashFields, measureHashFields, ignoreFields;

  @Override
  public void init() throws ComponentNotReadyException {
    super.init();
    metadataHelper = MetadataHelper.getInstance();
    inMetadata = getInputPort(READ_FROM_PORT).getMetadata();
    outMetadata = getOutputPort(WRITE_TO_PORT).getMetadata();

    keyHashFields = Utils.toLinkedList(attrKeyHashFields.split(ATTR_VALUES_DELIMITER));
    measureHashFields = prepareMeasureFields();
    checkMetadataIn();
    checkMetadataOut();
  }

  @Override
  protected void checkGraphParameters() {
    //GraphParameters parameters = getGraph().getGraphParameters();
  }

  @Override
  protected void checkAttributes() throws ComponentNotReadyException {
    if (attrKeyHashFields == null || attrKeyHashFields.isEmpty())
      throw new ComponentNotReadyException(COMPONENT_TYPE + ':'
          + XML_KEY_HASH_FIELDS_ATTRIBUTE + "\" attribute is not specified");

    if (attrKeyHashFieldName == null || attrKeyHashFieldName.isEmpty())
      throw new ComponentNotReadyException(COMPONENT_TYPE + ':'
          + XML_KEY_HASH_FIELD_NAME_ATTRIBUTE + "\" attribute is not specified");

    if (attrMeasureHashFieldName == null || attrMeasureHashFieldName.isEmpty())
      throw new ComponentNotReadyException(COMPONENT_TYPE + ':'
          + XML_MEASURE_HASH_FIELD_NAME_ATTRIBUTE + "\" attribute is not specified");

    boolean hasMeasure = attrMeasureHashFields != null && !attrMeasureHashFields.isEmpty(),
        hasIgnored = attrIgnoreFields != null && !attrIgnoreFields.isEmpty();

    if (hasMeasure && hasIgnored)
      throw new ComponentNotReadyException(COMPONENT_TYPE + ": You should specify only one attribute between \""
          + XML_MEASURE_HASH_FIELDS_ATTRIBUTE + "\" and \"" + XML_IGNORE_FIELDS_ATTRIBUTE + "\".");

    if (attrIgnoreFields != null && !attrIgnoreFields.isEmpty()) {
      List<String> ignoreFields = Utils.toLinkedList(attrIgnoreFields.split(ATTR_VALUES_DELIMITER)),
          keyHashFields = Utils.toLinkedList(attrKeyHashFields.split(ATTR_VALUES_DELIMITER));

      for (String ignoreField : ignoreFields)
        if (keyHashFields.contains(ignoreField))
          throw new ComponentNotReadyException(COMPONENT_TYPE + ": Check 'keyHashFields' property value. "
              + "There are some ignore fields."
              + "\nignoreFields=" + Arrays.toString(ignoreFields.toArray())
              + "\nkeyHashFields=" + Arrays.toString(keyHashFields.toArray()));
    }

    if (!HASH_FUNCTION_MD5.equals(attrHashFunction) && !HASH_FUNCTION_RAW.equals(attrHashFunction))
      throw new ComponentNotReadyException(COMPONENT_TYPE + ": Invalid \"" + XML_HASH_FUNCTION_ATTRIBUTE
          + "\" property value \"" + attrHashFunction + "\". Supported values: "
          + HASH_FUNCTION_MD5 + ", " + HASH_FUNCTION_RAW);
  }

  private List<String> prepareMeasureFields() {
    if (attrMeasureHashFields == null || attrMeasureHashFields.isEmpty()) {
      List<String> measureHashFields = new LinkedList<>(
          Utils.toLinkedList(metadataHelper.getFieldNames(inMetadata)));
      measureHashFields.removeAll(keyHashFields);

      if (attrIgnoreFields != null && !attrIgnoreFields.isEmpty()) {
        ignoreFields = Utils.toLinkedList(attrIgnoreFields.split(ATTR_VALUES_DELIMITER));
        measureHashFields.removeAll(ignoreFields);
      }

      if (measureHashFields.isEmpty())
        throw new IllegalStateException(COMPONENT_TYPE + ": No measure hash fields "
            + "to processing after removing key and ignore fields.");

      return measureHashFields;

    }
    return Utils.toLinkedList(attrMeasureHashFields.split(ATTR_VALUES_DELIMITER));
  }

  private void checkMetadataIn() {
    Map<String, List<String>> unknownFields = new HashMap<>();
    checkInFields(unknownFields, keyHashFields, XML_KEY_HASH_FIELDS_ATTRIBUTE);
    checkInFields(unknownFields, measureHashFields, XML_MEASURE_HASH_FIELDS_ATTRIBUTE);
    checkInFields(unknownFields, ignoreFields, XML_IGNORE_FIELDS_ATTRIBUTE);

    if (!unknownFields.isEmpty()) {
      StringBuilder msg = new StringBuilder(128);
      for (String attr : unknownFields.keySet())
        msg.append(unknownFields.size() > 1 ? "  \n" : "").append("Attribute ").append(attr)
            .append(": fields does not exist ").append(Arrays.toString(unknownFields.get(attr).toArray()));

      throw new IllegalArgumentException(COMPONENT_TYPE + ": " + msg);
    }

    metadataHelper.checkFieldsFormats(inMetadata, LOG);
  }

  private void checkInFields(Map<String, List<String>> unknownFields, List<String> fields, String key) {
    if (fields == null || fields.isEmpty()) return;

    List<String> unknownFieldsList = new ArrayList<>();
    for (String fieldName : fields)
      if (!metadataHelper.isFieldExist(inMetadata, fieldName))
        unknownFieldsList.add(fieldName);

    if (!unknownFieldsList.isEmpty()) unknownFields.put(key, unknownFieldsList);
  }

  private void checkMetadataOut() {
    if (!metadataHelper.isFieldExist(outMetadata, attrKeyHashFieldName))
      throw new IllegalArgumentException(COMPONENT_TYPE + ": Attribute " + XML_KEY_HASH_FIELD_NAME_ATTRIBUTE
          + ": field " + attrKeyHashFieldName + " does not exist");

    if (!metadataHelper.isFieldExist(outMetadata, attrMeasureHashFieldName))
      throw new IllegalArgumentException(COMPONENT_TYPE + ": Attribute " + XML_MEASURE_HASH_FIELD_NAME_ATTRIBUTE
          + ": field " + attrMeasureHashFieldName + " does not exist");
  }

  @Override
  protected Result execute() throws Exception {
    DataRecord inRecord = DataRecordFactory.newRecord(inMetadata),
        outRecord = DataRecordFactory.newRecord(outMetadata);

    while ((inRecord = readRecord(READ_FROM_PORT, inRecord)) != null && runIt) {
      fillOutRecordByInRecord(inRecord, outRecord);
      String keyRaw = getRawValues(inRecord, keyHashFields), measureRaw = getRawValues(inRecord, measureHashFields);
      assert (!keyRaw.isEmpty() || !measureRaw.isEmpty());

      DataField keyHashField = outRecord.getField(attrKeyHashFieldName),
          measureHashField = outRecord.getField(attrMeasureHashFieldName);

      if (HASH_FUNCTION_MD5.equals(attrHashFunction)) {
        String keyHash = Utils.md5(keyRaw), measureHash = Utils.md5(measureRaw);
        assert (!keyHash.isEmpty() || !measureHash.isEmpty());
        if (attrPrintDebugInfo)
          LOG.debug("\n\nKey: " + keyRaw + "\nMD5: " + keyHash
            + "\n\nMeasure: " + measureRaw + "\nMD5: " + measureHash + "\n");

        keyHashField.setValue(keyHash);
        measureHashField.setValue(measureHash);
      } else if (HASH_FUNCTION_RAW.equals(attrHashFunction)) {
        if (attrPrintDebugInfo)
        LOG.debug("\n\nKey: " + keyRaw + "\nMeasure: " + measureRaw + "\n");

        keyHashField.setValue(keyRaw);
        measureHashField.setValue(measureRaw);
      } else throw new RuntimeException("Unknown hash function: " + attrHashFunction);

      writeRecord(WRITE_TO_PORT, outRecord);
      SynchronizeUtils.cloverYield();
    }

    return runIt ? Result.FINISHED_OK : Result.ABORTED;
  }

  private void fillOutRecordByInRecord(DataRecord inRecord, DataRecord outRecord) {
    Map<String, Object> fieldValues = metadataHelper.getFieldValues(inMetadata, inRecord, LOG);
    for (String fieldName : fieldValues.keySet()) {
      if (outRecord.hasField(fieldName)) {
        DataField field = outRecord.getField(fieldName);
        if (field != null) field.setValue(fieldValues.get(fieldName));
      }
    }
  }

  private String getRawValues(DataRecord inRecord, List<String> fields) {
    Map<String, String> fieldValues = metadataHelper.getFormattedFieldValues(inMetadata, inRecord, fields, LOG);
    StringBuilder sb = new StringBuilder(128);

    for (int i = 0; i < fields.size(); i++) {
      if (i != 0) sb.append(RAW_VALUES_DELIMITER);
      sb.append(fieldValues.get(fields.get(i)));
    }

    return sb.toString();
  }

  /**
   * Factory method that creates the component from transformation graph source XML
   */
  public static Node fromXML(TransformationGraph graph, Element xmlElement) throws XMLConfigurationException {
    ComponentXMLAttributes xmlAttrs = new ComponentXMLAttributes(xmlElement, graph);
    try {
      return new HashCalc(
          xmlAttrs.getString(XML_ID_ATTRIBUTE),
          // dklimov 2018-10-29 changed getString to getStringEx to parse params like ${TABLE_NAME} in attributes
          xmlAttrs.getStringEx(XML_KEY_HASH_FIELDS_ATTRIBUTE, "", RefResFlag.REGULAR),
          xmlAttrs.getStringEx(XML_MEASURE_HASH_FIELDS_ATTRIBUTE, "", RefResFlag.REGULAR),
          xmlAttrs.getStringEx(XML_IGNORE_FIELDS_ATTRIBUTE, "", RefResFlag.REGULAR),
          xmlAttrs.getString(XML_HASH_FUNCTION_ATTRIBUTE, DEFAULT_HASH_FUNCTION),
          xmlAttrs.getString(XML_KEY_HASH_FIELD_NAME_ATTRIBUTE),
          xmlAttrs.getString(XML_MEASURE_HASH_FIELD_NAME_ATTRIBUTE),
          xmlAttrs.getBoolean(XML_PRINT_DEBUG_INFO_ATTRIBUTE, false)
      );
    } catch (Exception e) {
      throw new XMLConfigurationException(COMPONENT_TYPE + ':'
          + xmlAttrs.getString(XML_ID_ATTRIBUTE, " unknown ID ") + ':' + e.getMessage(), e);
    }
  }


}
