package deltix.timebase.connector.util;

import deltix.qsrv.hf.pub.md.*;
import deltix.timebase.connector.model.schema.TimescaleColumn;
import org.springframework.jdbc.support.JdbcUtils;

import java.sql.Types;
import java.util.Collections;

public class ConnectorUtils {

    public static TimescaleColumn convert(DataField dataField, String parentFieldName, String descriptorName) {
        if (dataField == null) {
            throw new IllegalArgumentException("Timebase data field is not specified.");
        }

        DataType dataType = dataField.getType();
        String name = dataField.getName();
        String fieldName;
        if (parentFieldName != null) {
            fieldName = parentFieldName + "_" + name;
        } else {
            fieldName = name;
        }

        TimescaleColumn.TimescaleDataType timescaleDataType = getTimescaleDataType(dataType);
        boolean isArray = isArray(dataType);

        return TimescaleColumn.builder()
                .name(fieldName)
                .dataType(timescaleDataType)
                .relatedDescriptors(descriptorName == null ? Collections.emptyList() : Collections.singletonList(descriptorName))
                .isArray(isArray)
                .build();
    }

    public static int getSqlDataType(TimescaleColumn.TimescaleDataType timescaleDataType) {
        int sqlDataType;
        switch (timescaleDataType) {
            case INTEGER:
            case SERIAL:
                sqlDataType = Types.INTEGER;
                break;
            case CHAR:
                sqlDataType = Types.CHAR;
                break;
            case DATE:
                sqlDataType = Types.DATE;
                break;
            case LONG:
                sqlDataType = Types.BIGINT;
                break;
            case BOOLEAN:
                sqlDataType = Types.BOOLEAN;
                break;
            case BYTEA:
                sqlDataType = Types.BINARY;
                break;
            case DATETIME:
                sqlDataType = Types.TIMESTAMP;
                break;
            case JSON:
            case JSONB:
            case UUID:
                sqlDataType = Types.OTHER;
                break;
            case DECIMAL:
            case DECIMAL64:
                sqlDataType = Types.DECIMAL;
                break;
            case TIME:
                sqlDataType = Types.TIME;
                break;
            case VARCHAR:
                sqlDataType = Types.VARCHAR;
                break;
            default:
                throw new IllegalArgumentException("Unknown timescale data type: " + timescaleDataType);
        }

        return sqlDataType;
    }

    public static String getSqlDataTypeName(TimescaleColumn.TimescaleDataType timescaleDataType) {
        String sqlDataType;
        switch (timescaleDataType) {
            case INTEGER:
                sqlDataType = JdbcUtils.resolveTypeName(Types.INTEGER);
                break;
            case CHAR:
                sqlDataType = JdbcUtils.resolveTypeName(Types.CHAR);
                break;
            case DATE:
                sqlDataType = JdbcUtils.resolveTypeName(Types.DATE);
                break;
            case LONG:
                sqlDataType = JdbcUtils.resolveTypeName(Types.BIGINT);
                break;
            case BOOLEAN:
                sqlDataType = JdbcUtils.resolveTypeName(Types.BOOLEAN);
                break;
            case BYTEA:
                sqlDataType = "BYTEA";
                break;
            case DATETIME:
                sqlDataType = JdbcUtils.resolveTypeName(Types.TIMESTAMP);
                break;
            case JSON:
                sqlDataType = "JSON";
                break;
            case JSONB:
                sqlDataType = "JSONB";
                break;
            case UUID:
                sqlDataType = "UUID";
                break;
            case DECIMAL:
                sqlDataType = JdbcUtils.resolveTypeName(Types.DECIMAL);
                break;
            case DECIMAL64:
                sqlDataType = "DECIMAL(36, 18)";
                break;
            case TIME:
                sqlDataType = JdbcUtils.resolveTypeName(Types.TIME);
                break;
            case VARCHAR:
                sqlDataType = JdbcUtils.resolveTypeName(Types.VARCHAR);
                break;
            case SERIAL:
                sqlDataType = "SERIAL";
                break;
            default:
                throw new IllegalArgumentException("Unknown timescale data type: " + timescaleDataType);
        }

        return sqlDataType;
    }

    public static String getShortDescriptorName(String name) {
        String[] splittedName = name.split("\\.");
        return splittedName[splittedName.length - 1];
    }

    private static TimescaleColumn.TimescaleDataType getTimescaleDataType(DataType dataType) {
        TimescaleColumn.TimescaleDataType timescaleDataType;
        if (dataType instanceof IntegerDataType) {
            IntegerDataType integerDataType = (IntegerDataType) dataType;
            if (integerDataType.getSize() <= 4) {
                timescaleDataType = TimescaleColumn.TimescaleDataType.INTEGER;
            } else {
                timescaleDataType = TimescaleColumn.TimescaleDataType.LONG;
            }
        } else if (dataType instanceof EnumDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.VARCHAR;
        } else if (dataType instanceof BinaryDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.BYTEA;
        } else if (dataType instanceof BooleanDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.BOOLEAN;
        } else if (dataType instanceof ClassDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.JSON;
        } else if (dataType instanceof CharDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.CHAR;
        } else if (dataType instanceof FloatDataType) {
            FloatDataType floatDataType = (FloatDataType) dataType;
            if (floatDataType.isDecimal64()) {
                timescaleDataType = TimescaleColumn.TimescaleDataType.DECIMAL64;
            } else {
                timescaleDataType = TimescaleColumn.TimescaleDataType.DECIMAL;
            }
        } else if (dataType instanceof ArrayDataType) {
            ArrayDataType arrayDataType = (ArrayDataType) dataType;
            DataType elementDataType = arrayDataType.getElementDataType();
            timescaleDataType = getTimescaleDataType(elementDataType);
        } else if (dataType instanceof VarcharDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.VARCHAR;
        } else if (dataType instanceof DateTimeDataType) {
            timescaleDataType = TimescaleColumn.TimescaleDataType.DATETIME;
        } else {
            timescaleDataType = TimescaleColumn.TimescaleDataType.TIME;
        }

        return timescaleDataType;
    }

    private static boolean isArray(DataType dataType) {
        return dataType instanceof ArrayDataType;
    }
}
