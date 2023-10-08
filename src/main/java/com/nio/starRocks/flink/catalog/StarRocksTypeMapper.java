package com.nio.starRocks.flink.catalog;

/**
 * 功能：
 * 作者：zhl
 * 日期:2023/9/21 16:01
 **/

import com.nio.starRocks.flink.catalog.starRocks.StarRocksType;
import org.apache.flink.table.api.DataTypes;
import org.apache.flink.table.types.DataType;
import static com.nio.starRocks.flink.catalog.starRocks.StarRocksType.VARCHAR;
import static com.nio.starRocks.flink.catalog.starRocks.StarRocksType.*;//

import org.apache.flink.table.types.logical.*;
import org.apache.flink.table.types.logical.utils.LogicalTypeDefaultVisitor;


public class StarRocksTypeMapper {

//    public static DataType toFlinkType(String columnName, String columnType, int precision, int scale) {
//        columnType = columnType.toUpperCase();
//        switch (columnType) {
//            case BOOLEAN:
//                return DataTypes.BOOLEAN();
//            case TINYINT:
//                if (precision == 0) {
//                    //The boolean type will become tinyint when queried in information_schema, and precision=0
//                    return DataTypes.BOOLEAN();
//                } else {
//                    return DataTypes.TINYINT();
//                }
//            case SMALLINT:
//                return DataTypes.SMALLINT();
//            case INT:
//                return DataTypes.INT();
//            case BIGINT:
//                return DataTypes.BIGINT();
//            case DECIMAL:
//            case DECIMAL_V3:
//                return DataTypes.DECIMAL(precision, scale);
//            case FLOAT:
//                return DataTypes.FLOAT();
//            case DOUBLE:
//                return DataTypes.DOUBLE();
//            case CHAR:
//                return DataTypes.CHAR(precision);
//            case VARCHAR:
//                return DataTypes.VARCHAR(precision);
//            case LARGEINT:
//            case STRING:
//            case JSONB:
//                return DataTypes.STRING();
//            case DATE:
//            case DATE_V2:
//                return DataTypes.DATE();
//            case DATETIME:
//            case DATETIME_V2:
//                return DataTypes.TIMESTAMP(0);
//            default:
//                throw new UnsupportedOperationException(
//                        String.format(
//                                "Doesn't support  type '%s' on column '%s'", columnType, columnName));
//        }
//    }

    public static DataType toFlinkType(String columnName, String columnType, int precision, int scale) {
        columnType = columnType.toUpperCase();
        switch (columnType) {
            case BOOLEAN:
                return DataTypes.BOOLEAN();
            case TINYINT:
                if (precision == 0) {
                    //The boolean type will become tinyint when queried in information_schema, and precision=0
                    return DataTypes.BOOLEAN();
                } else {
                    return DataTypes.TINYINT();
                }
            case SMALLINT:
                return DataTypes.SMALLINT();
            case INT:
                return DataTypes.INT();
            case BIGINT:
                return DataTypes.BIGINT();
            case DECIMAL:
                return DataTypes.DECIMAL(precision, scale);
            case FLOAT:
                return DataTypes.FLOAT();
            case DOUBLE:
                return DataTypes.DOUBLE();
            case CHAR:
                return DataTypes.CHAR(precision);
            case VARCHAR:
                return DataTypes.VARCHAR(precision);
//            case LARGEINT:
            case STRING:
            case JSONB:
                return DataTypes.STRING();
            case DATE:
                return DataTypes.DATE();
            case DATETIME:
                return DataTypes.TIMESTAMP(0);
            default:
                throw new UnsupportedOperationException(
                        String.format(
                                "Doesn't support  type '%s' on column '%s'", columnType, columnName));
        }
    }

    public static String toStarRocksType(DataType flinkType){
        LogicalType logicalType = flinkType.getLogicalType();
        return logicalType.accept(new LogicalTypeVisitor(logicalType));
    }

    private static class LogicalTypeVisitor extends LogicalTypeDefaultVisitor<String> {
        private final LogicalType type;

        LogicalTypeVisitor(LogicalType type) {
            this.type = type;
        }

        @Override
        public String visit(CharType charType) {
            return String.format("%s(%s)", StarRocksType.CHAR, charType.getLength());
        }

        @Override
        public String visit(VarCharType varCharType) {
            int length = varCharType.getLength();
            return length > 65533 ? STRING : String.format("%s(%s)", VARCHAR, length);
        }

        @Override
        public String visit(BooleanType booleanType) {
            return BOOLEAN;
        }

        @Override
        public String visit(VarBinaryType varBinaryType) {
            return STRING;
        }

//        @Override
//        public String  visit(DecimalType decimalType) {
//            int precision = decimalType.getPrecision();
//            int scale = decimalType.getScale();
//            return precision <= 38
//                    ? String.format("%s(%s,%s)", StarRocksType.DECIMAL_V3, precision, scale >= 0 ? scale : 0)
//                    : StarRocksType.STRING;
//        }
        @Override
        public String  visit(DecimalType decimalType) {
            int precision = decimalType.getPrecision();
            int scale = decimalType.getScale();
            return precision <= 38
                    ? String.format("%s(%s,%s)", StarRocksType.DECIMAL, precision, scale >= 0 ? scale : 0)
                    : StarRocksType.STRING;
        }

        @Override
        public String visit(TinyIntType tinyIntType) {
            return TINYINT;
        }

        @Override
        public String visit(SmallIntType smallIntType) {
            return SMALLINT;
        }

        @Override
        public String visit(IntType intType) {
            return INT;
        }

        @Override
        public String visit(BigIntType bigIntType) {
            return BIGINT;
        }

        @Override
        public String visit(FloatType floatType) {
            return FLOAT;
        }

        @Override
        public String visit(DoubleType doubleType) {
            return DOUBLE;
        }

//        @Override
//        public String visit(DateType dateType) {
//            return DATE_V2;
//        }
        @Override
        public String visit(DateType dateType) {
            return DATE;
        }

//        @Override
//        public String visit(TimestampType timestampType) {
//            int precision = timestampType.getPrecision();
//            return String.format("%s(%s)", StarRocksType.DATETIME_V2, Math.min(Math.max(precision, 0), 6));
//        }

        @Override
        public String visit(TimestampType timestampType) {
            return DATETIME;
        }

        @Override
        public String visit(ArrayType arrayType) {
            return STRING;
        }

        @Override
        public String visit(MapType mapType) {
            return STRING;
        }

        @Override
        public String visit(RowType rowType) {
            return STRING;
        }

        @Override
        protected String defaultMethod(LogicalType logicalType) {
            throw new UnsupportedOperationException(
                    String.format(
                            "Flink doesn't support converting type %s to StarRocks type yet.",
                            type.toString()));
        }
    }
}
