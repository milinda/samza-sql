/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.samza.sql.schema;

import org.apache.avro.Schema;
import org.apache.avro.SchemaBuilder;
import org.apache.calcite.rel.type.RelDataType;
import org.apache.calcite.rel.type.RelDataTypeFactory;
import org.apache.calcite.rel.type.RelDataTypeField;
import org.apache.calcite.sql.type.ArraySqlType;
import org.apache.calcite.sql.type.SqlTypeName;
import org.apache.samza.SamzaException;

import java.util.ArrayList;
import java.util.List;

/**
 * Defines method which converts Avro {@link org.apache.avro.Schema} to {@link org.apache.calcite.rel.type.RelDataType}
 * and vice versa.
 * <p>Inspired by parquet-mr.</p>
 */
public class AvroSchemaUtils {
  public static final String SAMZA_GENERATED_AVRO_NS = "org.apache.samza.sql";

  /**
   * Converts Avro schema to RelDataType.
   *
   * @param relDataTypeFactory RelDataType factory instance
   * @param avroSchema         avro schema
   * @return RelDataType
   */
  public static RelDataType avroSchemaToRelDataType(RelDataTypeFactory relDataTypeFactory, Schema avroSchema) {
    if (avroSchema.getType() == Schema.Type.RECORD) {
      return convertRecord(avroSchema, relDataTypeFactory, true);
    }

    return convertFieldType(avroSchema, relDataTypeFactory);
  }

  private static RelDataType convertRecord(Schema recordSchema, RelDataTypeFactory relDataTypeFactory, boolean isRoot) {
    RelDataTypeFactory.FieldInfoBuilder builder = relDataTypeFactory.builder();

    for (Schema.Field field : recordSchema.getFields()) {
      Schema fieldSchema = field.schema();
      if (fieldSchema.getType() == Schema.Type.NULL) {
        continue; // From parquet-avro: Avro nulls are not encoded, unless they are null unions
      }

      convertField(builder, relDataTypeFactory, field.name(), fieldSchema);
    }

    RelDataType record = builder.build();
    if (isRoot) {
      // Record at root level is treated differently.
      return record;
    }

    return relDataTypeFactory.createStructType(record.getFieldList());
  }

  private static void convertField(RelDataTypeFactory.FieldInfoBuilder builder,
                                   RelDataTypeFactory relDataTypeFactory,
                                   String fieldName,
                                   Schema fieldSchema) {
    builder.add(fieldName, convertFieldType(fieldSchema, relDataTypeFactory));
  }

  private static RelDataType convertFieldType(Schema elementType, RelDataTypeFactory relDataTypeFactory) {
    Schema.Type type = elementType.getType();
    if (type == Schema.Type.STRING) {
      return relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR);
    } else if (type == Schema.Type.INT) {
      return relDataTypeFactory.createSqlType(SqlTypeName.INTEGER);
    } else if (type == Schema.Type.BOOLEAN) {
      return relDataTypeFactory.createSqlType(SqlTypeName.BOOLEAN);
    } else if (type == Schema.Type.BYTES) {
      return relDataTypeFactory.createSqlType(SqlTypeName.BINARY);
    } else if (type == Schema.Type.LONG) {
      return relDataTypeFactory.createSqlType(SqlTypeName.BIGINT);
    } else if (type == Schema.Type.DOUBLE) {
      return relDataTypeFactory.createSqlType(SqlTypeName.DOUBLE);
    } else if (type == Schema.Type.FLOAT) {
      return relDataTypeFactory.createSqlType(SqlTypeName.FLOAT);
    } else if (type == Schema.Type.ARRAY) {
      return relDataTypeFactory.createArrayType(convertFieldType(elementType.getElementType(), relDataTypeFactory), -1);
    } else if (type == Schema.Type.RECORD) {
      return convertRecord(elementType, relDataTypeFactory, false);
    } else if (type == Schema.Type.MAP) {
      return relDataTypeFactory.createMapType(relDataTypeFactory.createSqlType(SqlTypeName.VARCHAR),
          convertFieldType(elementType.getValueType(), relDataTypeFactory));
    } else if (type == Schema.Type.FIXED) {
      // Remove the support for Fixed types as per the comments in https://reviews.apache.org/r/33280/
      throw new IllegalArgumentException("Unsupported type: " + type);
    } else if (type == Schema.Type.UNION) {
      List<Schema> types = elementType.getTypes();
      List<Schema> nonNullTypes = new ArrayList<Schema>();
      boolean foundNull = false;

      for (Schema s : types) {
        if (s.getType() == Schema.Type.NULL) {
          foundNull = true;
        } else {
          nonNullTypes.add(s);
        }
      }

      if (nonNullTypes.size() > 1) {
        throw new RuntimeException("Multiple non null types in a union is not supported.");
      } else {
        return relDataTypeFactory.createTypeWithNullability(convertFieldType(nonNullTypes.get(0), relDataTypeFactory), foundNull);
      }
    } else if (type == Schema.Type.ENUM) {
      // Remove the support for enums as per the comments in https://reviews.apache.org/r/33280/
      throw new IllegalArgumentException("Unsupported type " + type);
    }

    return relDataTypeFactory.createSqlType(SqlTypeName.ANY);
  }

  /**
   * Converts RelDataType to Avro Schema
   *
   * @param relDataType RelDataType to convert
   * @return Avro Schema
   */
  public static Schema relDataTypeToAvroSchema(RelDataType relDataType) {
    if (relDataType.isStruct()) {
      return convertStruct(relDataType, relDataType.getSqlIdentifier().toString());
    }

    return relDataTypeToAvroType(relDataType, relDataType.getSqlIdentifier().toString());
  }

  private static Schema convertStruct(final RelDataType type, String name) {
    SchemaBuilder.RecordBuilder rb = SchemaBuilder.record(name);
    rb.namespace(SAMZA_GENERATED_AVRO_NS);
    SchemaBuilder.FieldAssembler fa = rb.fields();
    for (RelDataTypeField f : type.getFieldList()) {
      fa = fa.name(f.getName()).type(relDataTypeToAvroType(f.getType(), f.getName())).noDefault();
    }

    return (Schema) fa.endRecord();
  }

  private static Schema relDataTypeToAvroType(RelDataType type, String fieldName) {
    SqlTypeName sqlTypeName = type.getSqlTypeName();
    boolean nullable = type.isNullable();

    if (type.isStruct()) {
      return convertStruct(type, String.format("%sType", fieldName));
    } else if (sqlTypeName == SqlTypeName.ARRAY) {
      ArraySqlType at = (ArraySqlType) type;

      if(nullable){
        return SchemaBuilder.unionOf()
            .nullType()
            .and()
            .array().items(relDataTypeToAvroType(at.getComponentType(), String.format("%sElementType", fieldName)))
            .endUnion();
      }

      return SchemaBuilder.array().items(relDataTypeToAvroType(at.getComponentType(), String.format("%sElementType", fieldName)));
    } else if (sqlTypeName == SqlTypeName.BIGINT) {
      return nullableLong(nullable);
    } else if (sqlTypeName == SqlTypeName.BINARY) {
      if(nullable){
        return SchemaBuilder.unionOf().nullType().and().bytesType().endUnion();
      }

      return SchemaBuilder.builder().bytesType();
    } else if (sqlTypeName == SqlTypeName.BOOLEAN) {
      if(nullable){
        return SchemaBuilder.unionOf().nullType().and().booleanType().endUnion();
      }

      return SchemaBuilder.builder().booleanType();
    } else if (sqlTypeName == SqlTypeName.CHAR) {
      if(nullable){
        return SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
      }

      return SchemaBuilder.builder().stringType();
    } else if (sqlTypeName == SqlTypeName.DOUBLE) {
      if(nullable){
        return SchemaBuilder.unionOf().nullType().and().doubleType().endUnion();
      }

      return SchemaBuilder.builder().doubleType();
    } else if (sqlTypeName == SqlTypeName.FLOAT) {
      if(nullable){
        return SchemaBuilder.unionOf().nullType().and().floatType().endUnion();
      }

      return SchemaBuilder.builder().floatType();
    } else if (sqlTypeName == SqlTypeName.INTEGER) {
      if(nullable){
        return SchemaBuilder.unionOf().nullType().and().intType().endUnion();
      }

      return SchemaBuilder.builder().intType();
    } else if (sqlTypeName == SqlTypeName.VARCHAR) {
      if(nullable){
        return SchemaBuilder.unionOf().nullType().and().stringType().endUnion();
      }

      return SchemaBuilder.builder().stringType();
    } else if (sqlTypeName == SqlTypeName.TIMESTAMP) {
      // TODO: Using long because Avro doesn't support date/time types. May be we should remove support for date/time types?
      return nullableLong(nullable);
    } else if (sqlTypeName == SqlTypeName.DATE) {
      throw new IllegalArgumentException("Unsupported type DATE.");
    } else if (sqlTypeName == SqlTypeName.TIME) {
      throw new IllegalArgumentException("Unsupported type TIME.");
    } else if (sqlTypeName == SqlTypeName.TINYINT) {
      if(nullable){
        return SchemaBuilder.unionOf().nullType().and().intType().endUnion();
      }

      return SchemaBuilder.builder().intType();
    } else {
      throw new SamzaException(String.format("fields with type %s is not supported in this version.", type.getFullTypeString()));
    }
  }

  private static Schema nullableLong(boolean nullable){
    if(nullable){
      return SchemaBuilder.unionOf().nullType().and().longType().endUnion();
    }

    return SchemaBuilder.builder().longType();
  }

}
