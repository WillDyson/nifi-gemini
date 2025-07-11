/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.ps.wdyson.nifi.processors.gemini;

import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Field;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.fasterxml.jackson.databind.node.ObjectNode;

public class SchemaConverter {
    public static ObjectNode convertSchema(Schema schema, Set<String> invariantFields) {
        return convertSchema(schema, invariantFields, new ObjectMapper());
    }

    public static ObjectNode convertSchema(Schema schema, Set<String> invariantFields, ObjectMapper objectMapper) {
        if (schema.getType() != Schema.Type.RECORD) {
            throw new IllegalArgumentException("Root schema must be of type RECORD.");
        }

        ObjectNode openAISchema = objectMapper.createObjectNode();
        openAISchema.put("type", "object");
        if (schema.getDoc() != null) {
            openAISchema.put("description", schema.getDoc());
        }

        ObjectNode propertiesNode = objectMapper.createObjectNode();
        ArrayNode requiredFields = objectMapper.createArrayNode();

        for (Field field : schema.getFields()) {
            if (invariantFields.contains(field.name())) {
                if (field.schema().getType().equals(Schema.Type.STRING)) {

                } else {
                    throw new IllegalArgumentException("Invariant fields must have STRING type");
                }
            } else {
                propertiesNode.set(field.name(), convert(field, objectMapper));

                if (!field.schema().isNullable()) {
                    requiredFields.add(field.name());
                }
            }
        }

        openAISchema.set("properties", propertiesNode);
        if (!requiredFields.isEmpty()) {
            openAISchema.set("required", requiredFields);
        }

        return openAISchema;
    }

    private static ObjectNode convert(Field field, ObjectMapper objectMapper) {
        ObjectNode propertyNode = objectMapper.createObjectNode();
        if (field.doc() != null) {
            propertyNode.put("description", field.doc());
        }

        Schema fieldSchema = field.schema();

        switch (fieldSchema.getType()) {
            case RECORD:
                propertyNode.put("type", "object");
                ObjectNode nestedProperties = objectMapper.createObjectNode();
                ArrayNode nestedRequired = objectMapper.createArrayNode();
                for (Field nestedField : fieldSchema.getFields()) {
                    nestedProperties.set(nestedField.name(), convert(nestedField, objectMapper));
                    if (!nestedField.schema().isNullable()) {
                        nestedRequired.add(nestedField.name());
                    }
                }
                propertyNode.set("properties", nestedProperties);
                if (!nestedRequired.isEmpty()) {
                    propertyNode.set("required", nestedRequired);
                }
                break;
            case ARRAY:
                propertyNode.put("type", "array");
                Schema elementSchema = fieldSchema.getElementType();
                Field elementField = new Field("item", elementSchema, null, null);
                propertyNode.set("items", convert(elementField, objectMapper));
                break;
            case INT:
            case LONG:
                propertyNode.put("type", "integer");
                break;
            case FLOAT:
            case DOUBLE:
                propertyNode.put("type", "number");
                break;
            case BOOLEAN:
                propertyNode.put("type", "boolean");
                break;
            case STRING:
            case ENUM:
            default:
                propertyNode.put("type", "string");
                break;
        }
        return propertyNode;
    }
}