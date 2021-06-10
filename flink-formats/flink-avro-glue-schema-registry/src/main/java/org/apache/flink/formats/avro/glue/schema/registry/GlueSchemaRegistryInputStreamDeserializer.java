/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.flink.formats.avro.glue.schema.registry;

import com.amazonaws.services.schemaregistry.deserializers.AWSDeserializer;
import com.amazonaws.services.schemaregistry.exception.AWSSchemaRegistryException;
import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;
import org.apache.avro.SchemaParseException;

import org.apache.flink.formats.avro.utils.MutableByteArrayInputStream;

import software.amazon.awssdk.auth.credentials.DefaultCredentialsProvider;

import java.io.IOException;
import java.io.InputStream;
import java.util.HashMap;
import java.util.Map;
import java.util.Random;

/**
 * AWS Glue Schema Registry input stream de-serializer to accept input stream and extract schema
 * from it and remove schema registry information in the input stream.
 */
public class GlueSchemaRegistryInputStreamDeserializer {
    private final AWSDeserializer awsDeserializer;
    private final Map<String, Schema> cache;
    private boolean jittered = false;

    /**
     * Constructor accepts configuration map for AWS Deserializer.
     *
     * @param configs configuration map
     */
    public GlueSchemaRegistryInputStreamDeserializer(Map<String, Object> configs) {
        awsDeserializer =
                AWSDeserializer.builder()
                        .credentialProvider(DefaultCredentialsProvider.builder().build())
                        .configs(configs)
                        .build();
        cache = new HashMap<>();
    }

    public GlueSchemaRegistryInputStreamDeserializer(AWSDeserializer awsDeserializer) {
        this.awsDeserializer = awsDeserializer;
        cache = new HashMap<>();
    }

    /**
     * Get schema and remove extra Schema Registry information within input stream.
     *
     * @param in input stream
     *
     * @return schema of object within input stream
     *
     * @throws IOException Exception during decompression
     */
    public Schema getSchemaAndDeserializedStream(InputStream in) throws IOException {
        byte[] inputBytes = new byte[in.available()];
        in.read(inputBytes);
        in.reset();

        if (!jittered) {
            injectJitter(1);
            jittered = true;
        }

        MutableByteArrayInputStream mutableByteArrayInputStream = (MutableByteArrayInputStream) in;
        String schemaDefinition = awsDeserializer.getSchema(inputBytes).getSchemaDefinition();
        byte[] deserializedBytes = awsDeserializer.getActualData(inputBytes);
        mutableByteArrayInputStream.setBuffer(deserializedBytes);
        Schema schema;
        if (cache.containsKey(schemaDefinition)) {
            schema = cache.get(schemaDefinition);
        } else {
            try {
                Parser schemaParser = new Schema.Parser();
                schema = schemaParser.parse(schemaDefinition);
                cache.put(schemaDefinition, schema);
            } catch (SchemaParseException e) {
                String message =
                        "Error occurred while parsing schema, see inner exception for details.";
                throw new AWSSchemaRegistryException(message, e);
            }
        }

        return schema;
    }


    /**
     * Since glue schema registry service has TPS limit,
     * we would like to delay a while to reduce the peak TPS during flink application startup
     *
     * @param jitterBoundInMinutes
     */
    private void injectJitter(int jitterBoundInMinutes) {
        final Random random = new Random(System.currentTimeMillis());
        final int sleepSecond = random.nextInt(
                jitterBoundInMinutes * 60
        );
        try {
            Thread.sleep(sleepSecond * 1000L);
        } catch (InterruptedException e) {
            // do nothing
        }
    }
}
