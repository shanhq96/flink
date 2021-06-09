package org.apache.flink.formats.avro.glue.schema.registry;

import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ConfigOptions;

public class GlueSchemaRegistryAvroOptions {
    public static final ConfigOption<String> GLUE_SCHEMA_REGISTRY_AWS_REGION =
            ConfigOptions.key("glue-schema-registry.aws-region")
                    .stringType()
                    .defaultValue("us-east-1")
                    .withDescription("Required aws-region to connect to schema registry service");

    public static final ConfigOption<String> GLUE_SCHEMA_REGISTRY_REGISTRY_NAME =
            ConfigOptions.key("glue-schema-registry.registry-name")
                    .stringType()
                    .defaultValue("default-registry")
                    .withDescription("Required registry-name to connect to schema registry service");

    public static final ConfigOption<String> GLUE_SCHEMA_REGISTRY_SCHEMA_NAME =
            ConfigOptions.key("glue-schema-registry.schema-name")
                    .stringType()
                    .noDefaultValue()
                    .withDescription("Required schema-name to connect to schema registry service");

    public static final ConfigOption<Boolean> GLUE_SCHEMA_REGISTRY_AUTO_REGISTRATION =
            ConfigOptions.key("glue-schema-registry.auto-registration")
                    .booleanType()
                    .defaultValue(false)
                    .withDescription("Required auto-registration to connect to schema registry service");

    public static final ConfigOption<Integer> GLUE_SCHEMA_REGISTRY_JITTER_BOUND_IN_MINUTES =
            ConfigOptions.key("glue-schema-registry.jitter-bound-in-minutes")
                    .intType()
                    .defaultValue(1)
                    .withDescription("Required jitter bound in minutes to init the schema registry service client");
}
