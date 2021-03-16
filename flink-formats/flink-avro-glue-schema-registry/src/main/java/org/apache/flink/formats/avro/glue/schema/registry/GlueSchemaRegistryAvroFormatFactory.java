package org.apache.flink.formats.avro.glue.schema.registry;

import com.amazonaws.services.schemaregistry.utils.AWSSchemaRegistryConstants;
import com.amazonaws.services.schemaregistry.utils.AvroRecordType;

import org.apache.flink.api.common.serialization.DeserializationSchema;
import org.apache.flink.api.common.serialization.SerializationSchema;
import org.apache.flink.api.common.typeinfo.TypeInformation;
import org.apache.flink.configuration.ConfigOption;
import org.apache.flink.configuration.ReadableConfig;
import org.apache.flink.formats.avro.AvroRowDataDeserializationSchema;
import org.apache.flink.formats.avro.AvroRowDataSerializationSchema;
import org.apache.flink.formats.avro.AvroToRowDataConverters;
import org.apache.flink.formats.avro.RowDataToAvroConverters;
import org.apache.flink.formats.avro.typeutils.AvroSchemaConverter;
import org.apache.flink.table.connector.ChangelogMode;
import org.apache.flink.table.connector.format.DecodingFormat;
import org.apache.flink.table.connector.format.EncodingFormat;
import org.apache.flink.table.connector.sink.DynamicTableSink;
import org.apache.flink.table.connector.source.DynamicTableSource;
import org.apache.flink.table.data.RowData;
import org.apache.flink.table.factories.DeserializationFormatFactory;
import org.apache.flink.table.factories.DynamicTableFactory;
import org.apache.flink.table.factories.FactoryUtil;
import org.apache.flink.table.factories.SerializationFormatFactory;
import org.apache.flink.table.types.DataType;
import org.apache.flink.table.types.logical.RowType;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import static org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroOptions.GLUE_SCHEMA_REGISTRY_AUTO_REGISTRATION;
import static org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroOptions.GLUE_SCHEMA_REGISTRY_AWS_REGION;
import static org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroOptions.GLUE_SCHEMA_REGISTRY_REGISTRY_NAME;
import static org.apache.flink.formats.avro.glue.schema.registry.GlueSchemaRegistryAvroOptions.GLUE_SCHEMA_REGISTRY_SCHEMA_NAME;

public class GlueSchemaRegistryAvroFormatFactory
        implements DeserializationFormatFactory, SerializationFormatFactory {

    public static final String IDENTIFIER = "avro-glue";

    @Override
    public DecodingFormat<DeserializationSchema<RowData>> createDecodingFormat(
            DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        final Map<String, Object> configs = new HashMap<>();
        configs.put(
                AWSSchemaRegistryConstants.SCHEMA_NAME,
                formatOptions.get(GLUE_SCHEMA_REGISTRY_SCHEMA_NAME));
        configs.put(
                AWSSchemaRegistryConstants.AWS_REGION,
                formatOptions.get(GLUE_SCHEMA_REGISTRY_AWS_REGION));
        configs.put(
                AWSSchemaRegistryConstants.REGISTRY_NAME,
                formatOptions.get(GLUE_SCHEMA_REGISTRY_REGISTRY_NAME));
        configs.put(
                AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
                AvroRecordType.GENERIC_RECORD.getName());

        return new DecodingFormat<DeserializationSchema<RowData>>() {
            @Override
            public DeserializationSchema<RowData> createRuntimeDecoder(
                    DynamicTableSource.Context context,
                    DataType producedDataType) {
                final RowType rowType = (RowType) producedDataType.getLogicalType();
                final TypeInformation<RowData> rowDataTypeInfo =
                        context.createTypeInformation(producedDataType);
                return new AvroRowDataDeserializationSchema(
                        GlueSchemaRegistryAvroDeserializationSchema.forGeneric(
                                AvroSchemaConverter.convertToSchema(rowType), configs),
                        AvroToRowDataConverters.createRowConverter(rowType),
                        rowDataTypeInfo);
            }

            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }
        };
    }

    @Override
    public EncodingFormat<SerializationSchema<RowData>> createEncodingFormat(
            DynamicTableFactory.Context context,
            ReadableConfig formatOptions) {
        FactoryUtil.validateFactoryOptions(this, formatOptions);

        final String schemaName = formatOptions.get(GLUE_SCHEMA_REGISTRY_SCHEMA_NAME);
        final Map<String, Object> configs = new HashMap<>();
        configs.put(AWSSchemaRegistryConstants.SCHEMA_NAME, schemaName);
        configs.put(
                AWSSchemaRegistryConstants.AWS_REGION,
                formatOptions.get(GLUE_SCHEMA_REGISTRY_AWS_REGION));
        configs.put(
                AWSSchemaRegistryConstants.REGISTRY_NAME,
                formatOptions.get(GLUE_SCHEMA_REGISTRY_REGISTRY_NAME));
        configs.put(
                AWSSchemaRegistryConstants.SCHEMA_AUTO_REGISTRATION_SETTING,
                formatOptions.get(GLUE_SCHEMA_REGISTRY_AUTO_REGISTRATION));
        configs.put(
                AWSSchemaRegistryConstants.AVRO_RECORD_TYPE,
                AvroRecordType.GENERIC_RECORD.getName());
        
        return new EncodingFormat<SerializationSchema<RowData>>() {
            @Override
            public ChangelogMode getChangelogMode() {
                return ChangelogMode.insertOnly();
            }

            @Override
            public SerializationSchema<RowData> createRuntimeEncoder(
                    DynamicTableSink.Context context,
                    DataType consumedDataType) {
                final RowType rowType = (RowType) consumedDataType.getLogicalType();
                return new AvroRowDataSerializationSchema(
                        rowType,
                        GlueSchemaRegistryAvroSerializationSchema.forGeneric(
                                AvroSchemaConverter.convertToSchema(rowType),
                                schemaName,
                                configs),
                        RowDataToAvroConverters.createConverter(rowType));
            }
        };
    }

    @Override
    public String factoryIdentifier() {
        return IDENTIFIER;
    }

    @Override
    public Set<ConfigOption<?>> requiredOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(GLUE_SCHEMA_REGISTRY_SCHEMA_NAME);

        return options;
    }

    @Override
    public Set<ConfigOption<?>> optionalOptions() {
        final Set<ConfigOption<?>> options = new HashSet<>();
        options.add(GLUE_SCHEMA_REGISTRY_AWS_REGION);
        options.add(GLUE_SCHEMA_REGISTRY_REGISTRY_NAME);
        options.add(GLUE_SCHEMA_REGISTRY_AUTO_REGISTRATION);

        return options;
    }
}
