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

import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.avro.Schema;
import org.apache.avro.Schema.Parser;

import org.apache.nifi.annotation.behavior.WritesAttribute;
import org.apache.nifi.annotation.behavior.WritesAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.annotation.lifecycle.OnScheduled;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.InputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;
import org.apache.nifi.record.path.RecordPath;
import org.apache.nifi.record.path.util.RecordPathCache;
import org.apache.nifi.schema.access.SchemaNotFoundException;
import org.apache.nifi.serialization.MalformedRecordException;
import org.apache.nifi.serialization.RecordReader;
import org.apache.nifi.serialization.RecordReaderFactory;
import org.apache.nifi.serialization.record.Record;

import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.storage.Storage;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.aiplatform.v1.BatchPredictionJob;
import com.google.cloud.aiplatform.v1.GcsDestination;
import com.google.cloud.aiplatform.v1.GcsSource;
import com.google.cloud.aiplatform.v1.JobServiceClient;
import com.google.cloud.aiplatform.v1.JobServiceSettings;
import com.google.cloud.aiplatform.v1.LocationName;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.BlobInfo;
import com.google.cloud.storage.StorageOptions;

@Tags({"gemini", "nlp"})
@CapabilityDescription("Submits batch inference request to vertex ai using structured output to enforce schema")
@WritesAttributes({@WritesAttribute (attribute="vertex_ai.batch.name", description="name of the vertex inference batch"),
                   @WritesAttribute (attribute="vertex_ai.batch.display_name", description="display name of the vertex inference batch"),
                   @WritesAttribute (attribute="vertex_ai.batch.created_time", description="created time of the vertex inference batch")})
public class SubmitGeminiBatchRecord extends AbstractProcessor {
    private volatile RecordPathCache recordPathCache;

    static final PropertyDescriptor RECORD_READER = new PropertyDescriptor.Builder()
        .name("record-reader")
        .displayName("Record Reader")
        .description("Specifies the Controller Service to use for reading incoming data")
        .identifiesControllerService(RecordReaderFactory.class)
        .required(true)
        .build();

    static final PropertyDescriptor SCHEMA_TEXT = new PropertyDescriptor.Builder()
        .name("schema-text")
        .displayName("Schema Text")
        .description("Avro Schema Text used for output record schema")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();

    static final PropertyDescriptor PROMPT = new PropertyDescriptor.Builder()
        .name("prompt")
        .displayName("Prompt")
        .description("Prompt used for each record inference")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();

    static final PropertyDescriptor INVARIANT_FIELDS = new PropertyDescriptor.Builder()
        .name("invariants")
        .displayName("Invariants")
        .description("Invariant string fields that should be retained")
        .addValidator(StandardValidators.NON_BLANK_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .build();

    static final PropertyDescriptor INPUT_BUCKET = new PropertyDescriptor.Builder()
        .name("input-bucket")
        .displayName("Input Bucket")
        .description("Google Storage bucket used for input batch files")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(true)
        .build();

    static final PropertyDescriptor INPUT_LOCATION = new PropertyDescriptor.Builder()
        .name("input-location")
        .displayName("Input Location")
        .description("Google Storage location used for input batch files")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(true)
        .build();

    static final PropertyDescriptor OUTPUT_BUCKET = new PropertyDescriptor.Builder()
        .name("output-bucket")
        .displayName("Output Bucket")
        .description("Google Storage bucket used for output batch files")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(true)
        .build();

    static final PropertyDescriptor OUTPUT_LOCATION = new PropertyDescriptor.Builder()
        .name("output-location")
        .displayName("Output Location")
        .description("Google Storage location used for output batch files")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.ENVIRONMENT)
        .required(true)
        .build();

    static final PropertyDescriptor GOOGLE_LOCATION = new PropertyDescriptor.Builder()
        .name("google-location")
        .displayName("Google Location")
        .description("Google Cloud Location")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();

    static final PropertyDescriptor GOOGLE_PROJECT_ID = new PropertyDescriptor.Builder()
        .name("google-project-id")
        .displayName("Google Project ID")
        .description("Google Cloud Project Id")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(true)
        .build();

    static final PropertyDescriptor GCP_CREDENTIALS_JSON = new PropertyDescriptor.Builder()
        .name("gcp-credentials-json")
        .displayName("GCP Credentials Json")
        .description("Google Cloud Credentials in JSON format")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(true)
        .sensitive(true)
        .build();

    static final PropertyDescriptor MAX_OUTPUT_TOKENS = new PropertyDescriptor.Builder()
        .name("max-output-tokens")
        .displayName("Max Output Tokens")
        .description("Maximum output tokens allowed for a single record")
        .addValidator(StandardValidators.POSITIVE_LONG_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .defaultValue("2048")
        .build();

    @Override
    protected PropertyDescriptor getSupportedDynamicPropertyDescriptor(final String propertyDescriptorName) {
        return new PropertyDescriptor.Builder()
            .name(propertyDescriptorName)
            .description("Record path value to be injected into prompt expression evaluation:" + propertyDescriptorName)
            .required(false)
            .dynamic(true)
            .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
            .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
            .build();
    }

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles that are successfully transformed will be routed to this relationship")
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles that are not successfully transformed will be routed to this relationship")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(RECORD_READER);
        properties.add(SCHEMA_TEXT);
        properties.add(PROMPT);
        properties.add(INVARIANT_FIELDS);
        properties.add(INPUT_BUCKET);
        properties.add(INPUT_LOCATION);
        properties.add(OUTPUT_BUCKET);
        properties.add(OUTPUT_LOCATION);
        properties.add(GOOGLE_LOCATION);
        properties.add(GOOGLE_PROJECT_ID);
        properties.add(GCP_CREDENTIALS_JSON);
        properties.add(MAX_OUTPUT_TOKENS);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        return relationships;
    }

    protected GoogleCredentials getGoogleCredentials(final ProcessContext context) throws IOException {
        String credentialsJson = context.getProperty(GCP_CREDENTIALS_JSON).getValue();
        InputStream credentialsInputStream = new ByteArrayInputStream(credentialsJson.getBytes());
        GoogleCredentials credentials = GoogleCredentials.fromStream(credentialsInputStream)
            .createScoped(Collections.singletonList("https://www.googleapis.com/auth/cloud-platform"));
        credentialsInputStream.close();
        return credentials;
    }

    @OnScheduled
    public void createRecordPaths(final ProcessContext context) {
        recordPathCache = new RecordPathCache(context.getProperties().size());
    }

    private String createBatchRequest(ObjectMapper mapper, String prompt, HashMap<String, String> invariants, ObjectNode openAISchema, long maxOutputTokens) {
        ObjectNode part = mapper.createObjectNode();
        part.put("text", prompt);

        ObjectNode invariantsNode = mapper.createObjectNode();
        invariants.forEach((k, v) -> invariantsNode.put(k, v));
        ObjectNode invariantPart = mapper.createObjectNode();
        invariantPart.put("text", invariantsNode.toString());

        ObjectNode content = mapper.createObjectNode();
        content.put("role", "user");
        content.putArray("parts").add(invariantPart).add(part);

        ObjectNode generationConfig = mapper.createObjectNode();
        generationConfig.put("temperature", 0.9);
        generationConfig.put("topP", 1);
        generationConfig.put("maxOutputTokens", maxOutputTokens);
        generationConfig.put("responseMimeType", "application/json");
        generationConfig.set("responseSchema", openAISchema);

        ObjectNode labels = mapper.createObjectNode();
        invariants.forEach((k, v) -> labels.put(k, v));

        ObjectNode request = mapper.createObjectNode();
        request.putArray("contents").add(content);
        request.set("generationConfig", generationConfig);

        ObjectNode root = mapper.createObjectNode();
        root.set("request", request);

        return root.toString();
    }


    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        ObjectMapper mapper = new ObjectMapper();

        final RecordReaderFactory readerFactory = context.getProperty(RECORD_READER).asControllerService(RecordReaderFactory.class);

        Set<String> invariantFields;
        if (context.getProperty(INVARIANT_FIELDS).isSet()) {
            invariantFields = Set.of(context.getProperty(INVARIANT_FIELDS).getValue().split(","));
        } else {
            invariantFields = Set.of();
        }

        String schemaText = context.getProperty(SCHEMA_TEXT).evaluateAttributeExpressions(flowFile).getValue();

        String uuid = flowFile.getAttribute("uuid");

        String inputBucket = context.getProperty(INPUT_BUCKET).evaluateAttributeExpressions().getValue();
        String inputLocation = context.getProperty(INPUT_LOCATION).evaluateAttributeExpressions().getValue();
        String inputFileLocation = inputLocation + "/" + uuid + ".jsonl";

        String outputBucket = context.getProperty(OUTPUT_BUCKET).evaluateAttributeExpressions().getValue();
        String outputLocation = context.getProperty(OUTPUT_LOCATION).evaluateAttributeExpressions().getValue();

        String googleLocation = context.getProperty(GOOGLE_LOCATION).evaluateAttributeExpressions(flowFile).getValue();
        String googleProjectId = context.getProperty(GOOGLE_PROJECT_ID).evaluateAttributeExpressions(flowFile).getValue();

        long maxOutputTokens = Long.valueOf(context.getProperty(MAX_OUTPUT_TOKENS).getValue());

        HashMap<String, String> newAttributes = new HashMap<>();

        try {
            Parser parser = new Schema.Parser();
            Schema avroSchema = parser.parse(schemaText);
            ObjectNode openAISchema = SchemaConverter.convertSchema(avroSchema, invariantFields);

            session.read(flowFile, new InputStreamCallback() {
                @Override
                public void process(InputStream in) throws IOException {
                    try (final RecordReader reader = readerFactory.createRecordReader(flowFile, in, getLogger())) {
                        StringBuilder batchInputBuilder = new StringBuilder();

                        Record record;
                        while ((record = reader.nextRecord()) != null) {
                            HashMap<String, String> templateValues = new HashMap<String, String>();
                            for (final PropertyDescriptor property : context.getProperties().keySet()) {
                                if (property.isDynamic()) {
                                    final String value = context.getProperty(property).evaluateAttributeExpressions(flowFile).getValue();
                                    final RecordPath recordPath = recordPathCache.getCompiled(value);
                                    recordPath.evaluate(record).getSelectedFields().findFirst().ifPresent(v -> templateValues.put(property.getName(), v.getValue().toString()));
                                }
                            }

                            HashMap<String, String> invariants = new HashMap<>();
                            for (String f : invariantFields) {
                                invariants.put(f, templateValues.getOrDefault(f, ""));
                            }

                            String prompt = context.getProperty(PROMPT).evaluateAttributeExpressions(flowFile, templateValues).getValue();

                            batchInputBuilder.append(createBatchRequest(mapper, prompt, invariants, openAISchema, maxOutputTokens));
                            batchInputBuilder.append('\n');
                        }

                        BlobId id = BlobId.of(inputBucket, inputFileLocation); 
                        Storage storage = StorageOptions.newBuilder().setCredentials(getGoogleCredentials(context)).build().getService();
                        BlobInfo info = BlobInfo.newBuilder(id).build();
                        InputStream batchInputStream = new ByteArrayInputStream(batchInputBuilder.toString().getBytes());
                        storage.createFrom(info, batchInputStream);
                        batchInputStream.close();

                        String apiEndpoint = String.format("%s-aiplatform.googleapis.com:443", googleLocation);
                        String modelId = String.format("projects/%s/locations/%s/publishers/google/models/gemini-2.5-flash", googleProjectId, googleLocation);

                        String inputUri = String.format("gs://%s/%s", inputBucket, inputFileLocation);
                        String outputUri = String.format("gs://%s/%s", outputBucket, outputLocation);

                        BatchPredictionJob job = BatchPredictionJob.newBuilder()
                            .setDisplayName("batch-" + uuid)
                            .setModel(modelId)
                            .setInputConfig(BatchPredictionJob.InputConfig.newBuilder()
                                .setInstancesFormat("jsonl")
                                .setGcsSource(GcsSource.newBuilder().addUris(inputUri)))
                            .setOutputConfig(BatchPredictionJob.OutputConfig.newBuilder()
                                .setPredictionsFormat("jsonl")
                                .setGcsDestination(GcsDestination.newBuilder().setOutputUriPrefix(outputUri)))
                            .build();

                        JobServiceClient aiclient = JobServiceClient.create(JobServiceSettings.newBuilder()
                            .setCredentialsProvider(FixedCredentialsProvider.create(getGoogleCredentials(context)))
                            .setEndpoint(apiEndpoint)
                            .build());

                        String parent = LocationName.of(googleProjectId, googleLocation).toString();

                        BatchPredictionJob submittedJob = aiclient.createBatchPredictionJob(parent, job);

                        newAttributes.put("vertex_ai.batch.name", submittedJob.getName());
                        newAttributes.put("vertex_ai.batch.display_name", submittedJob.getDisplayName());
                        newAttributes.put("vertex_ai.batch.created_time", String.valueOf(submittedJob.getCreateTime().getSeconds()));
                    } catch (final SchemaNotFoundException | MalformedRecordException e) {
                        throw new ProcessException("Could not parse incoming data", e);
                    }
                }
            });

            session.putAllAttributes(flowFile, newAttributes);
        } catch (final Exception e) {
            getLogger().error("Failed to process {}; will route to failure", new Object[] {flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }

        session.transfer(flowFile, REL_SUCCESS);
    }
}
