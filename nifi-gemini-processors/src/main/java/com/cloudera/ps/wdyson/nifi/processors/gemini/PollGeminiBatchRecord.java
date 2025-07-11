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

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.ByteArrayInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

import org.apache.nifi.annotation.behavior.ReadsAttribute;
import org.apache.nifi.annotation.behavior.ReadsAttributes;
import org.apache.nifi.annotation.documentation.CapabilityDescription;
import org.apache.nifi.annotation.documentation.Tags;
import org.apache.nifi.components.PropertyDescriptor;
import org.apache.nifi.expression.ExpressionLanguageScope;
import org.apache.nifi.flowfile.FlowFile;
import org.apache.nifi.processor.AbstractProcessor;
import org.apache.nifi.processor.ProcessContext;
import org.apache.nifi.processor.ProcessSession;
import org.apache.nifi.processor.Relationship;
import org.apache.nifi.processor.exception.ProcessException;
import org.apache.nifi.processor.io.OutputStreamCallback;
import org.apache.nifi.processor.util.StandardValidators;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import com.google.cloud.storage.Storage;
import com.google.api.gax.core.FixedCredentialsProvider;
import com.google.auth.oauth2.GoogleCredentials;
import com.google.cloud.aiplatform.v1.BatchPredictionJob;
import com.google.cloud.aiplatform.v1.JobServiceClient;
import com.google.cloud.aiplatform.v1.JobServiceSettings;
import com.google.cloud.storage.BlobId;
import com.google.cloud.storage.StorageOptions;

@Tags({"gemini", "nlp"})
@CapabilityDescription("Polls vertex ai for batch inference results")
@ReadsAttributes({@ReadsAttribute (attribute="vertex_ai.batch.name", description="name of the vertex inference batch")})
public class PollGeminiBatchRecord extends AbstractProcessor {
    static final PropertyDescriptor BATCH_NAME = new PropertyDescriptor.Builder()
        .name("batch-name")
        .displayName("Batch Name")
        .description("Name of Vertex AI Batch Inference")
        .addValidator(StandardValidators.NON_EMPTY_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.FLOWFILE_ATTRIBUTES)
        .required(false)
        .defaultValue("${vertex_ai.batch.name}")
        .build();

    static final PropertyDescriptor GOOGLE_LOCATION = new PropertyDescriptor.Builder()
        .name("google-location")
        .displayName("Google Location")
        .description("Google Cloud Location")
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

    static final PropertyDescriptor IGNORE_MAX_OUTPUT_FAILURES = new PropertyDescriptor.Builder()
        .name("ignore-max-output-failures")
        .displayName("Ignore Max Output Failures")
        .description("Ignore failures due to max output token limits")
        .addValidator(StandardValidators.BOOLEAN_VALIDATOR)
        .expressionLanguageSupported(ExpressionLanguageScope.NONE)
        .required(false)
        .defaultValue("false")
        .build();

    static final Relationship REL_SUCCESS = new Relationship.Builder()
        .name("success")
        .description("FlowFiles that are successfully transformed will be routed to this relationship")
        .build();

    static final Relationship REL_FAILURE = new Relationship.Builder()
        .name("failure")
        .description("FlowFiles that are not successfully transformed will be routed to this relationship")
        .build();

    static final Relationship REL_RETRY = new Relationship.Builder()
        .name("retry")
        .description("FlowFiles that are not ready and should be retried later will be routed to this relationship")
        .build();

    static final Relationship REL_ORIGNAL = new Relationship.Builder()
        .name("original")
        .description("Original FlowFile")
        .build();

    @Override
    protected List<PropertyDescriptor> getSupportedPropertyDescriptors() {
        final List<PropertyDescriptor> properties = new ArrayList<>();
        properties.add(BATCH_NAME);
        properties.add(GOOGLE_LOCATION);
        properties.add(GCP_CREDENTIALS_JSON);
        properties.add(IGNORE_MAX_OUTPUT_FAILURES);
        return properties;
    }

    @Override
    public Set<Relationship> getRelationships() {
        final Set<Relationship> relationships = new HashSet<>();
        relationships.add(REL_SUCCESS);
        relationships.add(REL_FAILURE);
        relationships.add(REL_RETRY);
        relationships.add(REL_ORIGNAL);
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

    private void writePredictionResponses(byte[] predictions, OutputStream os, boolean ignoreMaxOutputFailures) throws IOException {
        ObjectMapper mapper = new ObjectMapper();

        try (BufferedWriter writer = new BufferedWriter(new OutputStreamWriter(os))) {
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(new ByteArrayInputStream(predictions)))) {
                String currLine;
                while ((currLine = reader.readLine()) != null) {
                    JsonNode root = mapper.readTree(currLine);

                    ObjectNode invariants = (ObjectNode) mapper.readTree(root
                        .get("request")
                        .get("contents")
                        .get(0)
                        .get("parts")
                        .get(0)
                        .get("text")
                        .asText());

                    JsonNode responseContent = root
                        .get("response")
                        .get("candidates")
                        .get(0);


                    String finishReason = responseContent.get("finishReason").asText("undefined");

                    ObjectNode response;

                    if (finishReason.equals("STOP") && responseContent.get("content").has("parts")) {
                        response = (ObjectNode) mapper.readTree(
                            responseContent
                            .get("content")
                            .get("parts")
                            .get(0)
                            .get("text")
                            .asText()
                        );
                    } else if (ignoreMaxOutputFailures && finishReason.equals("MAX_TOKENS")) {
                        continue;
                    } else {
                        throw new IOException(String.format("No output returned by LLM due to finish reason %s", finishReason));
                    }

                    writer.append(response.setAll(invariants).toString());
                    writer.newLine();
                }
            }
        }
    }

    @Override
    public void onTrigger(ProcessContext context, ProcessSession session) throws ProcessException {
        FlowFile flowFile = session.get();
        if (flowFile == null) {
            return;
        }

        String batchName = context.getProperty(BATCH_NAME).evaluateAttributeExpressions(flowFile).getValue();

        String googleLocation = context.getProperty(GOOGLE_LOCATION).evaluateAttributeExpressions(flowFile).getValue();

        boolean ignoreMaxOutputFailures = Boolean.parseBoolean(context.getProperty(IGNORE_MAX_OUTPUT_FAILURES).getValue());

        try {
            String apiEndpoint = String.format("%s-aiplatform.googleapis.com:443", googleLocation);

            JobServiceClient aiclient = JobServiceClient.create(JobServiceSettings.newBuilder()
                .setCredentialsProvider(FixedCredentialsProvider.create(getGoogleCredentials(context)))
                .setEndpoint(apiEndpoint)
                .build());

            BatchPredictionJob job = aiclient.getBatchPredictionJob(batchName);

            session.putAttribute(flowFile, "vertex_ai.batch.state", job.getState().toString());

            switch (job.getState()) {
                case JOB_STATE_PENDING:
                case JOB_STATE_QUEUED:
                case JOB_STATE_RUNNING:
                case JOB_STATE_UPDATING:
                case JOB_STATE_PAUSED:
                    session.transfer(flowFile, REL_RETRY);
                    break;
                case UNRECOGNIZED:
                case JOB_STATE_CANCELLED:
                case JOB_STATE_CANCELLING:
                case JOB_STATE_EXPIRED:
                case JOB_STATE_FAILED:
                case JOB_STATE_PARTIALLY_SUCCEEDED:
                case JOB_STATE_UNSPECIFIED:
                    session.transfer(flowFile, REL_FAILURE);
                    break;
                case JOB_STATE_SUCCEEDED:
                    String predictionsPath = job.getOutputInfo().getGcsOutputDirectory() + "/predictions.jsonl";
                    session.putAttribute(flowFile, "vertex_ai.batch.output.path", predictionsPath);
                    Storage storage = StorageOptions.newBuilder().setCredentials(getGoogleCredentials(context)).build().getService();
                    byte[] predictions = storage.readAllBytes(BlobId.fromGsUtilUri(predictionsPath));
                    FlowFile predictionsFlowFile = session.create(flowFile);
                    try {
                        session.write(predictionsFlowFile, new OutputStreamCallback() {
                            @Override
                            public void process(OutputStream os) throws IOException {
                                writePredictionResponses(predictions, os, ignoreMaxOutputFailures);
                            }
                        });
                        session.transfer(predictionsFlowFile, REL_SUCCESS);
                        session.transfer(flowFile, REL_ORIGNAL);
                    } catch (Exception e) {
                        session.remove(predictionsFlowFile);
                        throw e;
                    }
            }

        } catch (final Exception e) {
            getLogger().error("Failed to process {}; will route to failure", new Object[] {flowFile, e});
            session.transfer(flowFile, REL_FAILURE);
            return;
        }
    }
}
