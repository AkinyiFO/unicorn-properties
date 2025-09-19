package properties;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.util.HashMap;
import java.util.Map;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.ObjectMapper;

import software.amazon.cloudwatchlogs.emf.logger.MetricsLogger;
import software.amazon.lambda.powertools.logging.CorrelationIdPathConstants;
import software.amazon.lambda.powertools.logging.Logging;
import software.amazon.lambda.powertools.metrics.Metrics;
import software.amazon.lambda.powertools.metrics.MetricsUtils;
import software.amazon.lambda.powertools.tracing.Tracing;
import software.amazon.lambda.powertools.tracing.TracingUtils;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import software.amazon.awssdk.services.dynamodb.DynamoDbClient;
import software.amazon.awssdk.services.dynamodb.model.AttributeValue;
import software.amazon.awssdk.services.dynamodb.model.UpdateItemRequest;
import software.amazon.awssdk.services.sfn.SfnClient;
import software.amazon.awssdk.services.sfn.model.SendTaskSuccessRequest;

import schema.unicorn_contracts.contractstatuschanged.AWSEvent;
import schema.unicorn_contracts.contractstatuschanged.ContractStatusChanged;
import schema.unicorn_contracts.contractstatuschanged.marshaller.Marshaller;


public class ContractStatusChangedHandlerFunction {

    private static final Logger LOGGER = LogManager.getLogger(ContractStatusChangedHandlerFunction.class);
    MetricsLogger metricsLogger = MetricsUtils.metricsLogger();

    final String TABLE_NAME = System.getenv("CONTRACT_STATUS_TABLE");

    ObjectMapper objectMapper = new ObjectMapper();

    DynamoDbClient dynamodbClient = DynamoDbClient.builder()
            .build();

    // ADDED: Step Functions client to resume workflow
    SfnClient sfnClient = SfnClient.builder().build();

    @Tracing
    public void handleRequest(InputStream inputStream, OutputStream outputStream,
                              Context context) throws IOException {

        AWSEvent<ContractStatusChanged> event = Marshaller.unmarshalEvent(inputStream);
        ContractStatusChanged contractStatusChanged = event.getDetail();

        if (contractStatusChanged != null) {
            saveContractStatusAndResumeWorkflow(
                    contractStatusChanged.getPropertyId(),
                    contractStatusChanged.getContractStatus(),
                    contractStatusChanged.getContractId(),
                    contractStatusChanged.getContractLastModifiedOn()
            );
        }

        OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
        writer.write(objectMapper.writeValueAsString("OK"));
        writer.close();
    }

    @Tracing
    void saveContractStatusAndResumeWorkflow(String propertyId,
                                             String contractStatus, String contractId, Long contractLastModifiedOn) {
        Map<String, AttributeValue> key = new HashMap<>();
        key.put("property_id", AttributeValue.fromS(propertyId));

        Map<String, AttributeValue> expressionAttributeValues = new HashMap<>();
        expressionAttributeValues.put(":t", AttributeValue.fromS(contractStatus));
        expressionAttributeValues.put(":c", AttributeValue.fromS(contractId));
        expressionAttributeValues.put(":m", AttributeValue.fromN(String.valueOf(contractLastModifiedOn)));

        UpdateItemRequest updateItemRequest = UpdateItemRequest.builder()
                .key(key)
                .tableName(TABLE_NAME)
                .updateExpression("set contract_status=:t, contract_last_modified_on=:m, contract_id=:c")
                .expressionAttributeValues(expressionAttributeValues)
                .build();

        dynamodbClient.updateItem(updateItemRequest);

        // ADDED: Resume Step Functions workflow if status is APPROVED
        if ("APPROVED".equals(contractStatus)) {
            resumeStepFunction(propertyId);
        }
    }

    // ADDED: Method to resume Step Functions workflow
    private void resumeStepFunction(String propertyId) {
        try {
            // Get task token from DynamoDB
            Map<String, AttributeValue> key = new HashMap<>();
            key.put("property_id", AttributeValue.builder().s(propertyId).build());

            Map<String, AttributeValue> item = dynamodbClient.getItem(r -> r
                    .tableName(TABLE_NAME)
                    .key(key)
            ).item();

            if (item == null || !item.containsKey("sfn_wait_approved_task_token")) {
                LOGGER.warn("No task token found for property: " + propertyId);
                return;
            }

            String taskToken = item.get("sfn_wait_approved_task_token").s();

            // Resume Step Functions workflow
            sfnClient.sendTaskSuccess(SendTaskSuccessRequest.builder()
                    .taskToken(taskToken)
                    .output("{\"status\": \"APPROVED\", \"property_id\": \"" + propertyId + "\"}")
                    .build());

            LOGGER.info("Resumed Step Functions workflow for property: " + propertyId);
        } catch (Exception e) {
            LOGGER.error("Failed to resume Step Functions workflow: " + e.getMessage(), e);
            // Don't throw - we don't want to fail the contract status update
        }
    }

    public void setDynamodbClient(DynamoDbClient dynamodbClient) {
        this.dynamodbClient = dynamodbClient;
    }

    // ADDED: Setter for testing
    public void setSfnClient(SfnClient sfnClient) {
        this.sfnClient = sfnClient;
    }
}