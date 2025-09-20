package property.requestapproval;

import java.io.IOException;
import java.io.InputStream;
import java.io.OutputStream;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;

import com.amazonaws.services.lambda.runtime.Context;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

import property.dao.Property;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbAsyncTable;
import software.amazon.awssdk.enhanced.dynamodb.DynamoDbEnhancedAsyncClient;
import software.amazon.awssdk.enhanced.dynamodb.Key;
import software.amazon.awssdk.enhanced.dynamodb.TableSchema;
import software.amazon.awssdk.enhanced.dynamodb.model.UpdateItemEnhancedRequest;
import software.amazon.awssdk.http.nio.netty.NettyNioAsyncHttpClient;
import software.amazon.awssdk.services.dynamodb.DynamoDbAsyncClient;
import schema.unicorn_properties.publicationevaluationcompleted.marshaller.Marshaller;
import schema.unicorn_properties.publicationevaluationcompleted.AWSEvent;
import schema.unicorn_properties.publicationevaluationcompleted.PublicationEvaluationCompleted;

/**
 * Function checks for the existence of a contract status entry for a specified
 * property.
 * 
 * If an entry exists, pause the workflow, and update the record with task
 * token.
 */
public class PublicationApprovedFunction {

        Logger logger = LogManager.getLogger();

        final String TABLE_NAME = System.getenv("DYNAMODB_TABLE");
        DynamoDbAsyncClient dynamodbClient = DynamoDbAsyncClient.builder()
                        .httpClientBuilder(NettyNioAsyncHttpClient.builder())
                        .build();

        DynamoDbEnhancedAsyncClient enhancedClient = DynamoDbEnhancedAsyncClient.builder()
                        .dynamoDbClient(dynamodbClient)
                        .build();

        DynamoDbAsyncTable<Property> propertyTable = enhancedClient.table(TABLE_NAME,
                        TableSchema.fromBean(Property.class));


        public void handleRequest(InputStream inputStream, OutputStream outputStream,
                        Context context) throws IOException {

            // Deserialize event into strongly typed object
            AWSEvent<PublicationEvaluationCompleted> event = Marshaller.unmarshalEvent(inputStream,PublicationEvaluationCompleted.class);

            String propertyId = event.getDetail().getPropertyId();
            String evaluationResult = event.getDetail().getEvaluationResult();

            publicationApproved(evaluationResult, propertyId);

                ObjectMapper objectMapper = new ObjectMapper();
                OutputStreamWriter writer = new OutputStreamWriter(outputStream, StandardCharsets.UTF_8);
                writer.write(objectMapper.writeValueAsString("'result': 'Successfully updated property status'"));
                writer.close();

        }

        private void publicationApproved(String evaluationResult, String propertyId) {

                String[] splitString = propertyId.split("/");
                String country = splitString[0];
                String city = splitString[1];
                String street = splitString[2];
                String number = splitString[3];
                String strPartionKey = ("property#" + country + "#" + city).replace(' ', '-').toLowerCase();
                String strSortKey = (street + "#" + number).replace(' ', '-').toLowerCase();

                Key key = Key.builder().partitionValue(strPartionKey).sortValue(strSortKey).build();
                Property existingProperty = propertyTable.getItem(key).join();
                
                if (existingProperty == null) {
                    logger.error("Property not found for ID: {}", propertyId);
                    throw new RuntimeException("Property not found with ID: " + propertyId);
                }
                
                // Always set the property number explicitly to ensure it's correct
                existingProperty.setPropertyNumber(number);
                existingProperty.setStatus(evaluationResult);
                
                logger.info("Updating property with status: {} and propertyNumber: {}", 
                           evaluationResult, existingProperty.getPropertyNumber());
                propertyTable.putItem(existingProperty).join();
        }

}
