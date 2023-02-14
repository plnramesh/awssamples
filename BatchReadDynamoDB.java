import java.io.IOException;
import java.util.List;
import java.util.Map;

import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClient;
import com.amazonaws.services.dynamodbv2.document.BatchGetItemOutcome;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.TableKeysAndAttributes;
import com.amazonaws.services.dynamodbv2.model.KeysAndAttributes;


public class BatchReadDynamoDB {

    static AmazonDynamoDBClient amazonDynamoDBClient = new AmazonDynamoDBClient (
            new ProfileCredentialsProvider());
    static DynamoDB dynamoDB;

    static String ibWebClaimsSegment = "IB_WEB_CLAIMS_SEGMENT";

    public static void main(String[] args) throws IOException {
        amazonDynamoDBClient.setEndpoint("https://dynamodb.eu-west-1.amazonaws.com");
        dynamoDB = new DynamoDB(amazonDynamoDBClient);
        retrieveMultipleItemsBatchGet();
    }
    private static void retrieveMultipleItemsBatchGet() {
        try {

            TableKeysAndAttributes segmentTableKeysAndAttributes = new TableKeysAndAttributes(ibWebClaimsSegment);
            // set the hash key and Range Key
            segmentTableKeysAndAttributes.addHashAndRangePrimaryKey("PNR","KHAFII","TKT","0755675663873");
            segmentTableKeysAndAttributes.addHashAndRangePrimaryKey("PNR","YUHPCP","TKT","0758622537562");
            System.out.println("Processing...");

            // filter for the required elements
            segmentTableKeysAndAttributes.withAttributeNames("id_golden_record");

            BatchGetItemOutcome outcome = dynamoDB.batchGetItem(
                    segmentTableKeysAndAttributes);

            Map<String, KeysAndAttributes> unprocessed = null;
            do {
                for (String tableName : outcome.getTableItems().keySet()) {
                    System.out.println("Table items for " + tableName);
                    List<Item> items = outcome.getTableItems().get(tableName);

                    for (Item item : items) {
                        System.out.println(item.toJSONPretty());
                    }
                }
                // Confirm no unprocessed items
                unprocessed = outcome.getUnprocessedKeys();

                if (unprocessed.isEmpty()) {
                    System.out.println("All items processed.");
                } else {
                    System.out.println("Gathering unprocessed items...");
                    outcome = dynamoDB.batchGetItemUnprocessed(unprocessed);
                }
            } while (!unprocessed.isEmpty());
        } catch (Exception e) {
            System.err.println("Could not get items.");
            System.err.println(e.getMessage());
        }
    }
