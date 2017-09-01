package com.pepese.sample.dynamodb;

import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;

import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemRequest;
import com.amazonaws.services.dynamodbv2.model.BatchWriteItemResult;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ReturnConsumedCapacity;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;

import lombok.extern.slf4j.Slf4j;

@Service
@Slf4j
public class DynamoDBClient {

	private static AmazonDynamoDB client;
	
	@Value("${aws.dynamodb.region:AP_NORTHEAST_1}")
	private String region;

	@PostConstruct
	public void init() {
		// 低レベルAPI を使用する
		client = AmazonDynamoDBClientBuilder.standard().withRegion(region).build();
		// default で、back-off strategy アルゴリズムでエラーリトライ 3 回される
		// そのため、各メソッドでリトライの実装はしない（batch系は除く）
	}

	public void putItem(String tableName, Map<String, AttributeValue> item) {
		if(tableName == null || item == null || item.size() == 0) {
			return;
		}
		PutItemRequest request = new PutItemRequest().withTableName(tableName).withItem(item);
		try {
			client.putItem(request);
			log.debug("PutItem is succeeded.");
		} catch (Exception e) {
			throwRuntimeException("PutItem is failed.", e);
		}
	}

	public Map<String, AttributeValue> getItem(String tableName, Map<String, AttributeValue> key) {
		if(tableName == null || key == null || key.size() == 0) {
			return null;
		}
		GetItemRequest request = new GetItemRequest().withTableName(tableName).withKey(key);
		GetItemResult result = null;
		try {
			result = client.getItem(request);
			if (result != null && result.getItem() != null) {
				log.debug("GetItem is succeeded.");
			} else {
				log.debug("GetItem is succeeded but no matching key is found.");
			}
		} catch (Exception e) {
			throwRuntimeException("GetItem is failed.", e);
		}
		return result != null ? result.getItem() : null;
	}

	public List<Map<String, AttributeValue>> query(String tableName, String keyConditionExpression, Map<String, AttributeValue> expressionAttributeValues) {
		if(tableName == null || keyConditionExpression == null || expressionAttributeValues == null) {
			return null;
		}
		QueryRequest request = new QueryRequest().withTableName(tableName)
				.withKeyConditionExpression(keyConditionExpression)
				.withExpressionAttributeValues(expressionAttributeValues);
		QueryResult result = null;
		try {
			result = client.query(request);
			if (result != null && result.getItems() != null) {
				log.debug("Query is succeeded.");
			} else {
				log.debug("Query is succeeded but no matching key is found.");
			}
		} catch (Exception e) {
			throwRuntimeException("Query is failed.", e);
			System.err.println(e.getMessage());
		}
		return result != null ? result.getItems() : null;
	}

	public List<Map<String, AttributeValue>> scan(String tableName) {
		if(tableName == null) {
			return null;
		}
		ScanRequest request = new ScanRequest().withTableName(tableName);
		ScanResult result = null;
		try {
			result = client.scan(request);
			if (result != null && result.getItems() != null) {
				log.debug("Scan result is succeeded.");
			} else {
				log.debug("Scan result is succeeded but no matching key was found.");
			}
		} catch (Exception e) {
			throwRuntimeException("Scan is failed.", e);
		}
		return result != null ? result.getItems() : null;
	}

	public void deleteItem(String tableName, Map<String, AttributeValue> key) {
		if(tableName == null || key == null || key.size() == 0) {
			return;
		}
		DeleteItemRequest request = new DeleteItemRequest().withTableName(tableName).withKey(key);
		try {
			client.deleteItem(request);
			System.out.println("DeleteItem is succeeded.");
		} catch (Exception e) {
			throwRuntimeException("DeleteItem is failed.", e);
		}
	}

	public void batchWrite(Map<String, List<WriteRequest>> items) {
		// 25 Request以下なのか気にしないようにしたい
		if (items == null || items.isEmpty()) {
			return;
		}
		try {
			BatchWriteItemRequest bwir = new BatchWriteItemRequest()
					.withReturnConsumedCapacity(ReturnConsumedCapacity.TOTAL).withRequestItems(items);
			BatchWriteItemResult result = client.batchWriteItem(bwir);
			if (result == null) {
				return;
			}
			System.out.println("[DEBUG] batchWrite() CC: " + result.getConsumedCapacity()); // 消費されたキャパシティーユニット

			if (result.getUnprocessedItems() != null && !result.getUnprocessedItems().isEmpty()) {
				Thread.sleep(1000); // Exponential Backoff アルゴリズムに変更する？
				System.out.println("[WARN] UNPROCESSED " + result.getUnprocessedItems().size());
				batchWrite(result.getUnprocessedItems());
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	private void throwRuntimeException(String message, Exception e) {
		log.error(message, e);
		throw new RuntimeException(message, e);
	}

	private void throwRuntimeException(String message) {
		log.error(message);
		throw new RuntimeException(message);
	}

}
