package com.pepese.sample;

import java.util.HashMap;
import java.util.Map;

import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemRequest;
import com.amazonaws.services.dynamodbv2.model.GetItemResult;
import com.amazonaws.services.dynamodbv2.model.PutItemRequest;
import com.amazonaws.services.dynamodbv2.model.QueryRequest;
import com.amazonaws.services.dynamodbv2.model.QueryResult;
import com.amazonaws.services.dynamodbv2.model.ScanRequest;
import com.amazonaws.services.dynamodbv2.model.ScanResult;

public class App {

	// 低レベルAPI を使用する
	private static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard().withRegion(Regions.AP_NORTHEAST_1)
			.build();

	public static void main(String[] args) {
		putItem();
		getItem();
		query();
		scan();
		deleteItem();
	}

	private static void putItem() {
		HashMap<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("test_hash", new AttributeValue().withS("xxxxx"));
		item.put("test_range", new AttributeValue().withS("yyyyy"));
		item.put("test_value", new AttributeValue().withS("zzzzz"));

		PutItemRequest request = new PutItemRequest().withTableName("test_table").withItem(item);

		try {
			client.putItem(request);
			System.out.println("PutItem is succeeded");
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	private static void getItem() {
		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("test_hash", new AttributeValue().withS("xxxxx"));
		key.put("test_range", new AttributeValue().withS("yyyyy"));

		GetItemRequest request = new GetItemRequest().withTableName("test_table").withKey(key);

		try {
			GetItemResult result = client.getItem(request);
			if (result != null && result.getItem() != null) {
				AttributeValue value = result.getItem().get("test_value");
				System.out.println("GetItem result is " + value);
			} else {
				System.out.println("No matching key was found");
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	private static void query() {
		HashMap<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		expressionAttributeValues.put(":hash", new AttributeValue().withS("xxxxx"));

		QueryRequest request = new QueryRequest().withTableName("test_table")
				.withKeyConditionExpression("test_hash = :hash") // 比較演算子 「:hash」は置換される変数
				.withExpressionAttributeValues(expressionAttributeValues);

		try {
			QueryResult result = client.query(request);
			if (result != null && result.getItems() != null) {
				for (Map<String, AttributeValue> item : result.getItems()) {
					AttributeValue value = item.get("test_value");
					System.out.println("Query result is " + value);
				}
			} else {
				System.out.println("No matching key was found");
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	private static void scan() {
		ScanRequest request = new ScanRequest().withTableName("test_table");

		try {
			ScanResult result = client.scan(request);
			if (result != null && result.getItems() != null) {
				for (Map<String, AttributeValue> item : result.getItems()) {
					AttributeValue value = item.get("test_value");
					System.out.println("Scan result is " + value);
				}
			} else {
				System.out.println("No matching key was found");
			}
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}

	private static void deleteItem() {
		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("test_hash", new AttributeValue().withS("xxxxx"));
		key.put("test_range", new AttributeValue().withS("yyyyy"));

		DeleteItemRequest request = new DeleteItemRequest().withTableName("test_table").withKey(key);

		try {
			client.deleteItem(request);
			System.out.println("DeleteItem is succeeded");
		} catch (Exception e) {
			System.err.println(e.getMessage());
		}
	}
}
