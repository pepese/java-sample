package com.pepese.sample;

import java.nio.ByteBuffer;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.CommandLineRunner;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;

import com.amazonaws.services.dynamodbv2.model.AttributeValue;
import com.amazonaws.services.dynamodbv2.model.DeleteRequest;
import com.amazonaws.services.dynamodbv2.model.PutRequest;
import com.amazonaws.services.dynamodbv2.model.WriteRequest;
import com.pepese.sample.dynamodb.DynamoDBClient;

@SpringBootApplication
public class App implements CommandLineRunner {

	@Autowired
	private DynamoDBClient client;

	@Override
	public void run(String... args) {
		// create item data for putItem
		HashMap<String, AttributeValue> item = new HashMap<String, AttributeValue>();
		item.put("test_hash", new AttributeValue().withS("xxxxx"));
		item.put("test_range", new AttributeValue().withS("yyyyy"));
		List<Float> vector = new ArrayList<Float>();
		vector.add(new Float(0.5));vector.add(new Float(0.3));vector.add(new Float(0.2));
		ByteBuffer vectorByteBuffer = ByteBuffer.allocate(Float.BYTES * vector.size());
		for (Float num : vector) {
			vectorByteBuffer.putFloat(num);
		}
		vectorByteBuffer.rewind();
		item.put("test_binary", new AttributeValue().withB(vectorByteBuffer));

		// create key data for getItem and deleteItem
		HashMap<String, AttributeValue> key = new HashMap<String, AttributeValue>();
		key.put("test_hash", new AttributeValue().withS("xxxxx"));
		key.put("test_range", new AttributeValue().withS("yyyyy"));

		// create data for query
		Map<String, AttributeValue> expressionAttributeValues = new HashMap<String, AttributeValue>();
		expressionAttributeValues.put(":hash", new AttributeValue().withS("xxxxx"));

		// execute putItem, getItem, query, scan and deleteItem operation
		client.putItem("test_table", item);
		Map<String, AttributeValue> getItemResult = client.getItem("test_table", key);
		ByteBuffer bf = getItemResult.get("test_binary").getB();
		while (bf.remaining() > 0) {
			System.out.println("Vector num is " + bf.getFloat());
		}
		client.query("test_table", "test_hash = :hash", expressionAttributeValues); // 比較演算子 「:hash」は置換される変数
		client.scan("test_table");
		client.deleteItem("test_table", key);

		// execute batch operation
		vector = new ArrayList<Float>();
		int verctor_size = 1000;
		for (int i = 0; i < verctor_size; i++) {
			vector.add(new Float(0.3));
		}
		vectorByteBuffer = ByteBuffer.allocate(Float.BYTES * vector.size());
		for (Float num : vector) {
			vectorByteBuffer.putFloat(num);
		}
		vectorByteBuffer.rewind();

		// vector（サイズ1000）×500個で99秒かかる。（キャパシティーユニット：5）
		List<WriteRequest> prl = new ArrayList<WriteRequest>(); // WriteRequestは、PutRequestかDeleteRequest
		List<WriteRequest> drl = new ArrayList<WriteRequest>(); // WriteRequestは、PutRequestかDeleteRequest
		for (int i = 0; i < 10; i++) {
			// PutRequest
			HashMap<String, AttributeValue> putItem = new HashMap<String, AttributeValue>();
			putItem.put("test_hash", new AttributeValue().withS("550e8400-e29b-41d4-a716-446655440000"));
			putItem.put("test_range", new AttributeValue().withS("class_id_" + new Integer(i).toString()));
			putItem.put("vector", new AttributeValue().withB(vectorByteBuffer));
			PutRequest pr = new PutRequest(); // PutItemRequest ではないことに注意！テーブル名を設定する必要がない。
			pr.setItem(putItem);
			WriteRequest wr = new WriteRequest();
			wr.setPutRequest(pr);
			prl.add(wr);
			
			// DeleteRequest
			HashMap<String, AttributeValue> deleteKey = new HashMap<String, AttributeValue>();
			deleteKey.put("test_hash", new AttributeValue().withS("550e8400-e29b-41d4-a716-446655440000"));
			deleteKey.put("test_range", new AttributeValue().withS("class_id_" + new Integer(i).toString()));
			DeleteRequest dr = new DeleteRequest(); // PutItemRequest ではないことに注意！テーブル名を設定する必要がない。
			dr.setKey(deleteKey);
			wr = new WriteRequest();
			wr.setDeleteRequest(dr);
			drl.add(wr);
		}
		
		// PutRequest
		Map<String, List<WriteRequest>> put_items = new HashMap<String, List<WriteRequest>>(); // テーブル名、WriteRequestのリスト
		put_items.put("test_table2", prl);
		prl = new ArrayList<WriteRequest>();
		// DeleteRequest
		Map<String, List<WriteRequest>> delete_items = new HashMap<String, List<WriteRequest>>(); // テーブル名、WriteRequestのリスト
		delete_items.put("test_table2", drl);
		drl = new ArrayList<WriteRequest>();
		
		for (int i = 10; i < 20; i++) {
			// PutRequest
			HashMap<String, AttributeValue> putItem = new HashMap<String, AttributeValue>();
			putItem.put("test_hash", new AttributeValue().withS("550e8400-e29b-41d4-a716-446655440000"));
			putItem.put("test_range", new AttributeValue().withS("class_id_" + new Integer(i).toString()));
			putItem.put("vector", new AttributeValue().withB(vectorByteBuffer));
			PutRequest pr = new PutRequest(); // PutItemRequest ではないことに注意！テーブル名を設定する必要がない。
			pr.setItem(putItem);
			WriteRequest wr = new WriteRequest();
			wr.setPutRequest(pr);
			prl.add(wr);
			
			// DeleteRequest
			HashMap<String, AttributeValue> deleteKey = new HashMap<String, AttributeValue>();
			deleteKey.put("test_hash", new AttributeValue().withS("550e8400-e29b-41d4-a716-446655440000"));
			deleteKey.put("test_range", new AttributeValue().withS("class_id_" + new Integer(i).toString()));
			DeleteRequest dr = new DeleteRequest(); // PutItemRequest ではないことに注意！テーブル名を設定する必要がない。
			dr.setKey(deleteKey);
			wr = new WriteRequest();
			wr.setDeleteRequest(dr);
			drl.add(wr);
		}
		
		// PutRequest
		System.out.println("Batch Put Start.");
		//Map<String, List<WriteRequest>> put_items = new HashMap<String, List<WriteRequest>>(); // テーブル名、WriteRequestのリスト
		put_items.put("test_table", prl);
		long start = System.currentTimeMillis();
		client.batchWrite(put_items);
		System.out.println("Batch Put End");
		long end = System.currentTimeMillis();
		System.out.println("Batch Delete for " + (end - start) + "ms");

		// DeleteRequest
		System.out.println("Batch Delete Start.");
		//Map<String, List<WriteRequest>> delete_items = new HashMap<String, List<WriteRequest>>(); // テーブル名、WriteRequestのリスト
		delete_items.put("test_table", drl);
		start = System.currentTimeMillis();
		client.batchWrite(delete_items);
		System.out.println("Batch Delete End");
		end = System.currentTimeMillis();
		System.out.println("Batch Delete for " + (end - start) + "ms");
	}

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}
}