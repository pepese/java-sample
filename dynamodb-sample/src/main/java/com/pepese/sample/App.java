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

		int item_num = 200; // QAの数
		// vector（サイズ1000）×500個で99秒かかる。（キャパシティーユニット：5）
		System.out.println("Batch Put Start.");
		long start = System.currentTimeMillis();
		Map<String, List<WriteRequest>> items = new HashMap<String, List<WriteRequest>>(); // テーブル名、WriteRequestのリスト
		List<WriteRequest> wrl = new ArrayList<WriteRequest>(); // WriteRequestは、PutRequestかDeleteRequest
		for (int i = 0; i < item_num; i++) {
			HashMap<String, AttributeValue> putItem = new HashMap<String, AttributeValue>();
			putItem.put("test_hash", new AttributeValue().withS("550e8400-e29b-41d4-a716-446655440000"));
			putItem.put("test_range", new AttributeValue().withS("class_id_" + new Integer(i).toString()));
			putItem.put("vector", new AttributeValue().withB(vectorByteBuffer));
			PutRequest pr = new PutRequest(); // PutItemRequest ではないことに注意！テーブル名を設定する必要がない。
			pr.setItem(putItem);
			WriteRequest wr = new WriteRequest();
			wr.setPutRequest(pr);
			wrl.add(wr);

//			if ((i + 1) % 25 == 0) {
//				items.put("test_table", wrl);
//				client.batchWriteCore(items);
//				System.out.println("Batch End");
//				items = new HashMap<String, List<WriteRequest>>();
//				wrl = new ArrayList<WriteRequest>();
//			}
		}
//		if (items.size() > 0) {
//			items.put("test_table", wrl);
//			client.batchWrite(items);
//			System.out.println("Batch End");
//			items = new HashMap<String, List<WriteRequest>>();
//			wrl = new ArrayList<WriteRequest>();
//		}
		items.put("test_table", wrl);
		client.batchWrite(items);
		System.out.println("Batch End");
		items = new HashMap<String, List<WriteRequest>>();
		wrl = new ArrayList<WriteRequest>();
		long end = System.currentTimeMillis();
		long batchPutTime = end - start;
		start = System.currentTimeMillis();

		System.out.println("Batch Delete Start.");
		items = new HashMap<String, List<WriteRequest>>(); // テーブル名、WriteRequestのリスト
		wrl = new ArrayList<WriteRequest>(); // WriteRequestは、PutItemRequestかDeleteItemRequest
		for (int i = 0; i < item_num; i++) {
			HashMap<String, AttributeValue> deleteKey = new HashMap<String, AttributeValue>();
			deleteKey.put("test_hash", new AttributeValue().withS("550e8400-e29b-41d4-a716-446655440000"));
			deleteKey.put("test_range", new AttributeValue().withS("class_id_" + new Integer(i).toString()));
			DeleteRequest dr = new DeleteRequest(); // PutItemRequest ではないことに注意！テーブル名を設定する必要がない。
			dr.setKey(deleteKey);
			WriteRequest wr = new WriteRequest();
			wr.setDeleteRequest(dr);
			wrl.add(wr);

//			if ((i + 1) % 25 == 0) {
//				items.put("test_table", wrl);
//				client.batchWriteCore(items);
//				System.out.println("Batch End");
//				items = new HashMap<String, List<WriteRequest>>();
//				wrl = new ArrayList<WriteRequest>();
//			}
		}
//		if (items.size() > 0) {
//			items.put("test_table", wrl);
//			client.batchWriteCore(items);
//			System.out.println("Batch End");
//		}
		items.put("test_table", wrl);
		client.batchWrite(items);
		System.out.println("Batch End");
		end = System.currentTimeMillis();
		long batchDeleteTime = end - start;
		System.out.println("Batch Delete for " + batchPutTime + "ms");
		System.out.println("Batch Delete for " + batchDeleteTime + "ms");
	}

	public static void main(String[] args) {
		SpringApplication.run(App.class, args);
	}
}