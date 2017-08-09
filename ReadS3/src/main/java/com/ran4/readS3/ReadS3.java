/*
 * To change this license header, choose License Headers in Project Properties.
 * To change this template file, choose Tools | Templates
 * and open the template in the editor.
 */
package com.ran4.readS3;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.regions.Region;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDB;
import com.amazonaws.services.dynamodbv2.AmazonDynamoDBClientBuilder;
import com.amazonaws.services.dynamodbv2.document.DynamoDB;
import com.amazonaws.services.dynamodbv2.document.Item;
import com.amazonaws.services.dynamodbv2.document.PutItemOutcome;
import com.amazonaws.services.dynamodbv2.document.Table;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.S3Object;
import java.util.ArrayList;
import java.util.Scanner;
import java.util.logging.Level;
import java.util.logging.Logger;

/**
 *
 * @author Robert
 */
public class ReadS3 {

    //holds json objects
    static ArrayList jsonArray = new ArrayList();
    //declare client, database and get the table
    static AmazonDynamoDB client = AmazonDynamoDBClientBuilder.standard()
            .withRegion(Regions.US_EAST_1).build();
    static DynamoDB dynamoDB = new DynamoDB(client);
    static Table table = dynamoDB.getTable("TestCrawl");

    public static void main(String[] args) {
        //make a S3 object and set the region to my aws region
        BasicAWSCredentials creds
                = new BasicAWSCredentials("AKIAJF5HSCJSLPGQTJBQ", "EAWJg2jkfwxq3MXrqoAB+laMF3/w/Ve1eWIFcSk+");
        AmazonS3 myBucket = AmazonS3ClientBuilder.standard()
                .withRegion(Regions.US_EAST_1)
                .withCredentials(new AWSStaticCredentialsProvider(creds))
                .build();
        //make name of my bucket I am using and the key
        String bucketName;
        String key = "Redirecting.txt";

        System.out.println("===========================================");
        System.out.println("Getting Started with Amazon S3");
        System.out.println("===========================================\n");
        //get name of the bucket user wants. store in holder then add to bucketName
        System.out.println("enter name of bucket");
        Scanner sc = new Scanner(System.in);
        bucketName = sc.nextLine();
        //make a default value for bucketName : ran4
        if (bucketName.equals("")) {
            bucketName = "ran4";
            System.out.println("You didn't choose a bucket. Using " + bucketName);
        } else {
            System.out.println("You are looking for the bucket called " + bucketName);
        }

        try {
            System.out.println("Searching for bucket " + bucketName);
            boolean foundBucket = false;
            for (Bucket bucket : myBucket.listBuckets()) {
                String getBucketName = bucket.getName();
                if (getBucketName.equals(bucketName)) {
                    foundBucket = true;
                }
            }
            if (foundBucket == false) {
                System.err.println("+++++++++++++++++++++++++++++++++++++++");
                System.err.println("No bucket by the name of " + bucketName);
                System.err.println("+++++++++++++++++++++++++++++++++++++++");
                System.exit(1);
            }
            System.out.println("Name of file you want to access:");
            Scanner fileName = new Scanner(System.in);
            String fName = fileName.nextLine();
            if (fName.equals("")) {
                fName = "Redirecting.txt";
                System.out.println("You didn't choose a filename. Using " + fName);
            } else {
                System.out.println("You are looking for the file called " + fName);
            }
            System.out.println("Looking for file called " + fName);
            S3Object object = myBucket.getObject(new GetObjectRequest(bucketName, key));
            System.out.println("Content-Type: " + object.getObjectMetadata().getContentType());
            displayTextInputStream(object.getObjectContent());
        } catch (AmazonServiceException ase) {
            System.out.println("Caught an AmazonServiceException, which means your request made it "
                    + "to Amazon S3, but was rejected with an error response for some reason.");
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            System.out.println("Caught an AmazonClientException, which means the client encountered "
                    + "a serious internal problem while trying to communicate with S3, "
                    + "such as not being able to access the network.");
            System.out.println("Error Message: " + ace.getMessage());
        } catch (IOException ex) {
            Logger.getLogger(ReadS3.class.getName()).log(Level.SEVERE, null, ex);
        }

    }

    private static void displayTextInputStream(InputStream input) throws IOException {
        BufferedReader reader = new BufferedReader(new InputStreamReader(input));

        while (true) {
            String line = reader.readLine();
            if (line == null) {
                break;
            }

            String[] array = line.split(":");
            jsonArray.clear();
            for (String s : array) {
                String[] temp = s.split(",");
                for(String t : temp) {
                    jsonArray.add(t);
                }
            }
                String primaryKey = jsonArray.get(9) + ":" + jsonArray.get(10);
                String timestamp = jsonArray.get(5).toString();
                String urlKey = jsonArray.get(1) + "," + jsonArray.get(2) + "," + jsonArray.get(3);
                Item item = new Item()
                    .withPrimaryKey("url", primaryKey)
                    .withString("timestamp", timestamp)
                    .withString("urlkey", urlKey)
                    .withString("status", jsonArray.get(7).toString())
                    .withString("filename", jsonArray.get(12).toString())
                    .withString("length", jsonArray.get(14).toString())
                    .withString("mime", jsonArray.get(16).toString())
                    .withString("mime-detected", jsonArray.get(18).toString())
                    .withString("offset", jsonArray.get(20).toString())
                    .withString("digest", jsonArray.get(22).toString());
                PutItemOutcome outcome = table.putItem(item);

            }

        }
        /*
        int size = jsonArray.size();
        for(int i =0; i <size; i++) {
            System.out.println("**********************************************");
            System.out.println("**********************************************");
            System.out.println(jsonArray.get(i));
            System.out.println("**********************************************");
            System.out.println("**********************************************");
        }
         */
    }

}
