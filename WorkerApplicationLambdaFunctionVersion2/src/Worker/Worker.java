package Worker;

import java.io.BufferedReader;
import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.File;
import java.io.FileWriter;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.nio.charset.StandardCharsets;
import java.text.DecimalFormat;
import java.text.NumberFormat;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import org.json.JSONObject;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.services.lambda.runtime.Context;
import com.amazonaws.services.lambda.runtime.RequestHandler;
import com.amazonaws.services.lambda.runtime.events.S3Event;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.GetObjectRequest;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.ObjectMetadata;
import com.amazonaws.services.s3.model.S3Object;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.opencsv.CSVWriter;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.s3.model.PutObjectResponse;
import software.amazon.awssdk.services.sqs.SqsClient;
import software.amazon.awssdk.services.sqs.model.Message;
import software.amazon.awssdk.services.sqs.model.ReceiveMessageRequest;
import software.amazon.awssdk.services.sqs.model.SqsException;

public class Worker implements RequestHandler<String, String>  {
  static String bucketName  = "storeproductexcelfiles";
  static String bucketName1  = "storetotalprofit";
  static String bucketName2  = "totalindividualproductprofit";
 public String handleRequest(String arg, Context context) {
   
   SqsClient sqsClient = SqsClient.builder()
       .region(Region.US_EAST_1)
       .build();
String queue = "https://sqs.us-east-1.amazonaws.com/200815170578/messaging-app-queue";
processExcelFile(sqsClient, queue);
sqsClient.close();
return arg;
}

// Retrieve sqs queues messages 
 public static void processExcelFile(SqsClient sqsClient, String queueUrl) {
   try {
      ReceiveMessageRequest receiveMessageRequest = ReceiveMessageRequest.builder()
              .queueUrl(queueUrl)
              .maxNumberOfMessages(5)
              .build();
      List<Message> messages = sqsClient.receiveMessage(receiveMessageRequest).messages();
      System.out.println(messages);
      for (Message i : messages) {
        JSONObject json = new JSONObject(i.body());
        System.out.println(i);
        String receivedMessage   = json.getString("Message");
        System.out.println(json.getString("Message"));
        String []  separatedMessage  = receivedMessage .split(";") ;
        String bucketName  = separatedMessage[0];
        String fileName = separatedMessage[1];
        System.out.println(bucketName);
        System.out.println(fileName);
        
      }
   } catch (SqsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
   }

   //s3 client builder 
      final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
      // get the object list from bucket 
      ObjectListing o1 = s3.listObjects(bucketName);
     
      List<S3ObjectSummary> objects = o1.getObjectSummaries();
      for(S3ObjectSummary os: objects) {
        System.out.println("*" + os.getKey());
        S3Object object = s3.getObject(new GetObjectRequest(bucketName,os.getKey()));
        InputStream objectData = object.getObjectContent();
        try {
          BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
          String line;
          String finalProfit   = ""; 
       Double totalProfit  = 0.0;
          boolean skip = true;
          HashMap<String, List<Product>> products = new HashMap<String, List<Product>>();
          String storeName = "";
      
          while((line=reader.readLine()) != null) {
            if(skip) {
              skip = false; // Skip only the first line
              continue;
            }
            
           
           String [] tokens = line.split(";");
          
           storeName  = tokens[1];
           //calculate the total profit in one store 
              Double unitProfit = Double.parseDouble(tokens[6]);
              Integer unitQuantity  = Integer.parseInt(tokens[3]);
              Double test  = totalProfit;
             totalProfit = unitProfit * unitQuantity;
             totalProfit= totalProfit + test ;
            
             NumberFormat formatter = new DecimalFormat("0.00000000000");
            finalProfit = formatter.format(totalProfit);
            System.out.println(tokens[2]);
            System.out.println(products.containsKey(tokens[2]));
         // split the individual product information
            if(products.containsKey(tokens[2])) {
              System.out.println("conditiontrue");
              Product product = new Product();
              product.product = tokens[2];
              product.unitProfit = Double.parseDouble(tokens[6]);
              product.quantity = Integer.parseInt(tokens[3]);
         
          //   System.out.println(productExistingList);
           
             List<Product> arrlist  = new ArrayList<Product>();
             arrlist  =  products.get(tokens[2]);
             arrlist.add(product);
             products.put(tokens[2],  arrlist);
             System.out.println(arrlist);
            } else {
              System.out.println("conditiontfalse");
              Product product1 = new Product();
              product1.product = tokens[2];
              product1.unitProfit = Double.parseDouble(tokens[6]);
              product1.quantity = Integer.parseInt(tokens[3]);
              List<Product> arrlist  = new ArrayList<Product>();
              arrlist.add(product1);       
              products.put(tokens[2],  arrlist);
            }     
            
          
          }
          
           ByteArrayOutputStream stream = new ByteArrayOutputStream();
           OutputStreamWriter streamWriter = new OutputStreamWriter(stream, StandardCharsets.UTF_8);
           CSVWriter writer  =  new CSVWriter(streamWriter, ',', Character.MIN_VALUE, '"', System.lineSeparator());
          
           
           ByteArrayOutputStream streamStore = new ByteArrayOutputStream();
           OutputStreamWriter streamWriterStore = new OutputStreamWriter(streamStore, StandardCharsets.UTF_8);
           CSVWriter writerStream  =  new CSVWriter(streamWriterStore, ',', Character.MIN_VALUE, '"', System.lineSeparator());
          
           //calculate the individual product profit 
          
         // CSVWriter csvWriterProduct = new CSVWriter(new FileWriter(storeName + "productprofit" + ".csv"));
         // CSVWriter csvWriterStore = new CSVWriter(new FileWriter(storeName + ".csv"));
           writer.writeNext(new String[]{ "ProductName" , "ProductTotalProfit" , "ProductTotalQuantity"});
           writerStream.writeNext(new String[]{ "StoreName" ,  "TotalProfit"});
          for (String key : products.keySet()) {
            System.out.println(key);
            Integer totalQuantity = 0;
            Double productProfit = 0.0;
            String product ="";
            for (int i = 0; i < products.get(key).size(); i++) {
              
               product = products.get(key).get(i).product;         
             Integer quantity  = products.get(key).get(i).quantity;
             Double unitProfit  = products.get(key).get(i).unitProfit;
           
             Double test1  = productProfit;
             productProfit = unitProfit * quantity;
             
             productProfit = productProfit + test1 ;
             totalQuantity = totalQuantity + quantity;
             
             System.out.println(product + " " + productProfit + " " + totalQuantity);
            
             
            }
          //  csvWriterProduct.writeNext(new String[]{ product , String.valueOf(productProfit) , String.valueOf(totalQuantity)});
           
            writer.writeNext(new String[]{ product , String.valueOf(productProfit) , String.valueOf(totalQuantity)});
        }
          
        //  csvWriterProduct.close();
          writerStream.writeNext(new String[]{ storeName , String.valueOf(totalProfit) });
        //  csvWriterStore.close();
          writer.close();
          writerStream.close();
          ObjectMetadata meta = new ObjectMetadata();
         meta.setContentLength(stream.toByteArray().length);
         
         ObjectMetadata metaStream = new ObjectMetadata();
         metaStream.setContentLength(stream.toByteArray().length);
          try {
        //save the create excel file in the bucket 
          s3.putObject(bucketName2, storeName + "productprofit" + ".csv" , new ByteArrayInputStream(stream.toByteArray()), meta);
         s3.putObject(bucketName1, storeName ,totalProfit.toString());
          }catch(AmazonServiceException e){
           System.err.println(e.getErrorMessage());
          }
          System.out.println(products);
          System.out.println("TotalProfit" + finalProfit);
         objectData.close();
        }catch(IOException e){
          e.printStackTrace();
        }
        
        //delete the new excel file from the bucket
        try {
          s3.deleteObject(bucketName,os.getKey());
        }catch (AmazonServiceException e) {
          System.err.println(e.getErrorMessage());
          System.exit(1);
        }
      }
      
    }
}
