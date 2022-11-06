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
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map.Entry;
import org.json.JSONObject;
import com.amazonaws.AmazonServiceException;
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

public class Worker {
 static String bucketName  = "totalindividualproductprofit";
 static String bucketName1  = "storetotalprofit";
 public static void main(String [] arg ) throws IOException {
   
//s3 client builder 
   final AmazonS3 s3 = AmazonS3ClientBuilder.defaultClient();
   // get the object list from bucket 
   
   //calculate the total profit of all store and find the least profit one and most profit one 
   ByteArrayOutputStream streamStore = new ByteArrayOutputStream();
   OutputStreamWriter streamWriterStore = new OutputStreamWriter(streamStore, StandardCharsets.UTF_8);
   CSVWriter writerStream  =  new CSVWriter(streamWriterStore, ',', Character.MIN_VALUE, '"', System.lineSeparator());
   ObjectListing o2 = s3.listObjects(bucketName1);  
   List<S3ObjectSummary> objectsStores = o2.getObjectSummaries();
   HashMap<String, Double> totalProfitoFStores = new HashMap<String,Double>();
   for(S3ObjectSummary os1: objectsStores) {
     S3Object object1 = s3.getObject(new GetObjectRequest(bucketName1,os1.getKey()));
     System.out.println(os1.getKey());
   
       BufferedReader reader1 = new BufferedReader(new InputStreamReader(object1.getObjectContent()));
       String storeTotalProfit= reader1.readLine() ;
       totalProfitoFStores.put(os1.getKey(),Double.parseDouble(storeTotalProfit)) ;
   }
   
   Double allStoresTotalProfit = 0.0 ;
   String storeMinimumProfit = "";
   String storeMaximumProfit = "" ;
   Double minimumProfit = 0.0 ;
   Double maximumProfit = 0.0 ;
   Boolean flag = true;
   for (String key : totalProfitoFStores.keySet()) {
     if(flag) {
       minimumProfit = totalProfitoFStores.get(key);
       maximumProfit = totalProfitoFStores.get(key);
       flag = false ;   
     }
     allStoresTotalProfit = allStoresTotalProfit +  totalProfitoFStores.get(key);
     if(totalProfitoFStores.get(key)  < minimumProfit  )
     {
       minimumProfit = totalProfitoFStores.get(key) ;
     }
     
     if(maximumProfit  < totalProfitoFStores.get(key) )
     {
       maximumProfit = totalProfitoFStores.get(key) ;
     }    
     
   }
   for(Entry<String, Double> entry: totalProfitoFStores.entrySet()) {

     // if give value is equal to value from entry
     // print the corresponding key 
     if(entry.getValue() ==  minimumProfit) {
       System.out.println("The key for value " + minimumProfit + " is " + entry.getKey());
       storeMinimumProfit =  entry.getKey() ;
       
     }
     
     if(entry.getValue() ==  maximumProfit) {
       System.out.println("The key for value " + minimumProfit + " is " + entry.getKey());
       storeMaximumProfit = entry.getKey() ; 
     }
   }
   
   System.out.println("djkfsjdfhsd"+ totalProfitoFStores);
   System.out.println("djkfsjdfhsd"+ allStoresTotalProfit);
   writerStream.writeNext(new String[]{ "All Store Total Profit" ,allStoresTotalProfit.toString() });
   writerStream.writeNext(new String[]{ "Store with maximum profit" ,storeMaximumProfit });
   writerStream.writeNext(new String[]{ "Store with minimum profit" ,storeMinimumProfit });
   //calculate the total profit of individual products
   ObjectListing o1 = s3.listObjects(bucketName);
  
   List<S3ObjectSummary> objects = o1.getObjectSummaries();
   HashMap<String, List<Product>> products = new HashMap<String, List<Product>>();
  
   for(S3ObjectSummary os: objects) {
     System.out.println("*" + os.getKey());
     S3Object object = s3.getObject(new GetObjectRequest(bucketName,os.getKey()));
     System.out.println(object);
   
   
       BufferedReader reader = new BufferedReader(new InputStreamReader(object.getObjectContent()));
       String line;
       boolean skip = true;
       
       try {
        System.out.println(reader.readLine());
        while((line=reader.readLine()) != null) {
          if(skip) {
            skip = false; // Skip only the first line
            continue;           
          }
         
          String [] productDetails = line.split(",");
          
          //individual product profit segregation from all the stores 
          if(products.containsKey(productDetails[0])) {
            Product product = new Product();
            product.product = productDetails[0];
            product.totalProfit = Double.parseDouble(productDetails[1]);
            product.totalQuantity = Integer.parseInt(productDetails[2]);
       
        //   System.out.println(productExistingList);
         
           List<Product> arrlist  = new ArrayList<Product>();
           arrlist  =  products.get(productDetails[0]);
           arrlist.add(product);
           products.put(productDetails[0],  arrlist);
          }else {
            Product product1 = new Product();
            product1.product = productDetails[0];
            product1.totalProfit = Double.parseDouble(productDetails[1]);
            product1.totalQuantity = Integer.parseInt(productDetails[2]);
            List<Product> arrlist  = new ArrayList<Product>();
            arrlist.add(product1);       
            products.put(productDetails[0],  arrlist);
          }    
          
         // System.out.println(products);
          
        }       
      } catch (IOException e) {
        // TODO Auto-generated catch block
        e.printStackTrace();
      }
 }
   
   writerStream.writeNext(new String[]{ "ProductName" , "ProductTotalProfit" , "ProductTotalQuantity"});
   for (String key : products.keySet()) {
     
     Integer totalQuantityAllStores = 0 ;
     Double totalProfitAllStores = 0.0 ;
     String product ="" ;
     for (int i = 0; i < products.get(key).size(); i++) {
        product = products.get(key).get(i).product;         
       Integer totalQantity  = products.get(key).get(i).totalQuantity;
       Double totalProfit  = products.get(key).get(i).totalProfit;
       totalQuantityAllStores = totalQantity +  totalQuantityAllStores ;
       totalProfitAllStores = totalProfit +  totalProfitAllStores;
      
       
     }
     writerStream.writeNext(new String[]{ product , String.valueOf(totalProfitAllStores) , String.valueOf(totalQuantityAllStores)});
    // System.out.println(product + "######" +  totalQuantityAllStores + "  ############## "  +   totalProfitAllStores ); 
   }
   writerStream.close(); 
   ObjectMetadata meta = new ObjectMetadata();
   meta.setContentLength(streamStore.toByteArray().length);
   try {
     Calendar today = Calendar.getInstance();
     //save the create excel file in the bucket 
       s3.putObject("finalconsolidatoroutput", today.getTime() + "productprofit" + ".csv" , new ByteArrayInputStream(streamStore.toByteArray()), meta);
 
       }catch(AmazonServiceException e){
        System.err.println(e.getErrorMessage());
       }
}
}
