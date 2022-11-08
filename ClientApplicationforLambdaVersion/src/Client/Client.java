package Client;


import java.io.File;
import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSStaticCredentialsProvider;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;

import java.io.FileInputStream;
import java.io.IOException;
import software.amazon.awssdk.core.sync.RequestBody;
import software.amazon.awssdk.regions.Region;
import software.amazon.awssdk.services.s3.S3Client;
import software.amazon.awssdk.services.s3.model.CreateBucketRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsRequest;
import software.amazon.awssdk.services.s3.model.ListBucketsResponse;
import software.amazon.awssdk.services.s3.model.PutObjectRequest;
import software.amazon.awssdk.services.sns.SnsClient;
import software.amazon.awssdk.services.sns.model.PublishRequest;
import software.amazon.awssdk.services.sns.model.PublishResponse;
import software.amazon.awssdk.services.sns.model.SnsException;
import software.amazon.awssdk.services.sqs.model.DeleteMessageRequest;
import software.amazon.awssdk.services.sqs.model.Message;

public class Client {

  public static void main(String[] args) {
    Region region = Region.US_EAST_1;
    String topicARN = "arn:aws:sns:us-east-1:200815170578:firstTopic";

    String bucketName = "storeproductexcelfiles";
    String path = args[0];
    String filename = args[1];

    S3Client s3 = S3Client.builder().region(region).build();

    ListBucketsRequest listBucketsRequest = ListBucketsRequest.builder()
        .build();
    ListBucketsResponse listBucketResponse = s3.listBuckets(listBucketsRequest);

    if ((listBucketResponse.hasBuckets()) && (listBucketResponse.buckets()
        .stream().noneMatch(x -> x.name().equals(bucketName)))) {

      CreateBucketRequest bucketRequest = CreateBucketRequest.builder()
          .bucket(bucketName).build();

      s3.createBucket(bucketRequest);
    }

    PutObjectRequest putOb = PutObjectRequest.builder().bucket(bucketName)
        .key(filename).build();
    s3.putObject(putOb,
        RequestBody.fromBytes(getObjectFile(path + File.separator + filename)));
    try {
      SnsClient snsClient = SnsClient.builder().region(region).build();

      PublishRequest request = PublishRequest.builder()
          .message(bucketName + ";" + filename).topicArn(topicARN).build();

      PublishResponse snsResponse = snsClient.publish(request);
      System.out.println(snsResponse.messageId() + " Message sent. Status is "
          + snsResponse.sdkHttpResponse().statusCode());

    } catch (SnsException e) {
      System.err.println(e.awsErrorDetails().errorMessage());
      System.exit(1);
    }

  }


  private static byte[] getObjectFile(String filePath) {

    FileInputStream fileInputStream = null;
    byte[] bytesArray = null;

    try {
      File file = new File(filePath);
      bytesArray = new byte[(int) file.length()];
      fileInputStream = new FileInputStream(file);
      fileInputStream.read(bytesArray);

    } catch (IOException e) {
      e.printStackTrace();
    } finally {
      if (fileInputStream != null) {
        try {
          fileInputStream.close();
        } catch (IOException e) {
          e.printStackTrace();
        }
      }
    }

    return bytesArray;
  }
}