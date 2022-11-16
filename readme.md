# Run Client Application - 

Install Java Version 11 in the local system 

Step 1 - Open Command Prompt 
Step 2 - Go to the location of jar file of client application
Step 3 - Enter the command  -  Java -jar ClientApplication.jar "pathname" "filename"

where path name is the location of the excel file and filename is the name of the excel file need to be processed.

### Reminder  -  I used SNS client in the Client application. So AWS credential need to updated before running this app which is expired every 24 hours.

# Run Worker Application as Lambda Function - 

Step1  -  Create the jar file using the command "mvn package"
Step2  -  Upload the file to Lambda function created for the Worker(Which is named as "testWorker")


#  Run Worker application as Java application in Ec2 instance

Step 1 - Turn on the ec2 machine 
Step 2 - Install java version 11 in the selected virtual machine using the command - sudo amazon-linux-extras install java-openjdk11 with the help of putty. (Download the SSH key from AWS details and upload it to putty software and connect the putty using the public DNS for the installation).
Step 4 -  Enter "AWS Configure" in the putty command prompt of virutal machine to update the AWS  credentials
Step 5 - Upload .AWS folder with updated credential to virtual machine with the help of fileZilla software(Connect filezilla to virtual machine with the help public DNS and SSH key)
Step 6 -  Upload the packaged worker application(jar file) to virtual machine with the help of filezilla
Step 7  -  Run the application using putty with the below command - 
"Java -jar workerApplication.jar"
Step 8  -  This application is run in a infinite loop 



### Reminder  -  AWS credential need to updated before running this app which is expired every 24 hours.


# Consolidator Application  -  

Step 1 - Install java version 11
Step 2 - Run the consolidator application using the below command 
"Java -jar consolidator.jar "Date" 

Date must me given in the format "YYYY-MM-DD"

### Reminder  -  AWS credential need to updated before running this app which is expired every 24 hours.

## Reminder  -  I upload vedios which show excevution of tht application which is available in the vedios folder. 
## Two folders are there named as  -  ProjectExecutionAndResults -  to see the results of the application - Approximately equal to 10 minutes 

Also the divide the vedio into four parts

Just in case if you need a code explanation please see other folder named as "CodeExplanation - Optional Incase required" which is more than the time limit of 10 minutes.


## In folder named as SomeGeneratedExcelFiles contain the sample excel files generated from the Worker application and the Consolidator Application


## See the folder named "code" for the program of Client, Worker and Consolidator

## See  ApplicationJarFiles which contain the jar files of Client, Worker and Consolidator

## Then the report folder
