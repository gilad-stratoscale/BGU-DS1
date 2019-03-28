package com.amazonaws.samples;
import java.util.Base64;
/*
 * Copyright 2010-2017 Amazon.com, Inc. or its affiliates. All Rights Reserved.
 *
 * Licensed under the Apache License, Version 2.0 (the "License").
 * You may not use this file except in compliance with the License.
 * A copy of the License is located at
 *
 *  http://aws.amazon.com/apache2.0
 *
 * or in the "license" file accompanying this file. This file is distributed
 * on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either
 * express or implied. See the License for the specific language governing
 * permissions and limitations under the License.
 */
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.io.File;
import java.nio.file.Path;
import java.nio.file.Paths;

import com.amazonaws.AmazonClientException;
import com.amazonaws.AmazonServiceException;
import com.amazonaws.auth.profile.ProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.ec2.AmazonEC2;
import com.amazonaws.services.ec2.AmazonEC2ClientBuilder;
import com.amazonaws.services.ec2.model.CreateTagsRequest;
import com.amazonaws.services.ec2.model.DescribeAvailabilityZonesResult;
import com.amazonaws.services.ec2.model.DescribeInstancesResult;
import com.amazonaws.services.ec2.model.Instance;
import com.amazonaws.services.ec2.model.InstanceType;
import com.amazonaws.services.ec2.model.Reservation;
import com.amazonaws.services.ec2.model.RunInstancesRequest;
import com.amazonaws.services.ec2.model.StartInstancesRequest;
import com.amazonaws.services.ec2.model.Tag;
import com.amazonaws.services.ec2.model.InstanceState;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3ClientBuilder;
import com.amazonaws.services.s3.model.AmazonS3Exception;
import com.amazonaws.services.s3.model.Bucket;
import com.amazonaws.services.s3.model.CannedAccessControlList;
import com.amazonaws.services.s3.model.ObjectListing;
import com.amazonaws.services.s3.model.PutObjectRequest;
import com.amazonaws.services.s3.model.S3ObjectSummary;
import com.amazonaws.services.sqs.AmazonSQS;
import com.amazonaws.services.sqs.AmazonSQSClientBuilder;
import com.amazonaws.services.sqs.model.CreateQueueRequest;
import com.amazonaws.services.sqs.model.SendMessageRequest;

/**
 * Welcome to your new AWS Java SDK based project!
 *
 * This class is meant as a starting point for your console-based application that
 * makes one or more calls to the AWS services supported by the Java SDK, such as EC2,
 * SimpleDB, and S3.
 *
 * In order to use the services in this sample, you need:
 *
 *  - A valid Amazon Web Services account. You can register for AWS at:
 *       https://aws-portal.amazon.com/gp/aws/developer/registration/index.html
 *
 *  - Your account's Access Key ID and Secret Access Key:
 *       http://aws.amazon.com/security-credentials
 *
 *  - A subscription to Amazon EC2. You can sign up for EC2 at:
 *       http://aws.amazon.com/ec2/
 *
 *  - A subscription to Amazon SimpleDB. You can sign up for Simple DB at:
 *       http://aws.amazon.com/simpledb/
 *
 *  - A subscription to Amazon S3. You can sign up for S3 at:
 *       http://aws.amazon.com/s3/
 */
public class AwsConsoleApp {
	
    /*
     * Before running the code:
     *      Fill in your AWS access credentials in the provided credentials
     *      file template, and be sure to move the file to the default location
     *      (/home/giladsever/.aws/credentials) where the sample code will load the
     *      credentials from.
     *      https://console.aws.amazon.com/iam/home?#security_credential
     *
     * WARNING:
     *      To avoid accidental leakage of your credentials, DO NOT keep
     *      the credentials file in your source directory.
     */

    static AmazonEC2      ec2;
    static AmazonS3       s3;
    static AmazonSQS      sqs; 
    static int INSTANCE_TERMINATED_STATUS_CODE = 48;
    static int INSTANCE_RUNNING_STATUS_CODE = 12;
    
    private static Bucket s3_bucket;
    private static String s3_key_name;
    private static String sqs_queue_url;

    /**
     * The only information needed to create a client are security credentials
     * consisting of the AWS Access Key ID and Secret Access Key. All other
     * configuration, such as the service endpoints, are performed
     * automatically. Client parameters, such as proxies, can be specified in an
     * optional ClientConfiguration object when constructing a client.
     *
     * @see com.amazonaws.auth.BasicAWSCredentials
     * @see com.amazonaws.auth.PropertiesCredentials
     * @see com.amazonaws.ClientConfiguration
     */
    
    private static void upload_file(String file_path) throws Exception {
    	/*
    	 * Create a new bucket in S3
    	 * Upload to S3 the file received from the user
    	 */
    	
    	String bucket_name = s3_bucket.getName();
    	s3_key_name = Paths.get(file_path).getFileName().toString();
    	
    	System.out.format("Uploading %s to S3 bucket %s...\n", file_path, bucket_name);
    	try {
    		s3.putObject(new PutObjectRequest(bucket_name, s3_key_name, new File(file_path))
    				.withCannedAcl(CannedAccessControlList.PublicRead));
    	} catch (AmazonServiceException e) {
    	    System.err.println(e.getErrorMessage());
    	    System.exit(1);
    	}
    }
    private static void init_aws_clients_and_credentials() throws Exception {

        /*
         * The ProfileCredentialsProvider will return your [gilad1]
         * credential profile by reading from the credentials file located at
         * (/home/giladsever/.aws/credentials).
         * 
         * Initialize amazon clients - EC2, S3, SQS
         */
        ProfileCredentialsProvider credentialsProvider = new ProfileCredentialsProvider();
        try {
            credentialsProvider.getCredentials();
        } catch (Exception e) {
            throw new AmazonClientException(
                    "Cannot load the credentials from the credential profiles file. " +
                    "Please make sure that your credentials file is at the correct " +
                    "location (/home/giladsever/.aws/credentials), and is in valid format.",
                    e);
        }
        ec2 = AmazonEC2ClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion("us-east-1")
            .build();
        sqs = AmazonSQSClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion("us-east-1")
            .build();
        s3  = AmazonS3ClientBuilder.standard()
            .withCredentials(credentialsProvider)
            .withRegion("us-east-1")
            .build();
    }

    private static void create_aws_resources() throws Exception {
    	System.out.println("Creating a new S3 bucket\n");
    	try {
    		s3_bucket = s3.createBucket("gilad.and.shohams.bucket");
    	} catch (AmazonS3Exception e) {
    		System.err.println(e.getErrorMessage());
    	}
    	
    	System.out.println("Creating a new SQS queue\n");
        CreateQueueRequest createQueueRequest = new CreateQueueRequest("gilad_and_shohams_queue");
        sqs_queue_url = sqs.createQueue(createQueueRequest).getQueueUrl();
    }
    public static void main(String[] args) throws Exception {

        System.out.println("===========================================");
        System.out.println("Local Application Started");
        System.out.println("===========================================");

        init_aws_clients_and_credentials();
//        create_aws_resources();
        
        // Upload input file to S3
        String input_file_name = args[0];
        File file = new File(input_file_name);
        String file_path = file.getAbsolutePath();
//        upload_file(file_path);
        
		// Send a message stating the location of the file
        send_file_location();
        
        // Start the manager
        start_manager();
            
//        	RunInstancesRequest runInstancesRequest = new RunInstancesRequest()
//        		    .withInstanceType(InstanceType.T1Micro)
//        		    .withImageId("ami-8c1fece5")
//        		    .withMinCount(1)
//        		    .withMaxCount(1);
////        		    .withUserData(Base64.encodeBase64String(myUserData.getBytes()))
//
//        	
//        	Instance instance = ec2.runInstances(runInstancesRequest).getReservation().getInstances().get(0);
//        	String instanceId = instance.getInstanceId();
//     
//            // Setting up the tags for the instance
//            CreateTagsRequest createTagsRequest = new CreateTagsRequest()
//                    .withResources(instanceId)
//                    .withTags(new Tag("Name", "manager"));
//            ec2.createTags(createTagsRequest);
//     
//            // Starting the Instance
//            StartInstancesRequest startInstancesRequest = new StartInstancesRequest().withInstanceIds(instanceId);       
//            ec2.startInstances(startInstancesRequest);
     
        	
//            System.out.println("Launch instance: " + instance);
        }
	private static void start_manager() {
		try {
        	// Find the manager instance
        	List<Reservation> reservations = ec2.describeInstances().getReservations();
            Set<Instance> instances = new HashSet<Instance>();

            for (Reservation reservation : reservations) {
            	System.out.println(reservation.getInstances().get(0).getTags());
                instances.addAll(reservation.getInstances());
            }
            for (Instance instance : instances) {
            	if (!isInstanceTerminated(instance) && isInstanceManager(instance))
            		// If the manager is active, continue
            		if (isInstanceRunning(instance))
            			return;
            		// Otherwise, start the manager
            		else {
            			StartInstancesRequest startInstancesRequest = new StartInstancesRequest()
            				    .withInstanceIds(instance.getInstanceId());
            			ec2.startInstances(startInstancesRequest);
            		}
            }
        } catch (AmazonServiceException ase) {
            System.out.println("Caught Exception: " + ase.getMessage());
            System.out.println("Reponse Status Code: " + ase.getStatusCode());
            System.out.println("Error Code: " + ase.getErrorCode());
            System.out.println("Request ID: " + ase.getRequestId());
    }   
        /*
         * Amazon EC2
         *
         * The AWS EC2 client allows you to create, delete, and administer
         * instances programmatically.
         *
         * In this sample, we use an EC2 client to get a list of all the
         * availability zones, and all instances sorted by reservation id.
         */
        try {
            DescribeAvailabilityZonesResult availabilityZonesResult = ec2.describeAvailabilityZones();
            System.out.println("You have access to " + availabilityZonesResult.getAvailabilityZones().size() +
                    " Availability Zones.");

            List<Reservation> reservations = ec2.describeInstances().getReservations();
            Set<Instance> instances = new HashSet<Instance>();
            Set<Instance> terminated_instances = new HashSet<Instance>();

            for (Reservation reservation : reservations) {
//            	System.out.println(reservation.getInstances().get(0).getTags());
            	for (Instance instance : reservation.getInstances()) {
            		if (isInstanceTerminated(instance))
            			instances.add(instance);
            		else
            			terminated_instances.add(instance);
            	}
            }
//            System.out.println("Instaces list: " + instances);
            System.out.println("You have " + instances.size() + " Amazon EC2 instance(s) running.");
            System.out.println("You have " + terminated_instances.size() + " Amazon EC2 instance(s) terminated.");
        } catch (AmazonServiceException ase) {
                System.out.println("Caught Exception: " + ase.getMessage());
                System.out.println("Reponse Status Code: " + ase.getStatusCode());
                System.out.println("Error Code: " + ase.getErrorCode());
                System.out.println("Request ID: " + ase.getRequestId());
        }

                /*
         * Amazon S3
         *
         * The AWS S3 client allows you to manage buckets and programmatically
         * put and get objects to those buckets.
         *
         * In this sample, we use an S3 client to iterate over all the buckets
         * owned by the current user, and all the object metadata in each
         * bucket, to obtain a total object and space usage count. This is done
         * without ever actually downloading a single object -- the requests
         * work with object metadata only.
         */
        try {
            List<Bucket> buckets = s3.listBuckets();

            long totalSize  = 0;
            int  totalItems = 0;
            for (Bucket bucket : buckets) {
                /*
                 * In order to save bandwidth, an S3 object listing does not
                 * contain every object in the bucket; after a certain point the
                 * S3ObjectListing is truncated, and further pages must be
                 * obtained with the AmazonS3Client.listNextBatchOfObjects()
                 * method.
                 */
                ObjectListing objects = s3.listObjects(bucket.getName());
                do {
                    for (S3ObjectSummary objectSummary : objects.getObjectSummaries()) {
                        totalSize += objectSummary.getSize();
                        totalItems++;
                    }
                    objects = s3.listNextBatchOfObjects(objects);
                } while (objects.isTruncated());
            }

            System.out.println("You have " + buckets.size() + " Amazon S3 bucket(s), " +
                    "containing " + totalItems + " objects with a total size of " + totalSize + " bytes.");
        } catch (AmazonServiceException ase) {
            /*
             * AmazonServiceExceptions represent an error response from an AWS
             * services, i.e. your request made it to AWS, but the AWS service
             * either found it invalid or encountered an error trying to execute
             * it.
             */
            System.out.println("Error Message:    " + ase.getMessage());
            System.out.println("HTTP Status Code: " + ase.getStatusCode());
            System.out.println("AWS Error Code:   " + ase.getErrorCode());
            System.out.println("Error Type:       " + ase.getErrorType());
            System.out.println("Request ID:       " + ase.getRequestId());
        } catch (AmazonClientException ace) {
            /*
             * AmazonClientExceptions represent an error that occurred inside
             * the client on the local host, either while trying to send the
             * request to AWS or interpret the response. For example, if no
             * network connection is available, the client won't be able to
             * connect to AWS to execute a request and will throw an
             * AmazonClientException.
             */
            System.out.println("Error Message: " + ace.getMessage());
        }
    }
	private static boolean isInstanceManager(Instance instance) {
		return instance.getTags().contains(new Tag("Name", "manager"));
	}
	private static boolean isInstanceTerminated(Instance instance) {
		return instance.getState().getCode() == INSTANCE_TERMINATED_STATUS_CODE;
	}
	private static boolean isInstanceRunning(Instance instance) {
		return instance.getState().getCode() == INSTANCE_RUNNING_STATUS_CODE;
	}
	
	private static void send_file_location() {
        sqs.sendMessage(new SendMessageRequest(sqs_queue_url, s3_key_name));
	}
	
}
