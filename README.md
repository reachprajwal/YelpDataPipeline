# YelpDataPipeline

The [architecture](https://github.com/reachprajwal/YelpDataPipeline/blob/master/architecturalDesign.jpg) design contains 5 dockers in total. 2 are pre-built images and 3 are built using docker-compose .

## Docker deatails

**Pre-built dockers:**

1. Zookeeper:
This is a simple docker container having only the zookeeper on-board.
   - Uses:
        - It is used to keep the kafka broker information for data communication
        - It is used to store the off-set for the kafka consumer
   - Image pulled from the dockerhub : zookeeper:3.4
   
2. Kafka:
This is used to create a kafka broker which is used as a producer, consumer or both (like int the current case)
   - Uses:
     - We use the same kafka broker to produce data and also consume the data
     - It provides a form of a placeholder for the data to be consumed and produced
   - Image pulled from the dockerhub : ches/kafka


**Docker files built using docker-compose:**

The dockerfiles are available [in here](https://github.com/reachprajwal/YelpDataPipeline/tree/master/Dockerfiles)

1. Dockerfile1
Details
   - The first docker has the following technologies available:
        - Kafka Jars
        - Java 8
   - The data copied into the docker:
        - We'll need to put the [kafkaexamples](https://github.com/reachprajwal/YelpDataPipeline/tree/master/kafkaexamples) code into the docker
        - We'll also need to copy the data csv files (not included in the repo as kaggle requires you to approve with an agreement to download the data files)
   - Role of the current docker in the whole process:
        - It will only need to run the producers which will all individually publish the data into seperate topics into the port 9092
        - So the program reads the csv files and publishes the data into the topic 
            
2. Dockerfile2
Detials
   - The second docker has the following technologies available:
        - Kafka Jars
        - Java 8
        - Spark
        - Connectoion to the docker volume
   - The data copied into the docker:
        - We'll need to put the [kafkaexamples](https://github.com/reachprajwal/YelpDataPipeline/tree/master/kafkaexamples) code into the docker
        - In the current implementation I've copied the current spark extracted files into the docker, which are executable files
   - Role of the docker in the whole process:
        - It firstly will be used to consume the data on the particular kafka topics and create files out of the data consumed from port 9092
        - It will then be used to execute the [ETL](https://github.com/reachprajwal/YelpDataPipeline/blob/master/src/main/scala/yelpETL/yelpETL.scala) process. This will be cleansing the whole dataset from most errors found in the data.
        - After each of the files have gone through the ETL process, then the files are saved as csv files in the docker volume for the last docker to access the data
            
3. Dockerfile3
Details
   - The third docker has the following technologies available:
        - Python 3.5
        - Pandas, Numpy, Matplotlib
        - Django
        - Connection to the docker volume
   - The data copied into the docker:
        - This docker will have the django [app](https://github.com/reachprajwal/YelpDataPipeline/tree/master/yelpApp) which is used to host a webapplication to show the charts generated from the final gold-data
   - Roles of the docker int he process:
        - It is mainly used to perform analysis on the data to determine the business patterns and so on
        - It can be used to determine the future crowd-flow into a particular business depending on previous data (available in the data-set)
            
## Work-flow details

Now we shall see the procedure required to run the whole project end to end manually:

1. First we up the zookeeper with following command : docker run -d --name zookeeper --network kafka-net zookeeper:3.4 (you will need to create a user defined network for the zookeeper to connect to for communication, here I've assumed you've created a docker network called kafka-net)

2. Second we need to up the kafka broker as it provides the platform to store the topics data with the following command : docker run -d --name kafka --network kafka-net --env ZOOKEEPER_IP=zookeeper ches/kafka

3. Now we have a network created between the zookeeper and the kafka-broker 

4. We now up the custom docker1, keep in mind that the docker needs to have access to the zookeeper and kafka-broker port so include the following options while you run the docker : --network kafka-net --env ZOOKEEPER_IP=zookeeper
    
5. We now up the custom docker2, keep in mind to add the network options for it to work

6. Now run the producers from the current docker, this will write data into its individual topics in the kafka-broker located in the pre-built docker from step 2

7. Now here we run the consumers for kafka, this will pull all the data from the kafka-broker topics and write them into the file location "/home"

8. Next we run the spark job for the ETL process, this will cleanse the data and write all of the data into the docker volume with the path "/vol"

9. At the last step we perfrom required process to determine different aspects of the data to observer our findings of the data-set. For example I've extracted the [Businesses across the different states](https://github.com/reachprajwal/YelpDataPipeline/blob/master/yelpApp/charts/media/businesses.png) and the [varying number of checkins across the different days of the week](https://github.com/reachprajwal/YelpDataPipeline/blob/master/yelpApp/charts/media/checkings.png). This is two of the many scenarios we can address.

10. The final findings and observations and findings are represented using a django applications, which currently has only 2 images, mentioned in the previous step.

## Future Work:

With the need create more charts, I've been looking up kernels on kaggle to understand using seaborn and matplotlib better

Develop a full-fledge envirnment with jupyter notebooks for analysis and Django app for chart description and meaning
