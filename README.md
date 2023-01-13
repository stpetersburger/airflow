# airflow
/airflow repo is used to keep the configuration of airflow >2


##1. To create the customized image for airflow services:

docker build --tag stpetersburger/airflow:latest .

Services are as per the official airflow doc: postgres, redis, webserver, scheduler, worker, flower
Each is run on a separate container

webserver is accessible on localhost:8080
flower - localhost:5555


##2. To run the containers (locally on MacOs - docker desktop needs to be available and run):

docker-compose -f docker-compose.yaml up -d

##3. To stop the containers deployment:

docker-compose -f docker-compose.yaml