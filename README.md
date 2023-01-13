# Airflow
the repo is used to keep the configuration of airflow >2 (2.2.4)


###1. To create the customized image for airflow services:

docker build --tag stpetersburger/airflow:latest .

Services are as per the official airflow doc: postgres, redis, webserver, scheduler, worker, flower
Each is run on a separate container

webserver is accessible on localhost:8080
flower - localhost:5555

####Project description

.env file has the assigned values to the environmental variables of the airflow service
it is used on docker compose stage in order to make testing of the new features simpler

script/docker-entrypoint.sh has the additional functionality to calculate additional variables
as well as rise the airflow services within their containers

requirements.txt is used to manage the dependencies, needed for the airflow to manage tasks assigned

as of now /dags folder is mounted to the local path, where docker desktop is up
dags put there are available on the webserver to be tested

prod deployment requires additional github repo mounted to a cloud storage in order to not redeploy
airflow containers on a new dag (task) to develop and run

/pyproject consists of .py scripts to run within dags or to serve the execution of those scripts
an overall concept: from a datasource to the datalake (datawarehouse)

###2. To run the containers (locally on MacOs - docker desktop needs to be available and run):

docker-compose -f docker-compose.yaml up -d

###3. To stop the containers deployment:

docker-compose -f docker-compose.yaml