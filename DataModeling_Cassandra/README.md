#Data Modeling Cassandra

The purpose of this Database is to provide answear questions about user behavior in the use of the Sparkify music library.

To do this the Database was modeling to answear three distinct queries. That are shown in the Jupyter Notebook file;

To star this project is easy.
It's necessary to have docker and docker-compose installed

Go to project folder and execute yhe docker-compose builder
docker-compose -f docler-compose.yml up --build

After the build is done enther the docker: docker exec docker exec -it udacity_docker bash

Inside the docker run the script python: python3 execution_start.py
This begin to Create tables and read the files in data folder to insert on the tables

After finished you can run the jupyter notebook: jupyter notebook --ip 0.0.0.0 --port 4000 --allow-root

Copy the addres provided in the docker and pasted in you browser.
Navigate in jupyter folder and open the file Test.ipynb
This file contain the select statments that were necessary to model the tables.