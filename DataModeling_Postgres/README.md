#Data Modeling Postgres

The First Project of Data Modeling a DataBase in Postgres

The purpose of this Database is to provide insights of user and songs availabe in the startup Sparkify. This Database is structured to give a quick response for users taste in music and how much time they spent listening to those songs. 

The Database is designed based on a fact table composed by diferent id's that correlate tho its own table. With that its easy to ind a specific user and with songs they listened to, how much time they spent listened, and other useful information.

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
This file contain some select exemples in each table.