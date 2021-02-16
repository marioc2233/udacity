#Boticario Test

To star this project is easy. It's necessary to have docker and docker-compose installed

Go to project folder and execute the docker-compose builder: docker-compose -f docker-compose.yml up --build

After the build is done enter the docker: docker exec -it boticario_teste bash

Inside the docker run the script python: python3 execution_start.py; This begin to Cscript will execute all the steps necessary to complete the test;

After finished you can run the jupyter notebook: jupyter notebook --ip 0.0.0.0 --port 4000 --allow-root

Copy the addres provided in the docker and pasted in you browser. Navigate in jupyter folder and open the file Test.ipynb This file contain some select exemples for the created tables.
