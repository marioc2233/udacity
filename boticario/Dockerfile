#Sistema Operacional
FROM ubuntu:latest
#Dependências do sistema
RUN echo 'Starting'
RUN apt-get update -y
RUN apt install -y libpq-dev python3-dev
RUN apt-get install -y postgresql-server-dev-all
RUN apt-get install -y build-essential
RUN apt-get install -y python3-dev python3-pip build-essential
#Pacotes do Python
RUN pip3 install --upgrade pip #Atualização do gerenciador de pacotes
RUN pip3 install jupyter #Instalação do pacote jupyter notebook
COPY . .
RUN pip3 install -r requirements/requirements.txt
EXPOSE 4000
