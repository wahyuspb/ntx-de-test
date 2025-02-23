## Context
At NTX, we are highly reliant on Docker. It feels like our lives, especially as Data Engineers, revolve around it.

Inside the api/ folder, there's a Python script which serves as a dummy API that can be accessed via its /predict endpoint. Meanwhile, inside the etl/ folder, there's another Python script which acts as an ETL program that calls the endpoint provided by the api/ service.

Both folders are considered as separate services that we want to "dockerize". Each folder contains its own requirements.txt and a Dockerfile.

## Instructions
For this challenge, your task is to complete the Dockerfile scripts located inside both the api/ and etl/ folders. In addition, you'll need to complete the docker-compose.yml located in the Soal 2 - Docker Case folder. Once you've filled in the necessary details, execute the bash command docker-compose up --build from within the Soal 2 - Docker Case folder. Ensure the completion of the scripts such that the output log resembles the one shown in /output.jpg.

If your computer doesn't have access to Docker, don't worry. Just populate the three files based on your instincts and knowledge. It's advised to include comments in each Dockerfile and docker-compose.yml to describe the reasoning behind each line you've added.

Thank you and good luck!