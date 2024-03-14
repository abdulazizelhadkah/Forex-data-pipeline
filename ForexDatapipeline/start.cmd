REM Build the base images from which the Dockerfiles are based
docker build -t hadoop-base docker\hadoop\hadoop-base
docker build -t hive-base docker\hive\hive-base
docker build -t spark-base docker\spark\spark-base

REM Startup all the containers at once
docker-compose up -d --build
