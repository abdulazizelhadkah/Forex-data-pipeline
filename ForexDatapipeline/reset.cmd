@echo off

REM /!\ WARNING: RESET EVERYTHING!
REM Remove all containers/networks/volumes/images and data in db
docker-compose down
docker system prune -f
docker volume prune -f
docker network prune -f

REM Remove contents of the specified directory (equivalent to "rm -rf" in Bash)
rmdir /s /q .\mnt\postgres

REM Remove all Docker images
docker rmi -f $(docker images -a -q)
