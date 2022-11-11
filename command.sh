docker image rm -f 
docker ps
docker images
docker exec -it  /bin/bash
docker build -t receiver:latest .
docker build -t storage:latest .
cd ../storage
cd ../receiver
cd ../deployment
docker compose up -d
docker compose down
docker logs