## Run
local:
docker-compose up zookeeper kafka
run app

container:
build app
docker-compose up --build


## Swagger
http://localhost:8080/swagger-ui/index.html