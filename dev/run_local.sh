docker build -t spark .
docker run -it -p 8888:8888 -v ${PWD}:/home/jovyan/work --name spark spark_jupyter