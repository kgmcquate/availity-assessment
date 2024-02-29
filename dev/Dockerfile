FROM jupyter/all-spark-notebook

USER root 
RUN pip install toree

USER jovyan
RUN  jupyter toree  install --spark_home=/usr/local/spark