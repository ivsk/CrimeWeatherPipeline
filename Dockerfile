FROM apache/airflow:2.2.3-python3.7
RUN pip install sodapy
RUN pip install -U jupyter-core --user
RUN pip install -U jupyter --user
RUN chmod -R 775 /home/airflow/.local/share/jupyter
RUN pip install google-cloud-bigquery[bqstorage,pandas]