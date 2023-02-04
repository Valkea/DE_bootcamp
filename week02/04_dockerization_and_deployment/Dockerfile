FROM prefecthq/prefect:2.7.7-python3.9 

COPY docker-requirements.txt .

RUN pip install -r docker-requirements.txt --trusted-host pypi.python.org --no-cache-dir

# copy the current folder with scripts & data
COPY ./flow /opt/prefect/flow
COPY ./data /opt/prefect/data

# No need to add ENTRYPOINT or CMD