FROM python:3.9.1
# What should be executed after the docker image is created
RUN pip install pandas
# To which location should the file be copied
WORKDIR /app
# Copy file from current working directory to the docker image
# COPY "name in the source on our host machine" "name on the destination"
COPY pipeline.py pipeline.py
# What are the enter points and inputs to it inside the docker image
ENTRYPOINT [ "python", "pipeline.py" ]

# Example CLI input how to run this docker image
# docker run -it test:pandas 2021-01-15 test hello_world
# --> "2021-01-15", "test", "hello_world" are sys inputs to "python pipeline.py"