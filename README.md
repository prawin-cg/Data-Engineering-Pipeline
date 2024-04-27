# Data Engineering Pipeline
## Execution Instruction

### Requirements
- Docker CLI/Docker Desktop
- Linux based OS (Debian/Ubuntu/MacOS/CentOS)
- Disk Space 50 Gi
- Memory 8 Gi

### Execution Steps
    docker build -t datapipeline:1.0 --no-cache .
    docker images # to check if the image has been built succesfully
    docker run datapipeline:1.0
    # pipeline execution
    docker ps -a # to view the containers
    docker cp <containerid>:/data/output/resourceDetails.csv . # to copy the output file from docker container to host