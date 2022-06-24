#### Docker, Gitlab and Jenkins are few deployment tools which are most popular and widely used.

For this Project, we will use Docker for Deployment of code.

## Docker Deployment Steps
1. Checkout Git to your local repository
2. Go to ETL-Challenge -> app -> src -> config.yaml and update the basepath parameter to your current directory positon.
3. Open command prompt (Windows) / Terminal(Linux)
4. Change directory path to ‘ETL-Challenge’
5. To create Docker Image
    - For Linux - Run command - sudo docker build --tag ETL-Challenge .  
    - For Windows - Run command - docker build --tag ETL-Challenge .
6. To run Docker Image, run command - docker run ETL-Challenge
