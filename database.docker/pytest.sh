docker-compose up -d
docker build -t python_q2_test .
docker run -i --rm --network=host --rm  python_q2_test pytest ..
