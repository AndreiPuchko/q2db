FROM python:3.6
RUN git clone https://github.com/AndreiPuchko/q2db
RUN python -m pip install --upgrade pip
RUN pip install q2db
RUN pip install mysql-connector-python
RUN pip install psycopg2-binary
RUN pip install pytest
WORKDIR q2db/demo
