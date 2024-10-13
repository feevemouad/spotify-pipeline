FROM apache/airflow:2.8.1
COPY requirements.txt /opt/airflow/
USER airflow
RUN curl -L -O -C - https://files.pythonhosted.org/packages/1e/8c/b0cee957eee1950ce7655006b26a8894cee1dc4b8747ae913684352786eb/pycryptodome-3.21.0-pp310-pypy310_pp73-manylinux_2_17_x86_64.manylinux2014_x86_64.whl
RUN pip install --no-cache-dir -r /opt/airflow/requirements.txt

