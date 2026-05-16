FROM apache/spark:3.5.1

USER root

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    pandas==2.0.3 \
    numpy==1.24.4 \
    pdfplumber==0.10.3 \
    pdfminer.six==20231228 \
    openpyxl==3.1.2

CMD ["python3", "-m", "main"]