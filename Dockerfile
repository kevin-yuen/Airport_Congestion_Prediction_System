FROM apache/spark:3.5.1

USER root

WORKDIR /app

COPY . /app

RUN pip install --no-cache-dir \
    pyspark==3.5.1 \
    pandas==2.0.3 \
    numpy==1.24.4 \
    matplotlib==3.7.5 \
    seaborn==0.13.2 \
    scipy==1.10.1 \
    scikit-learn==1.3.2 \
    jupyter==1.0.0 \
    ipykernel==6.29.5 \
    pdfplumber==0.10.3 \
    pdfminer.six==20231228 \
    openpyxl==3.1.2 \
    xlrd==2.0.1

CMD ["python3", "-m", "main"]