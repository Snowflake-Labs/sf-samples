FROM nvcr.io/nvidia/pytorch:23.06-py3

COPY requirements.txt ./

RUN pip3 install --upgrade pip && \
    pip3 install -r requirements.txt
    
COPY training.py ./

ENTRYPOINT ["python3", "training.py"]