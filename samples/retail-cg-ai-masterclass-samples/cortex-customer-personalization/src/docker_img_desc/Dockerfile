FROM nvcr.io/nvidia/pytorch:23.06-py3

COPY requirements.txt entrypoint.sh makedirs.sh ./
RUN chmod -R 777 makedirs.sh
RUN ./makedirs.sh

RUN chmod -R 777 entrypoint.sh
 
RUN pip3 install --upgrade pip && pip3 install accelerate
RUN pip3 install fschat
RUN pip install -r requirements.txt

# Make port 8888 available to the world outside this container
EXPOSE 8888

ENTRYPOINT ["./entrypoint.sh"]
