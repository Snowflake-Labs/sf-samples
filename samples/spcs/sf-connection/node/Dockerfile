FROM node:18
EXPOSE 8080
WORKDIR /app
COPY src/package.json .
RUN npm install
COPY src/. .
RUN chmod +x ./entrypoint.sh
ENTRYPOINT [ "./entrypoint.sh" ]
