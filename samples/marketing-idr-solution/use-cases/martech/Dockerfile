# Build from repo root: docker build -f use-cases/martech/Dockerfile -t martech:latest use-cases/martech
FROM node:20-alpine AS frontend-build
ARG VITE_INDUSTRY=MARTECH
WORKDIR /frontend
COPY frontend/package*.json ./
RUN npm ci --legacy-peer-deps || npm install --no-audit --no-fund
COPY frontend/ ./
ENV VITE_INDUSTRY=${VITE_INDUSTRY}
RUN npm run build

FROM python:3.11-slim
WORKDIR /app

RUN apt-get update && apt-get install -y --no-install-recommends \
    nginx \
    && rm -rf /var/lib/apt/lists/* \
    && rm -f /etc/nginx/sites-enabled/default

COPY backend/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY backend/ ./backend/
COPY --from=frontend-build /frontend/dist ./static/

RUN echo 'server { \
    listen 8080; \
    add_header Cache-Control "no-cache, no-store, must-revalidate" always; \
    add_header Pragma "no-cache" always; \
    location / { \
        root /app/static; \
        try_files $uri $uri/ /index.html; \
    } \
    location /api { \
        proxy_pass http://127.0.0.1:8000; \
        proxy_set_header Host $host; \
        proxy_set_header X-Real-IP $remote_addr; \
        proxy_read_timeout 300s; \
        proxy_connect_timeout 10s; \
        proxy_send_timeout 300s; \
    } \
    location /health { \
        proxy_pass http://127.0.0.1:8000; \
    } \
}' > /etc/nginx/conf.d/default.conf

COPY <<EOF /app/start.sh
#!/bin/sh
cd /app/backend && uvicorn main:app --host 0.0.0.0 --port 8000 &
nginx -g 'daemon off;'
EOF
RUN chmod +x /app/start.sh

EXPOSE 8080
CMD ["/app/start.sh"]
