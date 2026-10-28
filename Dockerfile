FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY common/ common/
COPY WM_Central/ WM_Central/
COPY WM_WS/ WM_WS/
COPY WM_FO/ WM_FO/
RUN mkdir -p /data
ENV PYTHONUNBUFFERED=1
ENV WM_DB=/data/wm_central.db
CMD ["sh", "-c", "python WM_Central/WM_Central.py ${PUERTO_SOCKETS:-5050} ${KAFKA:-localhost:9092}"]