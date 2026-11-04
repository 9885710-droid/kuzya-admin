FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY app ./app
RUN useradd -u 10001 -M app && mkdir -p /app/uploads && chown app /app/uploads
USER app
# --proxy-headers не включаем: IP клиента админка берёт сама, только от доверенного хопа (Traefik)
CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8000"]
