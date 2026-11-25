# Imagem para rodar nós do Arquipélago em contêineres (cada contêiner = um "computador").
FROM python:3.13-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY src/ src/
ENV PYTHONPATH=/app/src PYTHONUNBUFFERED=1
ENTRYPOINT ["python", "-m", "arquipelago"]
CMD ["lan", "--auto-start", "20"]
