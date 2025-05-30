FROM python:3.10-slim

# Diretório de trabalho
WORKDIR /app

# Copia tudo, incluindo src/ e requirements.txt
COPY ./src /app/src
COPY requirements.txt .

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Roda o main.py dentro da pasta /app/src
CMD ["python", "src/main.py"]
