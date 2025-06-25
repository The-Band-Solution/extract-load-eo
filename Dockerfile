FROM python:3.10

# Define o diretório de trabalho
WORKDIR /app

# Copia o código-fonte e dependências
COPY ./src /app/src
COPY requirements.txt /app/

# Instala as dependências
RUN pip install --no-cache-dir -r requirements.txt

# Define PYTHONPATH para que 'src.' funcione nos imports
ENV PYTHONPATH=/app/src

# Comando de execução
CMD ["python", "-m", "src.main"]

