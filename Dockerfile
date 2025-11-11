FROM public.ecr.aws/lambda/python:3.11

# Define diretório de trabalho
WORKDIR /var/task

# Copia arquivos principais
COPY src/ ./src/
COPY main.py .
COPY api_endpoints.yaml .
COPY requirements.txt .

# Instala dependências
RUN pip install -r requirements.txt

# Define o handler (arquivo.main_function)
CMD ["main.lambda_handler"]
