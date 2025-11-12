# Imagem base oficial da AWS Lambda para Python 3.11
FROM public.ecr.aws/lambda/python:3.11

# Copiar todos os arquivos do projeto
COPY . ${LAMBDA_TASK_ROOT}

# Instalar dependências
RUN pip install --upgrade pip && \
    pip install -r requirements.txt

# Definir o handler principal (arquivo.main_function)
CMD ["main.lambda_handler"]
