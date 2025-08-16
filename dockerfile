# Usar la imagen base de AWS Lambda para Python
FROM public.ecr.aws/lambda/python:3.9

WORKDIR /var/task

# Copiar el archivo de dependencias
COPY requirements.txt ${LAMBDA_TASK_ROOT}

# Instalar las dependencias
RUN pip install --no-cache-dir -r requirements.txt

# Copiar el código de la aplicación
COPY lambda_function.py .
COPY *.py .

# Establecer el comando por defecto para Lambda
CMD [ "lambda_function.handler" ]

ENTRYPOINT ["/lambda-entrypoint.sh"]