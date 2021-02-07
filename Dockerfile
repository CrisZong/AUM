FROM public.ecr.aws/lambda/python:latest
RUN pip install --no-cache-dir pandas numpy gspread oauth2client apiclient python-lambda google-api-python-client
COPY src/ .
COPY .env/ ../.env/
CMD [ "service.handler" ]