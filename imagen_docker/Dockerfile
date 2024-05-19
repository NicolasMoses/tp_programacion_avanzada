FROM python:3.8

WORKDIR /app

COPY requirements.txt credential_DB.py main2.py ./

RUN pip install -r requirements.txt

EXPOSE 8000

CMD ["uvicorn", "main2:app", "--host", "0.0.0.0", "--port", "8000"]
