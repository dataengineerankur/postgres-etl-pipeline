FROM python:3.11-slim

RUN pip install --no-cache-dir fastapi==0.115.6 uvicorn==0.32.1 pydantic==2.10.3 faker==30.8.2
WORKDIR /app
COPY mock_pos_api/app.py /app/app.py
EXPOSE 8099
CMD ["uvicorn", "app:app", "--host", "0.0.0.0", "--port", "8099"]


