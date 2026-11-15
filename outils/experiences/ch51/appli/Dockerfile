FROM python:3.14-slim
RUN pip install --no-cache-dir aiohttp==3.14.5 redis==6.4.0
COPY appli.py /appli.py
CMD ["python", "-u", "/appli.py"]
