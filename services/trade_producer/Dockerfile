FROM python:3.10.3-slim-buster


# stream output to terminal
ENV PYTHONUNBUFFERED=1

#isntall poetry inside the container
RUN pip install poetry==1.8.3


WORKDIR /app

# add /app/src to python path
ENV PYTHONPATH "${PYTHONPATH}:/app/src"

#copy the pyproject.toml and poetry.lock to the container
COPY pyproject.toml poetry.lock /app/

# install python dependencies
RUN poetry install

# Copy all source code to the container
COPY . /app

#run the app

CMD ["poetry", "run", "python", "src/main.py"]

