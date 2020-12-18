FROM python:3.8-slim

WORKDIR /app/pdg/data_gathering
ADD ./data_gathering ./

WORKDIR /app/pdg
ADD ./starter/cl.py ./
ADD pyproject.toml poetry.lock ./

RUN apt update
RUN apt install -y git

RUN pip install poetry
RUN poetry install

RUN apt remove -y git
RUN apt purge -y git
RUN apt autoremove -y
RUN apt clean

CMD poetry run python -u cl.py
