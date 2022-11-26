FROM python:3.10.7-bullseye as build

RUN mkdir -p /builddir
WORKDIR /builddir
COPY ./ ./
RUN pip install -r requirements-dev.txt
RUN python -m build

FROM python:3.10.7-bullseye

ARG VERSION
LABEL name="gluetube" \
      maintainer="ctomkow@gmail.com" \
      version=${VERSION} \
      summary="A lightweight python script scheduler" \
      description="An orchestrator that runs and monitors python scripts with a shared key value store" \
      url="https://github.com/ctomkow/gluetube"

RUN useradd --uid 1000 --create-home --shell /bin/bash gluetube

USER gluetube
ENV PATH "$PATH:/home/gluetube/.local/bin"

WORKDIR /home/gluetube

COPY --from=build /builddir/dist/gluetube-${VERSION}-py3-none-any.whl ./
RUN pip install --user gluetube-${VERSION}-py3-none-any.whl

RUN gluetube --initdb

CMD ["sh", "-c", "exec gluetube daemon -f"]