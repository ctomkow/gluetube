# gluetube
todo

## dev env

docker build -t gluetube-dev:0.1.0 -f dockerfile.dev .

docker run -itd --name gluetube --net=host -v ~/code/gluetube/gluetube:/home/gluetube/.local/lib/python3.10/site-packages/gluetube -v ~/code/pipelines:/home/gluetube/.gluetube/pipelines gluetube-dev:0.1.0

docker exec -it gluetube bash
> cd ~/.local/lib/python3.10/site-packages/gluetube
> ./gluetube.py --init

## design

Interfaces (cli, gui) interact with the gluetubed socket. The daemon is responsible for making changes to both the scheduler and the database. Treat the database only as a user facing state view.