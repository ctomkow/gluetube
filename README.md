# gluetube
todo

## dev env

docker build -t gluetube-dev:0.1.0 -f dockerfile.dev .

docker run -itd --net=host -v ~/code/gluetube/gluetube:/home/gluetube/.local/lib/python3.10/site-packages/gluetube -v ~/code/pipelines:/home/gluetube/.gluetube/pipelines gluetube-dev:0.1.0
