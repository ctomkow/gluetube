# gluetube
A python script scheduler with a shared local database. Meant to enable easy automation and integration of infrastructure and systems. Like cron, but with more bells and whistles.

## installation

There are two ways to deploy gluetube. 

* docker
* virtual machine / bare metal

### docker

1. `docker volume create gluetube-cfg`

2. `docker volume create gluetube-db`

3. `docker run -d --init -v gluetube-cfg:/home/gluetube/.gluetube/etc -v gluetube-db:/home/gluetube/.gluetube/db ctomkow/gluetube`

### VM

1. `adduser gluetube`

2. `pip install --user gluetube`

3. `gt --configure`

4. `gt --initdb`

5. `gt daemon --background`

## example usage

> `gt --help`

> `gt summary`

> `gt schedule 1 --now`

## pipeline development

You are meant to develop your own pipelines in python for gluetube. The following is a brief description of how to get your development environment setup.

1. `docker pull ctomkow/gluetube`

2. `docker volume create gluetube-db`

3. `docker volume create gluetube-cfg`

4. `docker run -itd --name gluetube --net=host -v gluetube-db:/home/gluetube/.gluetube/db -v gluetube-cfg:/home/gluetube/.gluetube/etc ctomkow/gluetube:latest`

5. Use VS code. Attach VS code to running container. Clone your pipeline repository inside your VS code instance attached to the container. Update ~/.gluetube/etc/gluetube.cfg and point config to your pipeline directory

## roadmap

[gluetube roadmap](https://github.com/ctomkow/gluetube/wiki/Roadmap)