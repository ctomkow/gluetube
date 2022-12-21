# gluetube
A python script scheduler with a shared local database. Meant to enable easy automation and integration of infrastructure and systems. Like cron, but with more bells and whistles.

## installation

There are two ways to deploy gluetube. 

* docker
* virtual machine / bare metal

### docker

1. `docker volume create gluetube-cfg`

2. `docker volume create gluetube-db`

3. `docker run -d --init --name gluetube -v gluetube-cfg:/home/gluetube/.gluetube/etc -v gluetube-db:/home/gluetube/.gluetube/db ctomkow/gluetube`

4. `docker exec -it gluetube bash`

5. `gt --initdb`

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

You are meant to develop your own pipelines in python for gluetube. The following is a brief description of how to get your development environment setup. These instructions assume you use **VS code** and **docker**.

1. `docker pull ctomkow/gluetube`

2. `docker volume create gluetube-db`

3. `docker volume create gluetube-cfg`

4. `docker run -itd --name gluetube --net=host -v gluetube-db:/home/gluetube/.gluetube/db -v gluetube-cfg:/home/gluetube/.gluetube/etc ctomkow/gluetube:latest`

5. Open VS code. In the docker-> containers section, right click on the running gluetube container and `attach visual studio code` to running container. 

6. Within the container terminal, enter the pipelines directory. `cd pipelines`

7. Clone a repository. `git clone <url> .` Note the `.` at the end of the git clone. This is important because we don't want the project folder as a sub directory.
 
8. Now you can develop your pipelines (.py files) while having a live gluetube daemon running within the container. All the gluetube cli commands are available to test out your pipeline code in a production-like environment. `gt --help`

## roadmap

[gluetube roadmap](https://github.com/ctomkow/gluetube/wiki/Roadmap)
