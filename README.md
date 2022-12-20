# gluetube
todo

## roadmap

### 0.1.0 roadmap
 - [x] run pipeline in isolated env
 - [x] autodiscover pipelines
 - [x] tracking status and stages of pipeline
 - [x] shared variables across pipelines
 - [x] pipeline scheduler
 - [x] cron scheduling
 - [x] at scheduling (run once at date time)
 - [x] gluetube daemon
 - [x] sqlite database schema
 - [x] dockerfile
 - [ ] hello world tutorial
 - [x] encrypted values in store.db
 - [x] 50%+ test coverage
 - [x] cli v1 (daemon control, modify scheduling, view pipelines and runs)
 - [x] pip install (directory mgmt, upgrades, etc)
 - [x] github actions
 - [x] proxy support to ensure pip can pull pipeline dependencies
 
### 0.2.0 roadmap
 - [ ] remote shell connector (for executing cli apps on systems. e.g. ansible, rancid, etc)
 - [ ] re-run pipeline if crashed. specify max retries
 - [ ] git pull repo of pipelines into pipeline_dir
 - [ ] option to define (name, schedule) in pipeline (incl. run pipeline on change to get new potential name, schedule etc.)
 - [ ] a 'gluetube pipeline --clean' and 'gluetube pipeline --cleanall' to remove .venv's that are made
 - [ ] pipeline accessible database to store system object relationships related to pipeline
 - [ ] ability to view pipeline runs via cli
 - [ ] cgroup restrictions for pipelines

### 0.3.0 roadmap
 - [ ] frontend web ui
 - [ ] REST api endpoint to trigger running pipeline, including passing optional parameters as json payload that is accessable by pipeline (PUSH)
 - [ ] pipeline developement mode (ability to see the stdout of the pipeline to track it's run)
 - [ ] ability to 'attach' to running pipeline and see stdout (e.g. gluetube logs -f pipeline_name)
 
### 0.4.0 roadmap
 - [ ] dynamic webhook url that pipeline can access/monitor (long-lived LISTENER pipelines)
 - [ ] gracefully handle SIG[TERM|KILL] and ensure it waits for any running pipelines to stop or manually stop them
 - [ ] .rpm
 - [ ] .deb

 ### 0.5.0 roadmap
 - [ ] cli v2
 - [ ] ability to run pipelines serially that are linked together (e.g. pipeline reuse)

## installation
> adduser gluetube

> pip install --user gluetube

> gluetube --configure

> gluetube --initdb

> gluetube daemon --background

> gluetube daemon --stop

OR
> docker volume create gluetube

> docker run -d --init -v gluetube:/home/gluetube/.gluetube ctomkow/gluetube

todo: systemd unit file (when rpm/deb is built, it will include a unit file since the packages are run as root)

## usage

> gluetube --help

> gluetube summary

> gluetube schedule 1 --now

## pipeline development

docker pull ctomkow/gluetube

docker volume create gluetube-db

docker volume create gluetube-cfg

docker run -itd --name gluetube --net=host -v gluetube-db:/home/gluetube/.gluetube/db -v gluetube-cfg:/home/gluetube/.gluetube/etc ctomkow/gluetube:latest

Use VS code. Attach VS code to running container. Clone your pipeline repository inside your VS code instance attached to the container. Update ~/.gluetube/etc/gluetube.cfg and point config to pipeline directory


## gluetube dev env

docker build -t gluetube-dev:0.1.0 -f dockerfile.dev .

docker volume create gluetube-db

docker volume create gluetube-cfg

docker run -itd --name gluetube-dev --net=host -v ~/code/gluetube/gluetube:/home/gluetube/.local/lib/python3.10/site-packages/gluetube -v ~/code/test_pipelines:/home/gluetube/.gluetube/pipelines -v gluetube-db:/home/gluetube/.gluetube/db gluetube-dev:0.1.0

docker exec -it gluetube-dev bash

## design

Interfaces (cli, gui) interact with the gluetubed socket. The daemon is responsible for making changes to both the scheduler and the database. Treat the database only as a user facing state view.

If a pipeline is in the database, then it should be scheduled. It can be paused and not running, but still registered in the scheduler. Then, if a pipeline is removed, it is deleted from the database and deleted from the scheduler.

Remember, don't touch the db directly. All write should be through an RPC call, otherwise there will be db/scheduler mis-matches which whould require a daemon reload to resolve.