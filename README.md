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
 - [ ] encrypted key value database
 - [ ] 50%+ test coverage
 - [x] cli v1 (daemon control, modify scheduling, view pipelines and runs)
 - [x] pip install (directory mgmt, upgrades, etc)
 - [x] github actions 
 
### 0.2.0 roadmap
 - [ ] remote shell connector (for executing cli apps on systems. e.g. ansible, rancid, etc)
 - [ ] re-run pipeline if crashed. specify max retries
 - [ ] git pull repo of pipelines into pipeline_dir
 - [ ] option to define (name, schedule) in pipeline (incl. run pipeline on change to get new potential name, schedule etc.)
 - [ ] a 'gluetube pipeline --clean' and 'gluetube pipeline --cleanall' to remove .venv's that are made

### 0.3.0 roadmap
 - [ ] frontend web ui
 - [ ] REST api endpoint to trigger running pipeline
 - [ ] development pipeline mode (verbose output for testing pipeline runs)

### 0.4.0 roadmap
 - [ ] dynamic webhook url that pipeline can access (for event listener type pipelines)
 - [ ] .rpm
 - [ ] .deb

 ### 0.5.0 roadmap
 - [ ] cli v2
 - [ ] pipeline developement mode (ability to see the stdout of the pipeline to track it's run)

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

## dev env

docker build -t gluetube-dev:0.1.0 -f dockerfile.dev .

docker volume create gluetube-db

docker run -itd --name gluetube-dev --net=host -v ~/code/gluetube/gluetube:/home/gluetube/.local/lib/python3.10/site-packages/gluetube -v ~/code/pipelines:/home/gluetube/.gluetube/pipelines -v gluetube-db:/home/gluetube/.gluetube/db gluetube-dev:0.1.0

docker exec -it gluetube bash
> cd ~/.local/lib/python3.10/site-packages/gluetube

> ./gluetube.py --initdb

## design

Interfaces (cli, gui) interact with the gluetubed socket. The daemon is responsible for making changes to both the scheduler and the database. Treat the database only as a user facing state view.

If a pipeline is in the database, then it should be scheduled. It can be paused and not running, but still registered in the scheduler. Then, if a pipeline is removed, it is deleted from the database and deleted from the scheduler.

Remember, don't touch the db directly. All write should be through an RPC call, otherwise there will be db/scheduler mis-matches which whould require a daemon reload to resolve.