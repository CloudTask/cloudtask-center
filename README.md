# cloudtask-center
The cloudtask platform center scheduler.


Responsible for task scheduling distribution, processing task failover, cluster node discovery and status management.
### Documents 
* [APIs Manual](./APIs.md)
* [Configuration Introduction](./Configuration.md)

### Architecture
<img src="https://cloudtask.github.io/cloudtask/_media/cloudtask-architecture.png" width="640" height="300" alt="图片名称" align=center/>

### Usage

> binary

``` bash
$  ./cloudtask-center -f etc/config.yaml
```

> docker image

``` bash
$ docker run -d --net=host --restart=always \
  -v /opt/app/cloudtask-center/etc/config.yaml:/opt/cloudtask/etc/config.yaml \
  -v /opt/app/cloudtask-center/logs:/opt/cloudtask/logs \
  -v /etc/localtime:/etc/localtime \
  --name=cloudtask-center \
  cloudtask/cloudtask-center:2.0.0
```


## License
cloudtask source code is licensed under the [Apache Licence 2.0](http://www.apache.org/licenses/LICENSE-2.0.html). 
