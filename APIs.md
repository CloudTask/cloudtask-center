# Cloudtask Center APIs Manual

> `GET` - http://localhost:8985/cloudtask/v2/_ping

&nbsp;&nbsp;&nbsp;&nbsp; center faq api, get center service config value and status info. 

``` json 
/*Response*/
HTTP 200 OK
{
    "app": "296721666eddf741016105ebe1bc3fb4",
    "key": "3a95a871-9dc4-4955-b213-5a040e324309",
    "node": {
        "type": 1,
        "hostname": "host.localdomain",
        "datacenter": "",
        "location": "",
        "os": "linux",
        "platform": "amd64",
        "ipaddr": "192.168.2.80",
        "apiaddr": "http://192.168.2.80:8985",
        "pid": 1,
        "singin": false,
        "timestamp": 0,
        "alivestamp": 1521268194,
        "attach": null
    },
    "systemconfig": {
        "version": "v.2.0.0",
        "pidfile": "./jobserver.pid",
        "retrystartup": true,
        "useserverconfig": true,
        "cluster": {
            "hosts": "192.168.2.80:2181,192.168.2.81:2181,192.168.2.82:2181",
            "root": "/cloudtask",
            "device": "",
            "runtime": "",
            "os": "",
            "platform": "",
            "pulse": "30s",
            "timeout": "60s",
            "threshold": 1
        },
        "api": {
            "hosts": [
                ":8985"
            ],
            "enablecors": true
        },
        "scheduler": {
            "allocmode": "hash",
            "allocrecovery": "320s"
        },
        "cache": {
            "lrusize": 1024,
            "storagedriver": {
                "mongo": {
                    "database": "cloudtask",
                    "hosts": "192.168.2.80:27017,192.168.2.81:27017,192.168.2.82:27017",
                    "options": [
                        "maxPoolSize=20",
                        "replicaSet=mgoCluster"
                    ]
                }
            }
        },
        "notifications": {
            "EndPoints": [
                {
                    "Name": "smtp",
                    "URL": "",
                    "Enabled": true,
                    "Sender": "cloudtask@example.com",
                    "Host": "smtp.example.com",
                    "Port": 25,
                    "User": "",
                    "Password": ""
                }
            ]
        },
        "logger": {
            "logfile": "./logs/jobserver.log",
            "loglevel": "info",
            "logsize": 20971520
        }
    }
}
```

> `GET` - http://localhost:8985/cloudtask/v2/jobs/{jobid}/base

&nbsp;&nbsp;&nbsp;&nbsp; get a job base info.
``` json 
/*Response*/
HTTP 200 OK
{
    "jobid": "399d4b159c65c9b34d2a3c41",
    "jobname": "ACCT.ShippingCost.job",
    "filename": "",
    "filecode": "d41d8cd98f00b204e9800998ecf8427e",
    "cmd": "ps aux",
    "env": [],
    "timeout": 0,
    "version": 1,
    "schedule": [
        {
            "id": "1623e5f1a23",
            "enabled": 1,
            "turnmode": 2,
            "interval": 2,
            "startdate": "03/19/2018",
            "enddate": "",
            "starttime": "00:00",
            "endtime": "23:59",
            "selectat": "",
            "monthlyof": {
                "day": 1,
                "week": ""
            }
        },
        {
            "id": "1623e5f1a23",
            "enabled": 1,
            "turnmode": 2,
            "interval": 4,
            "startdate": "03/19/2018",
            "enddate": "",
            "starttime": "00:00",
            "endtime": "23:59",
            "selectat": "",
            "monthlyof": {
                "day": 1,
                "week": ""
            }
        }
    ]
}
```

> `GET` - http://localhost:8985/cloudtask/v2/runtimes/{runtime}/jobsalloc 

&nbsp;&nbsp;&nbsp;&nbsp; get a runtime jobs alloc table info.
``` json 
/*Response*/
HTTP 200 OK
{
    "content": "request successed.",
    "data": {
        "alloc": {
            "location": "myCluster",
            "version": 53,
            "data": [
                {
                    "jobid": "e7994ade7a32b13b4a51788d",
                    "key": "517ae088-779c-4520-b23f-e5f0c341081d",
                    "version": 3,
                    "jobname": "MKPL-XML-Update",
                    "ipaddr": "192.168.2.81",
                    "hostname": "localhost.localdomain"
                },
                {
                    "jobid": "666e3b394cfd1ee1577e72b4",
                    "key": "529ae1f4-d7ea-45e0-0f3f-32aefaba281c",
                    "version": 2,
                    "jobname": "ACCT.Accounting.job",
                    "ipaddr": "192.168.2.80",
                    "hostname": "localhost.localdomain"
                },
                {
                    "jobid": "399d4b159c65c9b34d2a3c41",
                    "key": "517ae088-779c-4520-b23f-e5f0c341081d",
                    "version": 1,
                    "jobname": "ACCT.ShippingCost.job",
                    "ipaddr": "192.168.2.81",
                    "hostname": "host.localdomain"
                },
                {
                    "jobid": "33bd7b52592f4f2c45262e3b",
                    "key": "509794cc-3539-4f58-a4d3-cc02a4f4848f",
                    "version": 3,
                    "jobname": "EDI.Portol",
                    "ipaddr": "192.168.2.82",
                    "hostname": "host.localdomain"
                },
                {
                    "jobid": "8fee1ea957b7b6b49bd4e75f",
                    "key": "529ae1f4-d7ea-45e0-0f3f-32aefaba281c",
                    "version": 2,
                    "jobname": "EC.Flash",
                    "ipaddr": "192.168.2.80",
                    "hostname": "localhost.localdomain"
                },
                {
                    "jobid": "72ec7bb9decf1e8ea92ad3da",
                    "key": "509794cc-3539-4f58-a4d3-cc02a4f4848f",
                    "version": 1,
                    "jobname": "MKPL-File-Update",
                    "ipaddr": "192.168.2.82",
                    "hostname": "host.localdomain"
                }
            ]
        }
    }
}
```

> `GET` - http://localhost:8985/cloudtask/v2/runtimes/{runtime}/servers

&nbsp;&nbsp;&nbsp;&nbsp; get a runtime current all `healthy` servers.
``` json 
/*Response*/
HTTP 200 OK
{
    "content": "request successed.",
    "data": {
        "servers": [
            {
                "key": "529ae1f4-d7ea-45e0-0f3f-32aefaba281c",
                "name": "host.localdomain",
                "ipaddr": "192.168.2.80",
                "apiaddr": "http://192.168.2.80:8600",
                "os": "linux",
                "platform": "amd64",
                "status": 1,
                "alivestamp": 1521276530
            },
            {
                "key": "517ae088-779c-4520-b23f-e5f0c341081d",
                "name": "localhost.localdomain",
                "ipaddr": "192.168.2.81",
                "apiaddr": "http://192.168.2.81:8600",
                "os": "linux",
                "platform": "amd64",
                "status": 1,
                "alivestamp": 1521277792
            },
            {
                "key": "509794cc-3539-4f58-a4d3-cc02a4f4848f",
                "name": "localhost.localdomain",
                "ipaddr": "192.168.2.82",
                "apiaddr": "http://192.168.2.82:8600",
                "os": "linux",
                "platform": "amd64",
                "status": 1,
                "alivestamp": 1521237452
            }
        ]
    }
}
```

> `PUT` - http://localhost:8985/cloudtask/v2/jobs/action

&nbsp;&nbsp;&nbsp;&nbsp; a job actions request, operation job `start` | `stop`.
``` json
/*Request*/
{
    "runtime": "myCluster",
    "jobid": "8fee1ea957b7b6b49bd4e75f",
    "action": "start"
}

/*Response*/
HTTP 202 Accepted
{
    "content": "request accepted."
}
```
