Migration Service 
===
Step 1: Modify cluster topology
---

Modify the hosts to be migrated in the topology file:

```shell 
$ vim topology.yaml 
```

```yaml 
kind: dingofs 
global: 
 variable: 
 home: /tmp 
 machine1: dingo001 
 machine2: dingo002 
 machine3: dingo003 
 machine4: dingo004 # Add new machines

mds_services: 
 config: 
 listen.ip: ${service_host} 
 listen.port: 6700 
 listen.dummy_port: 7700 
 deploy.
    - host: ${machine1}
    - host: ${machine2}
    - host: ${machine4} # Change ${machine3} to ${machine4} 
```

> :warning: **Warning:** 
> 
> * You can only migrate services with the same role at a time 
> * You can only migrate services with the same host at a time

Step 2: Migrate Services
---

```shell 
$ dingoadm migrate topology.yaml 
```

> :bulb: **REMINDER:** 
> 
> The migration operation is an idempotent operation, users can repeat the operation after it fails, don't worry about the service residual problem!