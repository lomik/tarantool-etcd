# tarantool-etcd
Tarantool replication setup helper

## Requirements
* [tarantool](https://github.com/tarantool/tarantool)
* [tarantool-http](https://github.com/tarantool/http)

## Configuration
Sample cluster instance configuration
```lua
-- reload package
package.loaded['etcd'] = nil
local etcd = require('etcd')

-- kill all fibers with name starts with "etcd:"
-- required for reload configuration without restart
etcd.killall()
 
-- init etcd client with login and password
local client = etcd.client("tarantool:****@10.255.6.110:4001")

-- init cluster object
local cluster = client:cluster(
    "/tarantool/cluster/mega_cluster", -- root directory in etcd for this cluster
    "192.168.0.145:2641",  -- this instance host/port
    {
        role = "master",
        replica_user = "replica",
        replica_password = "qwerty",
        dump = "/var/lib/tarantool/tarantool65_cluster.json",
        discovery_ttl = 60,
        discovery_refresh = 10,
    }
)

-- detect first start
if not etcd.file_exists("/snaps65/*.snap") then
    cluster:bootstrap()
end
 
box.cfg{
    listen = 2641,
    slab_alloc_arena = 0.2,
    slab_alloc_factor = 1.04,
    replication_source = cluster:replication_source(),
    wal_dir    = "/data/xlogs65",
    snap_dir   = "/snaps65",
    logger     = "/var/log/tarantool/tarantool65.log",
    pid_file   = "/var/run/tarantool/tarantool65.pid",
}
 
-- user for replication
box.once('user:replica:v1', function()
    box.schema.user.create('replica', {password = 'qwerty'})
    box.schema.user.grant('replica', 'read,write,execute', 'universe')
end)
 
-- run background etcd announce and replication_source refresh
cluster:run()
```
