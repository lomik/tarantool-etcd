module("etcd", package.seeall)

local fiber = require('fiber')
local http_client = require('http.client')
local log = require('log')
local urilib = require('uri')
local fio = require('fio')
local json = require('json')
local clock = require('clock')
local uuid = require('uuid')
local digest = require('digest')
local math = require('math')

local function debugf(fmt, ...)
    local c = debug.getinfo(2)
    log.debug("[etcd.lua:%d:%s] "..fmt, c.currentline, c.name, ...)
end

local function infof(fmt, ...)
    local c = debug.getinfo(2)
    log.info("[etcd.lua:%d:%s] "..fmt, c.currentline, c.name, ...)
end

local function errorf(fmt, ...)
    local c = debug.getinfo(2)
    log.error("[etcd.lua:%d:%s] "..fmt, c.currentline, c.name, ...)
end

function pp(...)
    local values = {...}
    for index, value in ipairs(values) do
        print(json.encode(value))
    end
end

local function spawn(func)
    local f = fiber.create(func)
    f:name("etcd:"..uuid.str())
    return f
end

function killall()
    local fibers = fiber.info()
    for fid, fvalue in pairs(fiber.info()) do
        if string.sub(fvalue.name, 1, 5) == "etcd:" then
            infof("kill fiber id=%d, name=%s", fid, fvalue.name)
            local f = fiber.find(fid)
            if f ~= nil and f:status() ~= "dead" then
                f:cancel()
            end
        end
    end
end

function file_exists(snap_glob)
    local files = fio.glob(snap_glob)
    if (#files > 0) then
        return true
    end
    return false
end

function call_timeout(timeout, func, ...)
    local out = fiber.channel(2)
    local args = {...}

    function delay()
        fiber.sleep(timeout)
        out:put(nil)
    end

    function call()
        local r = func(unpack(args))
        out:put({r})
    end

    local f1 = fiber.create(delay)
    local f2 = fiber.create(call)

    local r = out:get()

    if r == nil then
        -- timeout
        if f2:status() ~= "dead" then
            f2:cancel()
        end

        return nil, true
    end

    -- success
    if f1:status() ~= "dead" then
        f1:cancel()
    end

    return r[1], false
end

function uri_escape(str)
    local res = {}
    if type(str) == 'table' then
        for _, v in pairs(str) do
            table.insert(res, uri_escape(v))
        end
    else
        res = string.gsub(str, '[^a-zA-Z0-9_]',
            function(c)
                return string.format('%%%02X', string.byte(c))
            end
        )
    end
    return res
end

function uri_add_param(uri, param, value)
    if string.find(uri, "?", 1, true) then
        return uri.."&"..uri_escape(param).."="..uri_escape(value)
    else
        return uri.."?"..uri_escape(param).."="..uri_escape(value)
    end
end

function uri_join(...)
    local args = {...}

    local url = ""
    for index, a in ipairs(args) do
        if #url == 0 then
            url = a
        else
            if string.sub(a, 1, 1) == "/" then
                if string.sub(url, #url, #url) == "/" then
                    url = url .. string.sub(a, 2)
                else
                    url = url .. a
                end
            else
                if string.sub(url, #url, #url) == "/" then
                    url = url .. a
                else
                    url = url .. "/" .. a
                end
            end
        end
    end
    return url
end

local function error_timeout_default()
    return 1
end

local function error_timeout(t)
    local def = error_timeout_default()
    if not t or t < def then t = def end

    fiber.sleep(t)

    if t < 60 then t = (t + math.random(0, 1)) * 2 end
    return t
end

-- REQUEST HANDLE FUNCTIONS

function has_error(response)
    if response.status == 200 or response.status == 201 then
        -- success
        return false
    end
    return true
end

function error_message(response)
    if response.status == 200 or response.status == 201 then
        -- success
        return ""
    end

    if response.status == 595 or response.status == 599 then
        -- http client error
        return response.reason.." (status:"..response.status..")"
    end

    if response.body ~= nil and response.body ~= "" then
        local ok, data = pcall(json.decode, response.body)
        if ok and data ~= nil and data.errorCode ~= nil and data.message ~= nil then
            -- etcd response
            return data.message.." (code:"..data.errorCode..")"
        end
    end

    return response.reason.." (status:"..response.status..")"
end


-- <CLIENT>

local Client = {}

function Client.clone(self)
    return client(self.addr, {
        user = self.user,
        password = self.password,
        timeout = self.timeout,
    })
end

function Client.request(self, method, query, timeout)
    local headers = {}

    local u = urilib.parse(self.addr)
    local login, pass = u["login"], u["password"]
    local host, port = u["host"], u["service"]

    if login then
        if pass == nil then
            pass = ""
        end
        headers["Authorization"] = "Basic "..digest.base64_encode(login..":"..pass)
    end

    local hp = host
    if port then
        hp = host .. ':' .. port
    end

    local url = uri_join("http://", hp, "/v2/keys", query)

    if timeout == nil then
        timeout = self.timeout
    end

    debugf("%s %s (timeout=%d)", method, url, timeout)
    
    local r, is_timeout = call_timeout(timeout, http_client.request, method, url, "", {headers=headers})

    if is_timeout then
        r = {status  = 595, reason  = "timeout"}
    end

    if r == nil then
        r = {status  = 595, reason  = "unknown error"}
    end

    if r["headers"] ~= nil and r["headers"]["x-etcd-index"] ~= nil then
        self.index = tonumber(r["headers"]["x-etcd-index"])
        debugf("x-etcd-index: %d", self.index)
    end

    if has_error(r) then
        debugf("%s %s (timeout=%d): %s", method, url, timeout, error_message(r))
    else
        debugf("%s %s (timeout=%d): success", method, url, timeout)
    end

    return r
end

function Client.get(self, key, opts)
    if opts == nil then
        opts = {}
    end

    local url = key

    if opts["recursive"] then
        url = uri_add_param(url, "recursive", "true")
    end
    if opts["sort"] then
        url = uri_add_param(url, "sorted", "true")
    end

    return self:request("GET", url, opts["timeout"])
end

function Client.set(self, key, value, opts)
    if opts == nil then
        opts = {}
    end


    local url = key
    if value ~= nil then
        url = uri_add_param(url, "value", value)
    end

    if opts["ttl"] ~= nil then
        url = uri_add_param(url, "ttl", opts["ttl"])
    end

    return self:request("PUT", url, opts["timeout"])
end

function Client.create(self, key, value, opts)
    return self:set(uri_add_param(key, "prevExist", "false"), value, opts)
end

function Client.update(self, key, value, opts)
    return self:set(uri_add_param(key, "prevExist", "true"), value, opts)
end

function Client.refresh(self, key, opts)
    return self:set(uri_add_param(uri_add_param(key, "prevExist", "true"), "refresh", "true"), nil, opts)
end

function Client.cas(self, key, old_value, new_value, opts)
    return self:set(uri_add_param(uri_add_param(key, "prevValue", old_value), "prevExist", "true"), new_value, opts)
end

function Client.wait(self, key, opts)
    -- TODO: while true waiting. Continue on timeout
    return self:get(uri_add_param(uri_add_param(key, "wait", "true"), "waitIndex", self.index+1), opts)
end

function client(addr, opts)
    if opts == nil then
        opts = {}
    end

    self = {
        addr = addr,
        index = 0,
        user = opts["user"],
        password = opts["password"],
        timeout = opts["timeout"],
    }

    if self.timeout == nil then
        self.timeout = 1
    end

    setmetatable(self, {__index = Client})

    return self
end

-- </CLIENT>

-- <DISCOVERY>
function Client.announce(self, key, value, opts)
    local client = self:clone()

    if opts == nil then
        opts = {}
    end

    if opts.ttl == nil then
        opts.ttl = 60 -- one minute
    end
    if opts.refresh == nil then
        opts.refresh = 10 -- every 10 seconds
    end
    if opts.timeout == nil then
        opts.timeout = 5
    end

    local err_timeout = 0

    function refresh()
        local set_required = true

        while true do
            if set_required then
                local r = client:set(key, value, {timeout=opts.timeout, ttl=opts.ttl})
                if has_error(r) then
                    errorf("announce %s set error: %s", key, error_message(r))
                    err_timeout = error_timeout(err_timeout)
                else
                    set_required = false
                    infof("announce %s set success", key)
                    fiber.sleep(opts.refresh)
                    err_timeout = 0
                end
            else -- !set_required
                local r = client:refresh(key, {timeout=opts.timeout, ttl=opts.ttl})
                if has_error(r) then
                    errorf("announce %s refresh error: %s", key, error_message(r))
                    set_required = true
                    err_timeout = error_timeout(err_timeout)
                else
                    fiber.sleep(opts.refresh)
                    err_timeout = 0
                end
            end -- if set_required

        end -- while
    end

    return spawn(refresh)
end

-- </DISCOVERY>

-- <CLUSTER>

local Cluster = {}

function Client.cluster(self, key, self_addr, opts)
    if opts == nil then
        opts = {}
    end

    cluster = {
        client = self:clone(),
        key = key,
        self_addr = self_addr,
        replica_user = opts["replica_user"],
        replica_password = opts["replica_password"],
        discovery_ttl = opts["discovery_ttl"],
        discovery_refresh = opts["discovery_refresh"],
        role = opts["role"],
        is_bootstrap = false,
        bootstrap_tmp_uuid = "",
        bootstrap_ttl = 30, -- maximum time between box.cfg{...} and cluster:run()
        dump = opts["dump"],
        data = {}, -- internal state. backuped to dump file
    }

    -- default values
    if cluster.replica_user == nil then
        cluster.replica_user = "guest"
    end

    if cluster.replica_password == nil then
        cluster.replica_password = ""
    end

    if cluster.discovery_ttl == nil then
        cluster.discovery_ttl = 60 -- one minute
    end

    if cluster.discovery_refresh == nil then
        cluster.discovery_refresh = 10 -- every 10 seconds
    end

    if cluster.role == nil then
        cluster.role = "master"
    end

    setmetatable(cluster, {__index = Cluster})

    -- read data from dump file
    cluster:restore()

    return cluster
end

function Cluster.path(self, part)
    return uri_join(self.key, part)
end

function Cluster.save(self) -- backup data to dump file
    if self.dump == nil or self.dump == "" then
        return
    end

    local dump_fn = fio.basename(self.dump)
    local dump_dir = fio.dirname(self.dump)
    local tmp_file = fio.pathjoin(dump_dir, "."..dump_fn)

    local fh = fio.open(tmp_file, {"O_TRUNC", "O_WRONLY", "O_CREAT"}, tonumber('644',8))
    if fh ~= nil then
        if fh:write(json.encode(self.data)) then
            if fh:fsync() and fh:close() then
                fio.rename(tmp_file, self.dump)
            end
        end
    else
        errorf("open %s for write failed", tmp_file)
    end 
end

function Cluster.restore(self) -- read data from dump file
    if self.dump == nil or self.dump == "" then
        return
    end

    local fh = fio.open(self.dump, {"O_RDONLY"})
    if fh ~= nil then
        -- file exists
        local c = fh:read(1024*1024)
        fh:close()

        local ok, data = pcall(json.decode, c)
        if ok and data ~= nil then
            infof("restored from dump: %s", c)
            self.data = data
        end
    else
        errorf("restore from %s failed", self.dump)
    end
end

function Cluster.replication_source_from_server(self)
    local err_timeout = 0

    while true do
        local r = self.client:get(self:path("/master/"))
        if has_error(r) then
            errorf("fetch replication source from %s error: %s", self:path("/master/"), error_message(r))
        else
            local ok, response = pcall(json.decode, r.body)
            local sources = {}

            if ok and response["node"] ~= nil and response["node"]["nodes"] ~= nil then
                for index,node in ipairs(response["node"]["nodes"]) do
                    if node["value"] ~= nil then
                        local ok, value = pcall(json.decode, node["value"])
                        if ok and value ~= nil and value["addr"] ~= nil then
                            table.insert(sources, value["addr"])
                        end
                    end
                end
            end

            return sources
        end

        err_timeout = error_timeout(err_timeout)
    end
end

function Cluster.bootstrap(self)
    self.is_bootstrap = true
    self.bootstrap_tmp_uuid = uuid.str()
end

function Cluster.bootstrap_replication_source(self)
    -- Work:
    -- 1. check "cluster_id" node
    -- 2. if exists, then read masters list (break if not empty)
    -- 3. if not exists, try to create
    -- 4. go to 1.
    local cluster_id = self:path("cluster_id")

    local err_timeout = 0
    while true do
        infof("check %s", cluster_id)

        local r = self.client:get(cluster_id)
        
        if r.status == 200 then
            -- exists cluster_id, check for masters
            infof("%s found", cluster_id)

            local s, timeout = call_timeout(5, Cluster.replication_source_from_server, self)
            if timeout then
                errorf("fetch masters timeout")
            elseif #s > 0 then
                infof("master: %s", json.encode(s))
                self.is_bootstrap = false -- no need confirm success start
                return s
            else
                errorf("master list is empty")
            end
        elseif r.status == 404 and self.role == "master" then
            -- not found cluster_id, try to create
            infof("%s not found", cluster_id)

            local r = self.client:create(cluster_id, self.bootstrap_tmp_uuid, {ttl=self.bootstrap_ttl})
            if has_error(r) then
                errorf("create %s failed: %s", cluster_id, error_message(r))
            else
                -- new cluster
                infof("created %s: {value=%s, ttl=%d}", cluster_id, self.bootstrap_tmp_uuid, self.bootstrap_ttl)
                return {}
            end
        else
            errorf("check %s failed: %s", cluster_id, error_message(r))
        end

        err_timeout = error_timeout(err_timeout)
    end
end

function Cluster.replication_source(self)
    -- returns replication_sources for first box.cfg{}
    if self.is_bootstrap then
        local s = self:bootstrap_replication_source()
        return self:replication_source_to_dsn(s)
    end

    local s, is_timeout = call_timeout(10, Cluster.replication_source_from_server, self)
    if not is_timeout then
        self:store_replication_source(s)
    end

    return self:replication_source_to_dsn(self.data["replication_source"])
end

-- save new sources to self.data
-- dump to file
-- returns true if changed
function Cluster.store_replication_source(self, sources)
    debugf("store_replication_source: %s", json.encode(sources))
    
    table.sort(sources)

    if self.data["replication_source"] == nil then
        self.data["replication_source"] = sources
        self:save()
        return true
    end

    if #sources ~= #self.data["replication_source"] then
        self.data["replication_source"] = sources
        self:save()
        return true
    end

    for index, s in pairs(sources) do
        if sources[index] ~= self.data["replication_source"][index] then
            self.data["replication_source"] = sources
            self:save()
            return true
        end
    end

    return false
end

-- add user/password to host:port list
function Cluster.replication_source_to_dsn(self, sources)
    if sources == nil then
        return {}
    end

    local dsn = {}

    for index, s in ipairs(sources) do
        if s ~= self.self_addr then
            if self.replica_password ~= nil and self.replica_password ~= "" then
                table.insert(dsn, self.replica_user..":"..self.replica_password.."@"..s)
            else
                table.insert(dsn, self.replica_user.."@"..s)
            end
        end
    end

    return dsn
end

function Cluster.run(self)
    
    if self.is_bootstrap then
        -- finish bootstrap
        local new_cluster_id = box.space._schema:select{'cluster'}[1][2]
        local err_timeout = 0
        while true do
            local r = self.client:cas(self:path("cluster_id"), self.bootstrap_tmp_uuid, new_cluster_id)
            if has_error(r) then
                errorf("bootstrap finish failed: %s", error_message(r))
                err_timeout = error_timeout(err_timeout)
            else
                break
            end
        end
    end

    -- recreate cluster_id if need (existing cluster moves to etcd)
    local r = self.client:get(self:path("cluster_id"))
    if r.status == 404 then
        local cluster_id = box.space._schema:select{'cluster'}[1][2]
        infof("cluster_id not found. set %s=%s", self:path("cluster_id"), cluster_id)
        self.client:set(self:path("cluster_id"), cluster_id)
    end
    
    -- register self discovery
    self.client:announce(
        self:path(uri_join(self.role, box.info.server.uuid)),
        '{"addr": "'..self.self_addr..'"}',
        {ttl = self.discovery_ttl, refresh = self.discovery_refresh}
    )

    function refresh_replication_source()
        while true do
            local sources = self:replication_source_from_server()
            
            if self:store_replication_source(sources) then
                infof("new master: %s", json.encode(sources))
                local new_source = self:replication_source_to_dsn(sources)
                if box.cfg.replication_source then
                    box.cfg{replication_source=new_source}
                else
                    box.cfg{replication=new_source}
                end
            end

            self.client:wait(self:path("/master/"), {recursive=true, timeout=60})
        end
    end

    spawn(refresh_replication_source)
end
-- </CLUSTER>



