local get_files = {
    "/small_a.html",
    "/small_b.css",
    "/small_c.js",
    "/medium_a.bin",
    "/medium_b.bin",
    "/large_a.bin",
    "/large_b.bin",
}

local function weighted_get_file()
    local roll = math.random(100)

    if roll <= 70 then
        -- 70%: small files
        return get_files[math.random(3)]

    elseif roll <= 90 then
        -- 20%: medium files
        return get_files[math.random(4, 5)]

    else
        -- 10%: large files
        return get_files[math.random(6, 7)]
    end
end
local upload_targets = {}
for i = 1, 20 do
    upload_targets[i] = "/upload/bench_upload_" .. i .. ".bin"
end


local payload_small = string.rep("x", 2000)
local payload_medium = string.rep("y", 200000)

math.randomseed(os.time())

request = function()
    -- local roll = math.random(100)
    local roll = 80
    if roll <= 85 then
        -- GET
        local path = weighted_get_file()
        return wrk.format("GET", path)
    else
        -- POST upload
        local path = upload_targets[math.random(#upload_targets)]
        local body
        if math.random(100) <= 70 then
            body = payload_small
        else
            body = payload_medium
        end
        wrk.headers["Content-Type"] = "application/octet-stream"
        return wrk.format("POST", path, wrk.headers, body)
    end
end


done = function(summary, latency, requests)
    io.write("------------------------------\n")
    io.write(string.format("Total requests: %d\n", summary.requests))
    io.write(string.format("Total errors (non-2xx counted separately by wrk): %d\n", summary.errors.status))
    io.write("------------------------------\n")
end