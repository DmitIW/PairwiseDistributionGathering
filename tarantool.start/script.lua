#!/usr/bin/env tarantool

fiber = require("fiber")
expd = require("expirationd")

listen_port = os.getenv("TARANTOOL_PORT")
if listen_port == nil then
    listen_port = 3301
end

function create_field(n, t)
    return { name = n, type = t }
end

function create_format(...)
    format = {}
    for i = 1, arg.n do
        format[i] = arg[i]
    end
    return format
end

function obj2obj_dist_space_formatting(space_name, first_obj_name, second_obj_name, first_obj_type, second_obj_type)
    first_obj_type = first_obj_type or "unsigned"
    second_obj_type = second_obj_type or "unsigned"

    format = create_format(
            create_field(first_obj_name, first_obj_type),
            create_field(second_obj_name, second_obj_type),
            create_field("count", "unsigned"),
            create_field("time", "unsigned")
    )

    space = box.schema.space.create(space_name, { format = format, if_not_exists = true })
    space:create_index(
            'primary',
            {
                type = 'hash',
                parts = { first_obj_name, second_obj_name }
            }
    )
    return space
end

function current_time()
    return math.floor(fiber.time())
end

function is_tuple_expired(args, tuple)
    time_now = current_time()
    if (tuple["time"] < time_now) then
        return true
    end
    return false
end

function space_prepare(space_name, first_obj_name, second_obj_name, first_obj_type, second_obj_type)
    space = obj2obj_dist_space_formatting(space_name, first_obj_name, second_obj_name, first_obj_type, second_obj_type)
    expd.run_task(space_name, space.id, is_tuple_expired)
    return space
end

box.cfg {
    listen = listen_port,
    background = false,
}

box.once("init", function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, { if_not_exists = true })
end)

box.once("create_db", function()
    s2da = space_prepare("src2dst_at", "src_port", "dst_port")
    s2dl = space_prepare("src2dst_lg", "src_port", "dst_port")

    d2sa = space_prepare("dst2src_at", "dst_port", "src_port")
    d2sl = space_prepare("dst2src_lg", "dst_port", "src_port")

    d2pa = space_prepare("dst2proto_at", "dst_port", "proto")
    d2pl = space_prepare("dst2proto_lg", "dst_port", "proto")
end)

print("Starting on ", listen_port)