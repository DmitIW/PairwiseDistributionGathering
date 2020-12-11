#!/usr/bin/env tarantool

listen_port = os.getenv("TARANTOOL_PORT")
if listen_port == nil then
    listen_port = 3301
end

box.cfg {
    listen = listen_port,
    background = false,
}

box.once("init", function()
    box.schema.user.grant('guest', 'read,write,execute', 'universe', nil, { if_not_exists = true })
end)

function obj2obj_dist_space_formatting(space_name, first_obj_name, second_obj_name, first_obj_type, second_obj_type)
    first_obj_type = first_obj_type or "unsigned"
    second_obj_type = second_obj_type or "unsigned"

    space = box.schema.space.create(space_name, { if_not_exists = true })
    space:format({
        { name = first_obj_name, type = first_obj_type },
        { name = second_obj_name, type = second_obj_type },
        { name = 'count', type = 'unsigned' }
    })
    space:create_index(
            'primary',
            {
                type = 'hash',
                parts = { first_obj_name, second_obj_name }
            }
    )
    return space
end

box.once("create_db", function()
    s2da = obj2obj_dist_space_formatting("src2dst_at", "src_port", "dst_port")
    s2dl = obj2obj_dist_space_formatting("src2dst_lg", "src_port", "dst_port")

    d2sa = obj2obj_dist_space_formatting("dst2src_at", "dst_port", "src_port")
    d2sl = obj2obj_dist_space_formatting("dst2src_lg", "dst_port", "src_port")

    d2pa = obj2obj_dist_space_formatting("dst2proto_at", "dst_port", "proto")
    d2pl = obj2obj_dist_space_formatting("dst2proto_lg", "dst_port", "proto")
end)

print("Starting on ", listen_port)