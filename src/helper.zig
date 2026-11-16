const std = @import("std");
const Crc32c = std.hash.crc.@"CRC-32/ISCSI";

pub fn tsnLte(a: u32, b: u32) bool {
    return @as(i32, @bitCast(a -% b)) <= 0;
}

pub fn tsnGt(a: u32, b: u32) bool {
    return @as(i32, @bitCast(a -% b)) > 0;
}

pub fn checkSum(buffer: []u8) void {
    std.mem.writeInt(u32, buffer[8..12], Crc32c.hash(buffer), .little);
}

pub fn getCheckSum(data: []const u8) u32 {
    var crc = Crc32c.init();
    crc.update(data[0..8]);
    crc.update(&[_]u8{ 0, 0, 0, 0 });
    crc.update(data[12..]);
    return crc.final();
}

pub const StreamArrayMap = std.ArrayHashMapUnmanaged(u16, void, StreamArrayMapContext, false);

/// A hash map keyed by stream id, stream ids are their own hash.
pub fn StreamMap(comptime V: type) type {
    return std.HashMapUnmanaged(u16, V, StreamMapContext, std.hash_map.default_max_load_percentage);
}

const StreamMapContext = struct {
    pub fn hash(_: StreamMapContext, stream_id: u16) u64 {
        return stream_id;
    }

    pub fn eql(_: StreamMapContext, a: u16, b: u16) bool {
        return a == b;
    }
};

const StreamArrayMapContext = struct {
    pub fn hash(_: StreamArrayMapContext, stream_id: u16) u32 {
        return stream_id;
    }

    pub fn eql(_: StreamArrayMapContext, a: u16, b: u16, _: usize) bool {
        return a == b;
    }
};
