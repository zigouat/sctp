const std = @import("std");

const Crc32c = @This();

const Soft = std.hash.crc.Crc(u32, .{
    .polynomial = 0x1EDC6F41,
    .initial = 0xffffffff,
    .reflect_input = true,
    .reflect_output = true,
    .xor_output = 0xffffffff,
});

crc: u32,

pub fn init() Crc32c {
    return .{ .crc = 0xffffffff };
}

pub fn update(self: *Crc32c, data: []const u8) void {
    var crc: u64 = self.crc;
    var rest = data;
    while (rest.len >= 8) : (rest = rest[8..]) {
        crc = crc32q(crc, std.mem.readInt(u64, rest[0..8], .little));
    }

    var crc_32: u32 = @truncate(crc);
    for (rest) |byte| crc_32 = crc32b(crc_32, byte);
    self.crc = crc_32;
}

pub fn final(self: Crc32c) u32 {
    return ~self.crc;
}

pub fn hash(data: []const u8) u32 {
    var c = init();
    c.update(data);
    return c.final();
}

inline fn crc32q(crc: u64, value: u64) u64 {
    return asm ("crc32 %[value], %[out]"
        : [out] "=r" (-> u64),
        : [value] "r" (value),
          [crc] "0" (crc),
    );
}

inline fn crc32b(crc: u32, value: u8) u32 {
    return asm ("crc32 %[value], %[out]"
        : [out] "=r" (-> u32),
        : [value] "r" (value),
          [crc] "0" (crc),
    );
}

const testing = std.testing;

test "Crc32c: check value" {
    try testing.expectEqual(0xE3069283, Crc32c.hash("123456789"));
}

test "Crc32c: matches the software implementation for every length and alignment" {
    var buffer: [256]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(0);
    prng.random().bytes(&buffer);

    for (0..16) |offset| {
        for (offset..buffer.len) |end| {
            const data = buffer[offset..end];
            try testing.expectEqual(Soft.hash(data), Crc32c.hash(data));
        }
    }
}

test "Crc32c: incremental updates match a single update" {
    var buffer: [100]u8 = undefined;
    var prng = std.Random.DefaultPrng.init(1);
    prng.random().bytes(&buffer);

    for (0..buffer.len) |split| {
        var c = Crc32c.init();
        c.update(buffer[0..split]);
        c.update(buffer[split..]);
        try testing.expectEqual(Soft.hash(&buffer), c.final());
    }
}
