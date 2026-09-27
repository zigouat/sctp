const std = @import("std");

const Crc32c = @import("std").hash.crc.Crc(u32, .{
    .polynomial = 0x1EDC6F41,
    .initial = 0xffffffff,
    .reflect_input = true,
    .reflect_output = true,
    .xor_output = 0xffffffff,
});

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
