const std = @import("std");
const builtin = @import("builtin");
const testing = std.testing;

const Soft = std.hash.crc.Crc(u32, .{
    .polynomial = 0x1EDC6F41,
    .initial = 0xffffffff,
    .reflect_input = true,
    .reflect_output = true,
    .xor_output = 0xffffffff,
});

pub const Crc32c = if (builtin.cpu.has(.x86, .crc32)) @import("crc32c/x86.zig") else Soft;
