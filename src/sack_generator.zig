const std = @import("std");
const testing = std.testing;
const message = @import("message.zig");
const Helper = @import("helper.zig");
const TsnBitSet = @import("tsn_bit_set.zig");

const SackGenerator = @This();

pub const bitmap_bits = TsnBitSet.bitmap_bits;
pub const max_duplicates = 16;

received: TsnBitSet,
duplicates: [max_duplicates]u32,
duplicate_count: u8,

pub fn init(initial_tsn: u32) SackGenerator {
    return .{
        .received = .init(initial_tsn),
        .duplicates = undefined,
        .duplicate_count = 0,
    };
}

pub const TsnError = error{ OutOfWindow, Duplicate };

pub fn checkTsn(self: *SackGenerator, tsn: u32) TsnError!void {
    if (Helper.tsnLte(tsn, self.received.cumulative_tsn)) {
        self.recordDuplicate(tsn);
        return error.Duplicate;
    }

    const offset = tsn -% (self.received.cumulative_tsn +% 1);
    if (offset >= bitmap_bits - 1) return error.OutOfWindow;

    if (self.received.isAcked(tsn)) {
        self.recordDuplicate(tsn);
        return error.Duplicate;
    }
}

pub fn receiveTsn(self: *SackGenerator, tsn: u32) void {
    self.received.ack(tsn);
    self.received.advanceCumulativeTsn();
}

pub fn clearDuplicates(self: *SackGenerator) void {
    self.duplicate_count = 0;
}

pub fn writeSack(self: *const SackGenerator, buffer: []u8, a_rwnd: u32) u32 {
    std.debug.assert(buffer.len >= 16);

    const cumulative_tsn = self.received.cumulative_tsn;
    buffer[0] = @intFromEnum(message.ChunkType.sack);
    buffer[1] = 0; // flags
    std.mem.writeInt(u32, buffer[4..8], cumulative_tsn, .big);
    std.mem.writeInt(u32, buffer[8..12], a_rwnd, .big);

    var written: u32 = 16;
    var gap_block_count: u16 = 0;
    var it = self.received.iterateGapBlocks();
    while (it.next()) |block| {
        if (buffer.len - written < 4) break;
        std.mem.writeInt(u16, buffer[written..][0..2], @intCast(block.start -% cumulative_tsn), .big);
        std.mem.writeInt(u16, buffer[written + 2 ..][0..2], @intCast(block.end -% cumulative_tsn), .big);
        written += 4;
        gap_block_count += 1;
    }

    var duplicate_written: u16 = 0;
    for (self.duplicates[0..self.duplicate_count]) |tsn| {
        if (buffer.len - written < 4) break;
        std.mem.writeInt(u32, buffer[written..][0..4], tsn, .big);
        written += 4;
        duplicate_written += 1;
    }

    std.mem.writeInt(u16, buffer[12..14], gap_block_count, .big);
    std.mem.writeInt(u16, buffer[14..16], duplicate_written, .big);
    std.mem.writeInt(u16, buffer[2..4], @intCast(written), .big);
    return written;
}

/// Reneg the highest received TSN
pub fn reneg(self: *SackGenerator, tsn: u32) void {
    self.received.unack(tsn);
}

fn recordDuplicate(self: *SackGenerator, tsn: u32) void {
    if (self.duplicate_count >= max_duplicates) return;
    self.duplicates[self.duplicate_count] = tsn;
    self.duplicate_count += 1;
}

fn testReceive(g: *SackGenerator, tsn: u32) !void {
    try g.checkTsn(tsn);
    g.receiveTsn(tsn);
}

test "SackGenerator.receiveTsn: in-order delivery advances the cumulative tsn" {
    var g = SackGenerator.init(1);
    try testReceive(&g, 1);
    try testing.expectEqual(@as(u32, 1), g.received.cumulative_tsn);
    try testReceive(&g, 2);
    try testing.expectEqual(@as(u32, 2), g.received.cumulative_tsn);
    try testing.expectEqual(@as(u32, 2), g.received.highest_tsn_received);
}

test "SackGenerator.receiveTsn: out-of-order arrival is buffered without advancing the cumulative tsn" {
    var g = SackGenerator.init(1);
    try testReceive(&g, 3);
    try testing.expectEqual(@as(u32, 0), g.received.cumulative_tsn);
    try testing.expectEqual(@as(u32, 3), g.received.highest_tsn_received);
    try testing.expect(g.received.isAcked(3));
}

test "SackGenerator.receiveTsn: filling a gap advances the cumulative tsn across buffered bits" {
    var g = SackGenerator.init(1);
    try testReceive(&g, 3);
    try testReceive(&g, 2);
    try testing.expectEqual(@as(u32, 0), g.received.cumulative_tsn);
    try testReceive(&g, 1);
    try testing.expectEqual(@as(u32, 3), g.received.cumulative_tsn);
}

test "SackGenerator.receiveTsn: duplicate tsn are recorded" {
    {
        var g = SackGenerator.init(1);
        try testReceive(&g, 1);
        try testing.expectError(error.Duplicate, testReceive(&g, 1));
        try testing.expectEqual(@as(usize, 1), g.duplicate_count);
        try testing.expectEqual(@as(u32, 1), g.duplicates[0]);
    }

    {
        var g = SackGenerator.init(1);
        try testReceive(&g, 3);
        try testing.expectError(error.Duplicate, testReceive(&g, 3));
        try testing.expectEqual(@as(usize, 1), g.duplicate_count);
        try testing.expectEqual(@as(u32, 3), g.duplicates[0]);
    }

    // Max duplicates
    {
        var g = SackGenerator.init(1);
        try testReceive(&g, 1);
        for (0..max_duplicates + 4) |_| try testing.expectError(error.Duplicate, testReceive(&g, 1));
        try testing.expectEqual(@as(usize, max_duplicates), g.duplicate_count);
    }
}

test "SackGenerator.receiveTsn: a tsn far beyond the bitmap window returns OutOfWindow" {
    var g = SackGenerator.init(1);
    try testing.expectError(error.OutOfWindow, testReceive(&g, 1 + bitmap_bits));
}

test "SackGenerator.writeSack: writeSack encodes cumulative tsn, gap ack blocks, and duplicates" {
    var g = SackGenerator.init(1);
    try testReceive(&g, 1);
    try testReceive(&g, 3);
    try testReceive(&g, 4);
    try testing.expectError(error.Duplicate, testReceive(&g, 3)); // duplicate

    var buffer: [64]u8 = undefined;
    const chunk = buffer[0..g.writeSack(&buffer, 1500)];

    try testing.expectEqual(@intFromEnum(message.ChunkType.sack), chunk[0]);
    try testing.expectEqual(0, chunk[1]);
    try testing.expectEqual(chunk.len, std.mem.readInt(u16, chunk[2..4], .big));
    try testing.expectEqual(1, std.mem.readInt(u32, chunk[4..8], .big));
    try testing.expectEqual(1500, std.mem.readInt(u32, chunk[8..12], .big));
    try testing.expectEqual(1, std.mem.readInt(u16, chunk[12..14], .big));
    try testing.expectEqual(1, std.mem.readInt(u16, chunk[14..16], .big));
    try testing.expectEqual(2, std.mem.readInt(u16, chunk[16..18], .big));
    try testing.expectEqual(3, std.mem.readInt(u16, chunk[18..20], .big));
    try testing.expectEqual(3, std.mem.readInt(u32, chunk[20..24], .big));
    try testing.expectEqual(24, chunk.len);
}

test "SackGenerator.writeSack: writeSack truncates gap ack blocks and duplicates that don't fit" {
    var g = SackGenerator.init(1);
    try testReceive(&g, 3);
    try testReceive(&g, 5);
    try testReceive(&g, 1);
    try testing.expectError(error.Duplicate, testReceive(&g, 1));

    // Room for the fixed header plus exactly one gap ack block, nothing else.
    var buffer: [20]u8 = undefined;
    const chunk = buffer[0..g.writeSack(&buffer, 0)];

    try testing.expectEqual(20, chunk.len);
    try testing.expectEqual(20, std.mem.readInt(u16, chunk[2..4], .big));
    try testing.expectEqual(1, std.mem.readInt(u16, chunk[12..14], .big));
    try testing.expectEqual(0, std.mem.readInt(u16, chunk[14..16], .big));
}

test "SackGenerator.receiveTsn: tsn wraparound near 0xFFFFFFFF is handled correctly" {
    var g = SackGenerator.init(0xFFFFFFFF);
    try testing.expectEqual(@as(u32, 0xFFFFFFFE), g.received.cumulative_tsn);
    try testReceive(&g, 0xFFFFFFFF);
    try testing.expectEqual(@as(u32, 0xFFFFFFFF), g.received.cumulative_tsn);
    try testReceive(&g, 0);
    try testing.expectEqual(@as(u32, 0), g.received.cumulative_tsn);
    try testing.expectEqual(@as(u32, 0), g.received.highest_tsn_received);
}

test "SackGenerator.writeSack: gap ack blocks are offsets from the cumulative tsn and round-trip through the parser" {
    var g = SackGenerator.init(1);
    try testReceive(&g, 1);
    try testReceive(&g, 3);
    try testReceive(&g, 4);
    try testReceive(&g, 7);

    var buffer: [64]u8 = undefined;
    const n_written = g.writeSack(&buffer, 1500);
    const sack = try message.Sack.parse(buffer[4..n_written]);

    var acked: [4]u32 = undefined;
    var n: usize = 0;
    var it = sack.iterateGapAckBlocks();
    while (try it.next()) |tsn| : (n += 1) acked[n] = tsn;

    try testing.expectEqualSlices(u32, &.{ 3, 4, 7 }, acked[0..n]);
}

test "SackGenerator.receiveTsn: the furthest tsn inside the window still fits in a gap ack block" {
    var g = SackGenerator.init(1);
    const last = g.received.cumulative_tsn +% (bitmap_bits - 1);
    try testReceive(&g, last);
    try testing.expectError(error.OutOfWindow, testReceive(&g, last +% 1));

    var buffer: [64]u8 = undefined;
    try testing.expectEqual(20, g.writeSack(&buffer, 0));
    try testing.expectEqual(bitmap_bits - 1, std.mem.readInt(u16, buffer[16..18], .big));
    try testing.expectEqual(bitmap_bits - 1, std.mem.readInt(u16, buffer[18..20], .big));
}

test "SackGenerator.reneg: reneg received tsns" {
    var g = SackGenerator.init(1);
    try testReceive(&g, 1);
    try testReceive(&g, 2);
    try testReceive(&g, 3);
    try testReceive(&g, 5);
    try testReceive(&g, 7);

    try testing.expect(g.received.isAcked(7));
    try testing.expectEqual(@as(u32, 7), g.received.highest_tsn_received);

    g.reneg(7);
    try testing.expect(!g.received.isAcked(7));
    try testing.expectEqual(@as(u32, 5), g.received.highest_tsn_received);

    g.reneg(5);
    try testing.expect(!g.received.isAcked(5));
    try testing.expectEqual(@as(u32, 3), g.received.highest_tsn_received);

    try testReceive(&g, 6);
    try testReceive(&g, 8);
    g.reneg(6);
    try testing.expect(!g.received.isAcked(6));
    try testing.expectEqual(@as(u32, 8), g.received.highest_tsn_received);
}
