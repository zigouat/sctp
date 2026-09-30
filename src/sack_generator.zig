const std = @import("std");
const testing = std.testing;
const message = @import("message.zig");
const Helper = @import("helper.zig");

const SackGenerator = @This();

pub const bitmap_bits = 1024;
pub const max_duplicates = 16;

cumulative_tsn: u32,
highest_tsn_received: u32,
bitmap: std.StaticBitSet(bitmap_bits),
duplicates: [max_duplicates]u32,
duplicate_count: u8,

pub fn init(initial_tsn: u32) SackGenerator {
    return .{
        .cumulative_tsn = initial_tsn -% 1,
        .highest_tsn_received = initial_tsn -% 1,
        .bitmap = .empty,
        .duplicates = undefined,
        .duplicate_count = 0,
    };
}

pub fn receiveTsn(self: *SackGenerator, tsn: u32) error{ OutOfWindow, Duplicate }!void {
    if (Helper.tsnLte(tsn, self.cumulative_tsn)) {
        self.recordDuplicate(tsn);
        return error.Duplicate;
    }

    const offset = tsn -% (self.cumulative_tsn +% 1);
    if (offset >= bitmap_bits - 1) return error.OutOfWindow;

    const index = bitIndex(tsn);
    if (self.isBitSet(index)) {
        self.recordDuplicate(tsn);
        return error.Duplicate;
    }
    self.bitmap.set(index);

    if (Helper.tsnGt(tsn, self.highest_tsn_received)) self.highest_tsn_received = tsn;

    while (true) {
        const next_index = bitIndex(self.cumulative_tsn +% 1);
        if (!self.isBitSet(next_index)) break;
        self.bitmap.unset(next_index);
        self.cumulative_tsn +%= 1;
    }
}

pub fn clearDuplicates(self: *SackGenerator) void {
    self.duplicate_count = 0;
}

pub fn writeSack(self: *const SackGenerator, buffer: []u8, a_rwnd: u32) usize {
    std.debug.assert(buffer.len >= 16);

    buffer[0] = @intFromEnum(message.ChunkType.sack);
    buffer[1] = 0; // flags
    std.mem.writeInt(u32, buffer[4..8], self.cumulative_tsn, .big);
    std.mem.writeInt(u32, buffer[8..12], a_rwnd, .big);

    var written: usize = 16;
    var gap_block_count: u16 = 0;
    const limit = self.gapScanLimit();
    var offset: u32 = 1;
    while (offset < limit) : (offset += 1) {
        const index = bitIndex(self.cumulative_tsn +% offset);
        if (!self.isBitSet(index)) continue;

        const start = offset;
        while (offset + 1 < limit and
            self.isBitSet(bitIndex(self.cumulative_tsn +% (offset + 1)))) : (offset += 1)
        {}

        if (buffer.len - written < 4) break;
        std.mem.writeInt(u16, buffer[written..][0..2], @intCast(start), .big);
        std.mem.writeInt(u16, buffer[written + 2 ..][0..2], @intCast(offset), .big);
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

fn bitIndex(tsn: u32) u16 {
    return @intCast(tsn % bitmap_bits);
}

fn isBitSet(self: *const SackGenerator, index: u16) bool {
    const Bitmap = @TypeOf(self.bitmap);
    const mask = self.bitmap.masks[index / @bitSizeOf(Bitmap.MaskInt)];
    return (mask >> @as(Bitmap.ShiftInt, @truncate(index))) & 1 != 0;
}

fn gapScanLimit(self: *const SackGenerator) u32 {
    return (self.highest_tsn_received -% self.cumulative_tsn) + 1;
}

fn recordDuplicate(self: *SackGenerator, tsn: u32) void {
    if (self.duplicate_count >= max_duplicates) return;
    self.duplicates[self.duplicate_count] = tsn;
    self.duplicate_count += 1;
}

test "in-order delivery advances the cumulative tsn" {
    var g = SackGenerator.init(1);
    try g.receiveTsn(1);
    try testing.expectEqual(@as(u32, 1), g.cumulative_tsn);
    try g.receiveTsn(2);
    try testing.expectEqual(@as(u32, 2), g.cumulative_tsn);
    try testing.expectEqual(@as(u32, 2), g.highest_tsn_received);
}

test "out-of-order arrival is buffered without advancing the cumulative tsn" {
    var g = SackGenerator.init(1);
    try g.receiveTsn(3);
    try testing.expectEqual(@as(u32, 0), g.cumulative_tsn);
    try testing.expectEqual(@as(u32, 3), g.highest_tsn_received);
    try testing.expect(g.bitmap.isSet(3));
}

test "filling a gap advances the cumulative tsn across buffered bits" {
    var g = SackGenerator.init(1);
    try g.receiveTsn(3);
    try g.receiveTsn(2);
    try testing.expectEqual(@as(u32, 0), g.cumulative_tsn);
    try g.receiveTsn(1);
    try testing.expectEqual(@as(u32, 3), g.cumulative_tsn);
}

test "duplicate of an already cumulatively-acked tsn is recorded" {
    var g = SackGenerator.init(1);
    try g.receiveTsn(1);
    try testing.expectError(error.Duplicate, g.receiveTsn(1));
    try testing.expectEqual(@as(usize, 1), g.duplicate_count);
    try testing.expectEqual(@as(u32, 1), g.duplicates[0]);
}

test "duplicate of an already buffered out-of-order tsn is recorded" {
    var g = SackGenerator.init(1);
    try g.receiveTsn(3);
    try testing.expectError(error.Duplicate, g.receiveTsn(3));
    try testing.expectEqual(@as(usize, 1), g.duplicate_count);
    try testing.expectEqual(@as(u32, 3), g.duplicates[0]);
}

test "duplicate recording is capped at max_duplicates" {
    var g = SackGenerator.init(1);
    try g.receiveTsn(1);
    for (0..max_duplicates + 4) |_| try testing.expectError(error.Duplicate, g.receiveTsn(1));
    try testing.expectEqual(@as(usize, max_duplicates), g.duplicate_count);
}

test "a tsn far beyond the bitmap window returns OutOfWindow" {
    var g = SackGenerator.init(1);
    try testing.expectError(error.OutOfWindow, g.receiveTsn(1 + bitmap_bits));
}

test "writeSack encodes cumulative tsn, gap ack blocks, and duplicates" {
    var g = SackGenerator.init(1);
    try g.receiveTsn(1);
    try g.receiveTsn(3);
    try g.receiveTsn(4);
    try testing.expectError(error.Duplicate, g.receiveTsn(3)); // duplicate

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

test "writeSack truncates gap ack blocks and duplicates that don't fit" {
    var g = SackGenerator.init(1);
    try g.receiveTsn(3);
    try g.receiveTsn(5);
    try g.receiveTsn(1);
    try testing.expectError(error.Duplicate, g.receiveTsn(1));

    // Room for the fixed header plus exactly one gap ack block, nothing else.
    var buffer: [20]u8 = undefined;
    const chunk = buffer[0..g.writeSack(&buffer, 0)];

    try testing.expectEqual(20, chunk.len);
    try testing.expectEqual(20, std.mem.readInt(u16, chunk[2..4], .big));
    try testing.expectEqual(1, std.mem.readInt(u16, chunk[12..14], .big));
    try testing.expectEqual(0, std.mem.readInt(u16, chunk[14..16], .big));
}

test "tsn wraparound near 0xFFFFFFFF is handled correctly" {
    var g = SackGenerator.init(0xFFFFFFFF);
    try testing.expectEqual(@as(u32, 0xFFFFFFFE), g.cumulative_tsn);
    try g.receiveTsn(0xFFFFFFFF);
    try testing.expectEqual(@as(u32, 0xFFFFFFFF), g.cumulative_tsn);
    try g.receiveTsn(0);
    try testing.expectEqual(@as(u32, 0), g.cumulative_tsn);
    try testing.expectEqual(@as(u32, 0), g.highest_tsn_received);
}

test "gap ack blocks are offsets from the cumulative tsn and round-trip through the parser" {
    var g = SackGenerator.init(1);
    try g.receiveTsn(1);
    try g.receiveTsn(3);
    try g.receiveTsn(4);
    try g.receiveTsn(7);

    var buffer: [64]u8 = undefined;
    const n_written = g.writeSack(&buffer, 1500);
    const sack = try message.Sack.parse(buffer[4..n_written]);

    var acked: [4]u32 = undefined;
    var n: usize = 0;
    var it = sack.iterateGapAckBlocks();
    while (try it.next()) |tsn| : (n += 1) acked[n] = tsn;

    try testing.expectEqualSlices(u32, &.{ 3, 4, 7 }, acked[0..n]);
}

test "the furthest tsn inside the window still fits in a gap ack block" {
    var g = SackGenerator.init(1);
    const last = g.cumulative_tsn +% (bitmap_bits - 1);
    try g.receiveTsn(last);
    try testing.expectError(error.OutOfWindow, g.receiveTsn(last +% 1));

    var buffer: [64]u8 = undefined;
    try testing.expectEqual(20, g.writeSack(&buffer, 0));
    try testing.expectEqual(bitmap_bits - 1, std.mem.readInt(u16, buffer[16..18], .big));
    try testing.expectEqual(bitmap_bits - 1, std.mem.readInt(u16, buffer[18..20], .big));
}
