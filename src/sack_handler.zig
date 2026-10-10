const std = @import("std");
const testing = std.testing;
const message = @import("message.zig");
const Helper = @import("helper.zig");

const SackHandler = @This();

pub const bitmap_bits = 1024;

cumulative_tsn: u32,
highest_tsn_received: u32,
bitmap: std.bit_set.StaticBitSet(bitmap_bits),
all_acked: bool,

pub fn init(initial_tsn: u32) SackHandler {
    return .{
        .cumulative_tsn = initial_tsn -% 1,
        .highest_tsn_received = initial_tsn -% 1,
        .bitmap = .empty,
        .all_acked = true,
    };
}

pub fn handleSack(self: *SackHandler, sack: *const message.Sack, highest_tsn_sent: u32) error{Discard}!void {
    if (Helper.tsnGt(self.cumulative_tsn, sack.cumulative_tsn)) return error.Discard;
    if (Helper.tsnGt(sack.cumulative_tsn, highest_tsn_sent)) return error.Discard;

    var it = sack.iterateGapAckBlocks();
    while (it.nextBlock() catch return error.Discard) |block| {
        if (Helper.tsnGt(block.end, highest_tsn_sent)) return error.Discard;
    }

    if (Helper.tsnGt(self.highest_tsn_received, self.cumulative_tsn)) {
        self.setRange(self.cumulative_tsn +% 1, self.highest_tsn_received, false);
    }

    self.cumulative_tsn = sack.cumulative_tsn;
    self.highest_tsn_received = sack.cumulative_tsn;

    var last_block = message.Sack.GapAckBlockIterator.Block{
        .start = sack.cumulative_tsn,
        .end = sack.cumulative_tsn,
    };
    self.all_acked = true;

    it = sack.iterateGapAckBlocks();
    while (it.nextBlock() catch unreachable) |block| {
        self.setRange(block.start, block.end, true);
        if (Helper.tsnGt(block.end, self.highest_tsn_received)) self.highest_tsn_received = block.end;

        self.all_acked &= block.start == last_block.end +% 1;
        last_block = block;
    }
    self.all_acked &= last_block.end == highest_tsn_sent;
}

pub fn handleCumulativeTsn(self: *SackHandler, cumulative_tsn: u32, highest_tsn_sent: u32) void {
    if (!Helper.tsnGt(cumulative_tsn, self.cumulative_tsn)) return;
    self.setRange(self.cumulative_tsn +% 1, cumulative_tsn, false);
    self.cumulative_tsn = cumulative_tsn;
    if (Helper.tsnGt(cumulative_tsn, self.highest_tsn_received)) self.highest_tsn_received = cumulative_tsn;
    self.all_acked = cumulative_tsn == highest_tsn_sent;
}

pub fn isAcked(self: *const SackHandler, tsn: u32) bool {
    if (Helper.tsnLte(tsn, self.cumulative_tsn)) return true;
    if (Helper.tsnGt(tsn, self.highest_tsn_received)) return false;
    return self.isBitSet(bitIndex(tsn));
}

fn setRange(self: *SackHandler, start_tsn: u32, end_tsn: u32, value: bool) void {
    const start_index = bitIndex(start_tsn);
    const end_index = bitIndex(end_tsn);

    if (start_index <= end_index) {
        self.bitmap.setRangeValue(.{ .start = start_index, .end = @as(usize, end_index) + 1 }, value);
    } else {
        self.bitmap.setRangeValue(.{ .start = start_index, .end = bitmap_bits }, value);
        self.bitmap.setRangeValue(.{ .start = 0, .end = @as(usize, end_index) + 1 }, value);
    }
}

fn bitIndex(tsn: u32) u16 {
    return @intCast(tsn % bitmap_bits);
}

inline fn isBitSet(self: *const SackHandler, index: u16) bool {
    const Bitmap = @TypeOf(self.bitmap);
    const mask = self.bitmap.masks[index / @bitSizeOf(Bitmap.MaskInt)];
    return (mask >> @as(Bitmap.ShiftInt, @truncate(index))) & 1 != 0;
}

fn testSack(cumulative_tsn: u32, blocks: []const u8) message.Sack {
    return .{
        .cumulative_tsn = cumulative_tsn,
        .a_rwnd = 0,
        .gap_ack_blocks = blocks,
        .duplicate_tsns = &.{},
    };
}

test "SackHandler.handleSack: gap ack blocks mark tsns as acked" {
    var h = SackHandler.init(1);
    try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3 }), 100);

    try testing.expect(h.isAcked(1));
    try testing.expect(!h.isAcked(2));
    try testing.expect(h.isAcked(3));
    try testing.expect(h.isAcked(4));
    try testing.expect(!h.isAcked(5));
}

test "SackHandler.handleSack: a sack with an unchanged cumulative tsn still updates gap blocks" {
    var h = SackHandler.init(1);
    try h.handleSack(&testSack(1, &.{ 0, 2, 0, 2 }), 100);
    try h.handleSack(&testSack(1, &.{ 0, 2, 0, 4 }), 100);

    try testing.expect(h.isAcked(3));
    try testing.expect(h.isAcked(5));
}

test "SackHandler.handleSack: a sack with an older cumulative tsn is discarded" {
    var h = SackHandler.init(1);
    try h.handleSack(&testSack(5, &.{}), 100);
    try testing.expectError(error.Discard, h.handleSack(&testSack(4, &.{}), 100));
    try testing.expectEqual(5, h.cumulative_tsn);
}

test "SackHandler.handleSack: bits acked by old gap blocks are cleared once the cumulative tsn passes them" {
    var h = SackHandler.init(1);
    try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3 }), 100);
    try h.handleSack(&testSack(4, &.{}), 100);

    try testing.expect(!h.isBitSet(3));
    try testing.expect(!h.isBitSet(4));
    try testing.expect(!h.isAcked(3 + bitmap_bits));
}

test "SackHandler.handleSack: reneged gap blocks are no longer acked" {
    var h = SackHandler.init(1);
    try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3 }), 100);
    try h.handleSack(&testSack(1, &.{}), 100);

    try testing.expect(!h.isAcked(3));
    try testing.expect(!h.isAcked(4));
}

test "SackHandler.handleSack: an invalid gap block discards the sack without changing state" {
    var h = SackHandler.init(1);
    try h.handleSack(&testSack(1, &.{ 0, 2, 0, 2 }), 100);
    try testing.expectError(error.Discard, h.handleSack(&testSack(3, &.{ 0, 4, 0, 1 }), 100));

    try testing.expectEqual(1, h.cumulative_tsn);
    try testing.expect(h.isAcked(3));
}

test "SackHandler.handleSack: a sack acknowledging tsns that were never sent is discarded" {
    var h = SackHandler.init(1);
    try testing.expectError(error.Discard, h.handleSack(&testSack(6, &.{}), 5));
    try testing.expectError(error.Discard, h.handleSack(&testSack(1, &.{ 0, 2, 0, 5 }), 5));

    try testing.expectEqual(0, h.cumulative_tsn);
    try testing.expect(!h.isAcked(3));
}

test "SackHandler.handleSack: set all_acked" {
    // the cumulative tsn reaches the highest tsn sent
    {
        var h = SackHandler.init(1);
        try h.handleSack(&testSack(10, &.{}), 10);
        try testing.expect(h.all_acked);
    }

    // contiguous gap blocks reach the highest tsn sent
    {
        var h = SackHandler.init(1);
        try h.handleSack(&testSack(1, &.{ 0, 1, 0, 3, 0, 4, 0, 5 }), 6);
        try testing.expect(h.all_acked);
    }
}

test "SackHandler.handleSack: clear all_acked" {
    // a hole or a trailing unacked tsn
    {
        var h = SackHandler.init(1);
        try h.handleSack(&testSack(1, &.{ 0, 2, 0, 5 }), 6);
        try testing.expect(!h.all_acked);

        try h.handleSack(&testSack(1, &.{ 0, 1, 0, 4 }), 6);
        try testing.expect(!h.all_acked);
    }

    // reneged gap blocks
    {
        var h = SackHandler.init(1);
        try h.handleSack(&testSack(1, &.{ 0, 1, 0, 5 }), 6);
        try testing.expect(h.all_acked);

        try h.handleSack(&testSack(1, &.{}), 6);
        try testing.expect(!h.all_acked);
    }
}

test "SackHandler.handleCumulativeTsn: cumulative tsn clears the gap bits it passes" {
    var h = SackHandler.init(1);
    try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3, 0, 5, 0, 5 }), 100);
    h.handleCumulativeTsn(4, 100);

    try testing.expectEqual(4, h.cumulative_tsn);
    try testing.expect(!h.isBitSet(3));
    try testing.expect(!h.isBitSet(4));
    try testing.expect(h.isAcked(6));
    try testing.expect(!h.isAcked(3 + bitmap_bits));
}

test "SackHandler.handleCumulativeTsn: cumulative tsn past the highest tsn updates it" {
    var h = SackHandler.init(1);
    h.handleCumulativeTsn(10, 100);

    try testing.expectEqual(10, h.highest_tsn_received);
    try h.handleSack(&testSack(10, &.{ 0, 2, 0, 2 }), 100);
    try testing.expect(h.isAcked(12));
    try testing.expect(!h.isAcked(11));
}

test "SackHandler.handleCumulativeTsn: older cumulative tsn is ignored" {
    var h = SackHandler.init(1);
    try h.handleSack(&testSack(5, &.{}), 100);
    h.handleCumulativeTsn(3, 100);

    try testing.expectEqual(5, h.cumulative_tsn);
}

test "SackHandler.handleCumulativeTsn: all acked only when it reaches the highest tsn sent" {
    var h = SackHandler.init(1);
    h.handleCumulativeTsn(5, 10);
    try testing.expect(!h.all_acked);

    h.handleCumulativeTsn(10, 10);
    try testing.expect(h.all_acked);
}
