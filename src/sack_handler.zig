const std = @import("std");
const testing = std.testing;
const message = @import("message.zig");
const Helper = @import("helper.zig");

const SackHandler = @This();
const BitSet = std.bit_set.ArrayBitSet(usize, bitmap_bits);

pub const bitmap_bits = 1024;

cumulative_tsn: u32,
/// The highest tsn received in any sack, including the cumulative tsn.
highest_tsn_received: u32,
/// BitSet of tsns acked by gap blocks.
acked_tsn: BitSet,
/// 2-bit saturating miss indication counter per tsn, split into a low and a high bit plane.
miss_lo: BitSet,
miss_hi: BitSet,
/// tsns already fast retransmitted, they are not eligible for another fast retransmit.
fast_retransmitted: BitSet,

/// Iterates the tsns acked by gap blocks, i.e. the set bits in (cumulative_tsn, highest_tsn_received], in tsn order.
pub const GapAckedIterator = struct {
    const MaskInt = BitSet.MaskInt;
    const ShiftInt = BitSet.ShiftInt;
    const word_bits = @bitSizeOf(MaskInt);

    handler: *const SackHandler,
    /// the next tsn to examine
    next_tsn: u32,
    /// the number of tsns left to examine
    remaining: u32,

    fn init(handler: *const SackHandler) GapAckedIterator {
        return .{
            .handler = handler,
            .next_tsn = handler.cumulative_tsn +% 1,
            .remaining = handler.highest_tsn_received -% handler.cumulative_tsn,
        };
    }

    pub fn next(self: *GapAckedIterator) ?u32 {
        while (self.remaining > 0) {
            // bitmap_bits is a multiple of word_bits, so a word never crosses the end of the ring.
            const index = bitIndex(self.next_tsn);
            const shift: ShiftInt = @intCast(index % word_bits);
            const len = @min(word_bits - @as(u32, shift), self.remaining);

            var word = self.handler.acked_tsn.masks[index / word_bits] >> shift;
            if (len < word_bits) word &= (@as(MaskInt, 1) << @intCast(len)) - 1;

            if (word == 0) {
                self.next_tsn +%= len;
                self.remaining -= len;
                continue;
            }

            const skip: u32 = @ctz(word);
            const tsn = self.next_tsn +% skip;
            self.next_tsn = tsn +% 1;
            self.remaining -= skip + 1;
            return tsn;
        }
        return null;
    }
};

pub fn init(initial_tsn: u32) SackHandler {
    return .{
        .cumulative_tsn = initial_tsn -% 1,
        .highest_tsn_received = initial_tsn -% 1,
        .acked_tsn = .empty,
        .miss_lo = .empty,
        .miss_hi = .empty,
        .fast_retransmitted = .empty,
    };
}

pub fn handleSack(self: *SackHandler, sack: *const message.Sack, highest_tsn_sent: u32) error{Discard}!bool {
    if (Helper.tsnGt(self.cumulative_tsn, sack.cumulative_tsn)) return error.Discard;
    if (Helper.tsnGt(sack.cumulative_tsn, highest_tsn_sent)) return error.Discard;

    var prev_end = sack.cumulative_tsn;
    var validate_it = sack.iterateGapAckBlocks();
    while (validate_it.nextBlock() catch return error.Discard) |block| {
        if (Helper.tsnLte(block.start, prev_end)) return error.Discard;
        if (Helper.tsnGt(block.end, highest_tsn_sent)) return error.Discard;
        prev_end = block.end;
    }

    self.clearRange(self.cumulative_tsn, sack.cumulative_tsn);
    self.cumulative_tsn = sack.cumulative_tsn;
    self.highest_tsn_received = sack.cumulative_tsn;

    var start_tsn = self.cumulative_tsn +% 1;
    var htna: ?u32 = null;
    var it = sack.iterateGapAckBlocks();
    while (it.nextBlock() catch unreachable) |block| {
        self.highest_tsn_received = block.end;
        setRange(&self.acked_tsn, start_tsn, block.start -% 1, false);
        start_tsn = block.end +% 1;
        htna = self.highestUnacked(block.start, block.end) orelse continue;
        setRange(&self.acked_tsn, block.start, htna.?, true);
    }

    const tsn = htna orelse return false;
    return self.incrementMissCounts(self.cumulative_tsn +% 1, tsn);
}

pub fn handleCumulativeTsn(self: *SackHandler, cumulative_tsn: u32) void {
    if (!Helper.tsnGt(cumulative_tsn, self.cumulative_tsn)) return;
    self.clearRange(self.cumulative_tsn +% 1, cumulative_tsn);
    self.cumulative_tsn = cumulative_tsn;
    if (Helper.tsnGt(cumulative_tsn, self.highest_tsn_received)) self.highest_tsn_received = cumulative_tsn;
}

pub fn isAcked(self: *const SackHandler, tsn: u32) bool {
    if (Helper.tsnLte(tsn, self.cumulative_tsn)) return true;
    if (Helper.tsnGt(tsn, self.highest_tsn_received)) return false;
    return self.isBitSet(bitIndex(tsn));
}

pub fn isRangeAcked(self: *const SackHandler, start_tsn: u32, end_tsn: u32) bool {
    const MaskInt = BitSet.MaskInt;
    const word_bits = @bitSizeOf(MaskInt);

    if (Helper.tsnLte(end_tsn, self.cumulative_tsn)) return true;

    var tsn = if (Helper.tsnLte(start_tsn, self.cumulative_tsn)) self.cumulative_tsn +% 1 else start_tsn;
    var remaining = end_tsn -% tsn +% 1;
    while (remaining > 0) {
        const index = bitIndex(tsn);
        const shift: BitSet.ShiftInt = @intCast(index % word_bits);
        const len = @min(word_bits - @as(u32, shift), remaining);

        var unacked = ~self.acked_tsn.masks[index / word_bits] >> shift;
        if (len < word_bits) unacked &= (@as(MaskInt, 1) << @intCast(len)) - 1;
        if (unacked != 0) return false;

        tsn +%= len;
        remaining -= len;
    }

    return true;
}

pub fn allAcked(self: *const SackHandler, highest_tsn_sent: u32) bool {
    return self.highest_tsn_received == highest_tsn_sent and self.isRangeAcked(self.cumulative_tsn, highest_tsn_sent);
}

pub fn iterateGapAcked(self: *const SackHandler) GapAckedIterator {
    return GapAckedIterator.init(self);
}

pub fn markFastRetransmitted(self: *SackHandler, tsn: u32) void {
    self.fast_retransmitted.set(bitIndex(tsn));
}

pub fn clearFastRetransmit(self: *SackHandler) void {
    self.miss_lo = .empty;
    self.miss_hi = .empty;
    self.fast_retransmitted = .empty;
}

pub fn firstTsnWithThreeMisses(self: *const SackHandler, start_tsn: u32) ?u32 {
    const MaskInt = BitSet.MaskInt;
    const word_bits = @bitSizeOf(MaskInt);

    if (Helper.tsnGt(start_tsn, self.highest_tsn_received)) return null;

    var tsn = start_tsn;
    var remaining = self.highest_tsn_received -% start_tsn;
    while (remaining > 0) {
        const index = bitIndex(tsn);
        const word_index = index / word_bits;
        const shift: BitSet.ShiftInt = @intCast(index % word_bits);
        const len = @min(word_bits - @as(u32, shift), remaining);

        const candidates = self.miss_hi.masks[word_index] & self.miss_lo.masks[word_index] &
            ~self.acked_tsn.masks[word_index] & ~self.fast_retransmitted.masks[word_index];
        var word = candidates >> shift;
        if (len < word_bits) word &= (@as(MaskInt, 1) << @intCast(len)) - 1;
        if (word != 0) return tsn +% @ctz(word);

        tsn +%= len;
        remaining -= len;
    }

    return null;
}

fn highestUnacked(self: *const SackHandler, start_tsn: u32, end_tsn: u32) ?u32 {
    const MaskInt = BitSet.MaskInt;
    const word_bits = @bitSizeOf(MaskInt);

    var tsn = end_tsn;
    var remaining = end_tsn -% start_tsn +% 1;
    while (remaining > 0) {
        const index = bitIndex(tsn);
        // move tsn's bit to the top of the word, the len bits below it are the ones to examine.
        const shift: BitSet.ShiftInt = @intCast(word_bits - 1 - index % word_bits);
        const len = @min(word_bits - @as(u32, shift), remaining);

        var unacked = ~self.acked_tsn.masks[index / word_bits] << shift;
        if (len < word_bits) unacked &= ~@as(MaskInt, 0) << @intCast(word_bits - len);
        if (unacked != 0) return tsn -% @clz(unacked);

        tsn -%= len;
        remaining -= len;
    }

    return null;
}

fn incrementMissCounts(self: *SackHandler, start_tsn: u32, end_tsn: u32) bool {
    const MaskInt = BitSet.MaskInt;
    const word_bits = @bitSizeOf(MaskInt);

    var tsn = start_tsn;
    var remaining = end_tsn -% start_tsn +% 1;
    var fast_retransmit = false;
    while (remaining > 0) {
        const index = bitIndex(tsn);
        const word_index = index / word_bits;
        const shift: BitSet.ShiftInt = @intCast(index % word_bits);
        const len = @min(word_bits - @as(u32, shift), remaining);

        var range = ~@as(MaskInt, 0) << shift;
        if (len < word_bits) range &= ((@as(MaskInt, 1) << @intCast(len)) - 1) << shift;
        const missing = ~self.acked_tsn.masks[word_index] & range;

        // 2-bit saturating increment of (hi, lo) where missing is set: 00 -> 01 -> 10 -> 11 -> 11.
        const lo = self.miss_lo.masks[word_index];
        const hi = self.miss_hi.masks[word_index];
        const new_hi = hi | (lo & missing);
        const new_lo = (lo ^ missing) | (hi & lo);
        self.miss_hi.masks[word_index] = new_hi;
        self.miss_lo.masks[word_index] = new_lo;
        fast_retransmit = fast_retransmit or (new_hi & new_lo & missing & ~self.fast_retransmitted.masks[word_index]) != 0;

        tsn +%= len;
        remaining -= len;
    }

    return fast_retransmit;
}

fn setRange(bitset: *BitSet, start_tsn: u32, end_tsn: u32, value: bool) void {
    if (Helper.tsnGt(start_tsn, end_tsn)) return;
    const start_index = bitIndex(start_tsn);
    const end_index = bitIndex(end_tsn);

    if (start_index <= end_index) {
        bitset.setRangeValue(.{ .start = start_index, .end = @as(usize, end_index) + 1 }, value);
    } else {
        bitset.setRangeValue(.{ .start = start_index, .end = bitmap_bits }, value);
        bitset.setRangeValue(.{ .start = 0, .end = @as(usize, end_index) + 1 }, value);
    }
}

fn clearRange(self: *SackHandler, start_tsn: u32, end_tsn: u32) void {
    setRange(&self.acked_tsn, start_tsn, end_tsn, false);
    setRange(&self.miss_lo, start_tsn, end_tsn, false);
    setRange(&self.miss_hi, start_tsn, end_tsn, false);
    setRange(&self.fast_retransmitted, start_tsn, end_tsn, false);
}

fn bitIndex(tsn: u32) u16 {
    return @intCast(tsn % bitmap_bits);
}

inline fn isBitSet(self: *const SackHandler, index: u16) bool {
    const Bitmap = @TypeOf(self.acked_tsn);
    const mask = self.acked_tsn.masks[index / @bitSizeOf(Bitmap.MaskInt)];
    return (mask >> @as(Bitmap.ShiftInt, @truncate(index))) & 1 != 0;
}

fn missCount(self: *const SackHandler, tsn: u32) u2 {
    const index = bitIndex(tsn);
    return @as(u2, @intFromBool(self.miss_hi.isSet(index))) << 1 | @intFromBool(self.miss_lo.isSet(index));
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
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3 }), 100);

    try testing.expect(h.isAcked(1));
    try testing.expect(!h.isAcked(2));
    try testing.expect(h.isAcked(3));
    try testing.expect(h.isAcked(4));
    try testing.expect(!h.isAcked(5));
}

test "SackHandler.handleSack: a sack with an unchanged cumulative tsn still updates gap blocks" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 2 }), 100);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 4 }), 100);

    try testing.expect(h.isAcked(3));
    try testing.expect(h.isAcked(5));
}

test "SackHandler.handleSack: a sack with an older cumulative tsn is discarded" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(5, &.{}), 100);
    try testing.expectError(error.Discard, h.handleSack(&testSack(4, &.{}), 100));
    try testing.expectEqual(5, h.cumulative_tsn);
}

test "SackHandler.handleSack: bits acked by old gap blocks are cleared once the cumulative tsn passes them" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3 }), 100);
    _ = try h.handleSack(&testSack(4, &.{}), 100);

    try testing.expect(!h.isBitSet(3));
    try testing.expect(!h.isBitSet(4));
    try testing.expect(!h.isAcked(3 + bitmap_bits));
}

test "SackHandler.handleSack: reneged gap blocks are no longer acked" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3 }), 100);
    _ = try h.handleSack(&testSack(1, &.{}), 100);

    try testing.expect(!h.isAcked(3));
    try testing.expect(!h.isAcked(4));
}

test "SackHandler.handleSack: an invalid gap block discards the sack without changing state" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 2 }), 100);
    try testing.expectError(error.Discard, h.handleSack(&testSack(3, &.{ 0, 4, 0, 1 }), 100));

    try testing.expectEqual(1, h.cumulative_tsn);
    try testing.expect(h.isAcked(3));
}

test "SackHandler.handleSack: unordered or overlapping gap blocks discard the sack without changing state" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 2 }), 100);
    try testing.expectError(error.Discard, h.handleSack(&testSack(3, &.{ 0, 5, 0, 6, 0, 2, 0, 3 }), 100));
    try testing.expectError(error.Discard, h.handleSack(&testSack(3, &.{ 0, 2, 0, 4, 0, 4, 0, 6 }), 100));
    try testing.expectError(error.Discard, h.handleSack(&testSack(3, &.{ 0, 2, 0, 4, 0, 2, 0, 4 }), 100));

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

test "SackHandler.handleCumulativeTsn: cumulative tsn clears the gap bits it passes" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3, 0, 5, 0, 5 }), 100);
    h.handleCumulativeTsn(4);

    try testing.expectEqual(4, h.cumulative_tsn);
    try testing.expect(!h.isBitSet(3));
    try testing.expect(!h.isBitSet(4));
    try testing.expect(h.isAcked(6));
    try testing.expect(!h.isAcked(3 + bitmap_bits));
}

test "SackHandler.handleCumulativeTsn: cumulative tsn past the highest tsn updates it" {
    var h = SackHandler.init(1);
    h.handleCumulativeTsn(10);

    try testing.expectEqual(10, h.highest_tsn_received);
    _ = try h.handleSack(&testSack(10, &.{ 0, 2, 0, 2 }), 100);
    try testing.expect(h.isAcked(12));
    try testing.expect(!h.isAcked(11));
}

test "SackHandler.handleCumulativeTsn: older cumulative tsn is ignored" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(5, &.{}), 100);
    h.handleCumulativeTsn(3);

    try testing.expectEqual(5, h.cumulative_tsn);
}

test "SackHandler.allAcked" {
    var h = SackHandler.init(1);
    try testing.expect(h.allAcked(0));

    // a hole
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 5 }), 6);
    try testing.expect(!h.allAcked(6));

    // a trailing unacked tsn
    _ = try h.handleSack(&testSack(1, &.{ 0, 1, 0, 4 }), 6);
    try testing.expect(!h.allAcked(6));

    // contiguous gap blocks reach the highest tsn sent
    _ = try h.handleSack(&testSack(1, &.{ 0, 1, 0, 3, 0, 4, 0, 5 }), 6);
    try testing.expect(h.allAcked(6));

    // reneged gap blocks
    _ = try h.handleSack(&testSack(1, &.{}), 6);
    try testing.expect(!h.allAcked(6));

    h.handleCumulativeTsn(5);
    try testing.expect(!h.allAcked(6));
    h.handleCumulativeTsn(6);
    try testing.expect(h.allAcked(6));

    // the cumulative tsn reaches the highest tsn sent
    _ = try h.handleSack(&testSack(10, &.{}), 10);
    try testing.expect(h.allAcked(10));
    try testing.expect(!h.allAcked(12));
}

test "GapAckedIterator: yields the gap acked tsns in order" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3, 0, 5, 0, 5 }), 100);

    var it = SackHandler.GapAckedIterator.init(&h);
    try testing.expectEqual(3, it.next().?);
    try testing.expectEqual(4, it.next().?);
    try testing.expectEqual(6, it.next().?);
    try testing.expectEqual(null, it.next());
}

test "GapAckedIterator: tsns wrapping around u32" {
    const init_tsn: u32 = 0xFFFFFFFA;
    var h = SackHandler.init(init_tsn);
    _ = try h.handleSack(&testSack(init_tsn, &.{ 0, 2, 0, 3, 0, 8, 0, 10 }), init_tsn +% 100);

    var it = SackHandler.GapAckedIterator.init(&h);
    try testing.expectEqual(0xFFFFFFFC, it.next().?);
    try testing.expectEqual(0xFFFFFFFD, it.next().?);
    try testing.expectEqual(2, it.next().?);
    try testing.expectEqual(3, it.next().?);
    try testing.expectEqual(4, it.next().?);
    try testing.expectEqual(null, it.next());
}

test "GapAckedIterator: tsns wrapping around the bitmap and spanning words" {
    const init_tsn: u32 = bitmap_bits * 5 - 3;
    var h = SackHandler.init(init_tsn);
    _ = try h.handleSack(&testSack(init_tsn, &.{ 0, 1, 0, 1, 0, 3, 0, 4, 0, 70, 0, 70, 0, 200, 0, 202 }), init_tsn +% 500);

    var it = SackHandler.GapAckedIterator.init(&h);
    for ([_]u32{ 1, 3, 4, 70, 200, 201, 202 }) |offset| {
        try testing.expectEqual(init_tsn +% offset, it.next().?);
    }
    try testing.expectEqual(null, it.next());
}

test "GapAckedIterator: nothing to iterate without gap blocks" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(5, &.{}), 100);

    var it = SackHandler.GapAckedIterator.init(&h);
    try testing.expectEqual(null, it.next());
}

test "SackHandler.isRangeAcked: ranges spanning multiple words" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3, 0, 5, 0, 5, 0, 14, 0, 200 }), 1000);

    try testing.expect(h.isRangeAcked(3, 4));
    try testing.expect(h.isRangeAcked(70, 189));
    try testing.expect(!h.isRangeAcked(3, 5));
}

test "SackHandler.isRangeAcked: an unacked start tsn" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 3, 0, 4 }), 1000);

    try testing.expect(!h.isRangeAcked(3, 5));
    try testing.expect(h.isRangeAcked(4, 5));
}

test "SackHandler.isRangeAcked: range ending on the last bit of a word" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 60, 0, 70 }), 1000);

    try testing.expect(h.isRangeAcked(61, 63));
    try testing.expect(h.isRangeAcked(61, 71));
}

test "SackHandler.isRangeAcked: tsns wrapping around u32" {
    const init_tsn: u32 = 0xFFFFFFF0;
    var h = SackHandler.init(init_tsn);
    _ = try h.handleSack(&testSack(init_tsn, &.{ 0, 30, 0, 30 }), init_tsn +% 100);

    try testing.expect(!h.isRangeAcked(0xFFFFFFF5, 5));
    try testing.expect(h.isRangeAcked(init_tsn +% 30, init_tsn +% 30));
}

test "SackHandler.isRangeAcked: ranges starting at or below the cumulative tsn" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(10, &.{ 0, 1, 0, 2 }), 1000);

    try testing.expect(h.isRangeAcked(5, 10));
    try testing.expect(h.isRangeAcked(5, 12));
    try testing.expect(!h.isRangeAcked(5, 13));
}

test "SackHandler.highestUnacked" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3, 0, 5, 0, 5, 0, 60, 0, 200 }), 1000);

    // bits set: 3, 4, 6, 61..201
    try testing.expectEqual(null, h.highestUnacked(3, 4));
    try testing.expectEqual(5, h.highestUnacked(3, 6));
    try testing.expectEqual(2, h.highestUnacked(2, 4));
    try testing.expectEqual(60, h.highestUnacked(10, 201));
    try testing.expectEqual(null, h.highestUnacked(61, 201));
    try testing.expectEqual(202, h.highestUnacked(61, 202));
}

test "SackHandler.highestUnacked: tsns wrapping around the bitmap and u32" {
    const init_tsn: u32 = 0xFFFFFFF0;
    var h = SackHandler.init(init_tsn);
    _ = try h.handleSack(&testSack(init_tsn, &.{ 0, 5, 0, 30 }), init_tsn +% 100);

    try testing.expectEqual(null, h.highestUnacked(init_tsn +% 5, init_tsn +% 30));
    try testing.expectEqual(init_tsn +% 4, h.highestUnacked(init_tsn +% 1, init_tsn +% 30));
}

test "SackHandler.handleSack: missing tsns below the highest newly acked tsn get a miss indication" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3, 0, 5, 0, 5 }), 100);

    try testing.expectEqual(1, h.missCount(2));
    try testing.expectEqual(1, h.missCount(5));
    for ([_]u32{ 3, 4, 6, 7 }) |tsn| try testing.expectEqual(0, h.missCount(tsn));
}

test "SackHandler.handleSack: miss indications follow htna and saturate at 3" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3 }), 100);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3, 0, 7, 0, 7 }), 100);
    try testing.expectEqual(2, h.missCount(2));
    for ([_]u32{ 5, 6, 7 }) |tsn| try testing.expectEqual(1, h.missCount(tsn));

    // nothing newly acked
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3, 0, 7, 0, 7 }), 100);
    try testing.expectEqual(2, h.missCount(2));

    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3, 0, 7, 0, 8 }), 100);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 3, 0, 7, 0, 9 }), 100);
    try testing.expectEqual(3, h.missCount(2));
    for ([_]u32{ 5, 6, 7 }) |tsn| try testing.expectEqual(3, h.missCount(tsn));
}

test "SackHandler.handleSack: missing tsns above htna get no miss indication" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 4, 0, 4 }), 100);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 2, 0, 4, 0, 4 }), 100);

    try testing.expectEqual(2, h.missCount(2));
    try testing.expectEqual(1, h.missCount(4));
}

test "SackHandler.handleSack: miss indications wrapping around the bitmap and spanning words" {
    const init_tsn: u32 = bitmap_bits * 5 - 3;
    var h = SackHandler.init(init_tsn);
    _ = try h.handleSack(&testSack(init_tsn, &.{ 0, 70, 0, 70 }), init_tsn +% 100);

    for (1..70) |offset| try testing.expectEqual(1, h.missCount(init_tsn +% @as(u32, @intCast(offset))));
    try testing.expectEqual(0, h.missCount(init_tsn +% 70));
    try testing.expectEqual(0, h.missCount(init_tsn +% 71));
}

test "SackHandler: miss counts are cleared once the cumulative tsn passes them" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 2, 0, 2, 0, 5, 0, 5 }), 100);
    try testing.expectEqual(1, h.missCount(2));
    try testing.expectEqual(1, h.missCount(5));

    _ = try h.handleSack(&testSack(3, &.{ 0, 3, 0, 3 }), 100);
    try testing.expectEqual(0, h.missCount(2));
    try testing.expectEqual(0, h.missCount(2 + bitmap_bits));
    try testing.expectEqual(1, h.missCount(5));

    h.handleCumulativeTsn(6);
    try testing.expectEqual(0, h.missCount(5));
    try testing.expectEqual(0, h.missCount(5 + bitmap_bits));
}

test "SackHandler.firstTsnWithThreeMisses" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 3, 0, 3 }), 100);
    _ = try h.handleSack(&testSack(1, &.{ 0, 3, 0, 4 }), 100);
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(2));

    _ = try h.handleSack(&testSack(1, &.{ 0, 3, 0, 5 }), 100);
    try testing.expectEqual(2, h.firstTsnWithThreeMisses(2).?);
    try testing.expectEqual(3, h.firstTsnWithThreeMisses(3).?);
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(4));

    // a gap acked tsn is skipped even with 3 misses
    _ = try h.handleSack(&testSack(1, &.{ 0, 1, 0, 1, 0, 3, 0, 6 }), 100);
    try testing.expectEqual(3, h.firstTsnWithThreeMisses(2).?);
}

test "SackHandler.firstTsnWithThreeMisses: tsns wrapping around the bitmap and spanning words" {
    const init_tsn: u32 = bitmap_bits * 5 - 3;
    var h = SackHandler.init(init_tsn);
    _ = try h.handleSack(&testSack(init_tsn, &.{ 0, 1, 0, 69, 0, 71, 0, 71 }), init_tsn +% 100);
    _ = try h.handleSack(&testSack(init_tsn, &.{ 0, 1, 0, 69, 0, 71, 0, 72 }), init_tsn +% 100);
    _ = try h.handleSack(&testSack(init_tsn, &.{ 0, 1, 0, 69, 0, 71, 0, 73 }), init_tsn +% 100);

    try testing.expectEqual(init_tsn +% 70, h.firstTsnWithThreeMisses(init_tsn +% 1).?);
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(init_tsn +% 71));
}

test "SackHandler.markFastRetransmitted: a fast retransmitted tsn is skipped until the cumulative tsn passes it" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 3, 0, 3 }), 100);
    _ = try h.handleSack(&testSack(1, &.{ 0, 3, 0, 4 }), 100);
    _ = try h.handleSack(&testSack(1, &.{ 0, 3, 0, 5 }), 100);

    h.markFastRetransmitted(2);
    try testing.expectEqual(3, h.firstTsnWithThreeMisses(2).?);
    h.markFastRetransmitted(3);
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(2));

    // still missing, a new sack does not make them eligible again
    _ = try h.handleSack(&testSack(1, &.{ 0, 3, 0, 6 }), 100);
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(2));

    h.handleCumulativeTsn(3);
    try testing.expect(!h.fast_retransmitted.isSet(2));
    try testing.expect(!h.fast_retransmitted.isSet(3));
}

test "SackHandler.firstTsnWithThreeMisses: nothing to search without gap blocks" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(5, &.{}), 100);
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(h.cumulative_tsn +% 1));
}

test "SackHandler.handleSack: returns true when a missing tsn has 3 miss indications" {
    var h = SackHandler.init(1);
    try testing.expect(!try h.handleSack(&testSack(1, &.{ 0, 3, 0, 3 }), 100));
    try testing.expect(!try h.handleSack(&testSack(1, &.{ 0, 3, 0, 4 }), 100));
    try testing.expect(try h.handleSack(&testSack(1, &.{ 0, 3, 0, 5 }), 100));

    h.markFastRetransmitted(2);
    h.markFastRetransmitted(3);
    try testing.expect(!try h.handleSack(&testSack(1, &.{ 0, 3, 0, 6 }), 100));
    try testing.expect(!try h.handleSack(&testSack(1, &.{ 0, 3, 0, 6 }), 100));
    try testing.expect(!try h.handleSack(&testSack(3, &.{ 0, 4, 0, 4 }), 100));
}

test "SackHandler.clearFastRetransmit" {
    var h = SackHandler.init(1);
    _ = try h.handleSack(&testSack(1, &.{ 0, 3, 0, 3 }), 100);
    _ = try h.handleSack(&testSack(1, &.{ 0, 3, 0, 4 }), 100);
    _ = try h.handleSack(&testSack(1, &.{ 0, 3, 0, 5 }), 100);
    h.markFastRetransmitted(2);

    h.clearFastRetransmit();
    try testing.expectEqual(0, h.missCount(2));
    try testing.expectEqual(0, h.missCount(3));
    try testing.expect(!h.fast_retransmitted.isSet(2));
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(2));
    try testing.expect(h.isAcked(4));
}
