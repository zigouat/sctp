const std = @import("std");
const testing = std.testing;
const Helper = @import("helper.zig");

const TsnBitSet = @This();
const BitSet = std.bit_set.ArrayBitSet(usize, bitmap_bits);
const MaskInt = BitSet.MaskInt;
const word_bits = @bitSizeOf(MaskInt);

pub const bitmap_bits = 1024;

comptime {
    std.debug.assert(bitmap_bits % word_bits == 0);
}

cumulative_tsn: u32,
/// The highest tsn received in any sack, including the cumulative tsn.
highest_tsn_received: u32,
/// BitSet of tsns acked by gap blocks.
acked_tsn: BitSet,

/// The part of a word of the bitset that falls inside a tsn range.
pub const Word = struct {
    /// index of the word in the bitset masks
    index: u16,
    /// bits of the word inside the range
    mask: MaskInt,
    /// tsn of the word's bit 0
    base_tsn: u32,

    /// The tsn of the lowest set bit in `bits`, which must not be 0.
    pub fn firstTsn(self: Word, bits: MaskInt) u32 {
        return self.base_tsn +% @ctz(bits);
    }

    /// The tsn of the highest set bit in `bits`, which must not be 0.
    pub fn lastTsn(self: Word, bits: MaskInt) u32 {
        return self.base_tsn +% (word_bits - 1 - @as(u32, @clz(bits)));
    }
};

/// Walks the words covering an inclusive tsn range, in tsn order or in reverse.
/// Lets bitsets indexed the same way be combined word by word over the range.
pub const WordIterator = struct {
    /// the next tsn to examine
    next_tsn: u32,
    /// the number of tsns left to examine
    remaining: u32,
    reverse: bool,

    pub fn init(start_tsn: u32, end_tsn: u32, reverse: bool) WordIterator {
        const len = if (Helper.tsnGt(start_tsn, end_tsn)) 0 else end_tsn -% start_tsn +% 1;
        std.debug.assert(len <= bitmap_bits);
        return .{ .next_tsn = if (reverse) end_tsn else start_tsn, .remaining = len, .reverse = reverse };
    }

    pub fn next(self: *WordIterator) ?Word {
        if (self.remaining == 0) return null;

        const index = bitIndex(self.next_tsn);
        const bit: u32 = index % word_bits;
        const len, const low = if (self.reverse) blk: {
            const len = @min(bit + 1, self.remaining);
            break :blk .{ len, bit + 1 - len };
        } else .{ @min(word_bits - bit, self.remaining), bit };

        const ones = if (len == word_bits) ~@as(MaskInt, 0) else (@as(MaskInt, 1) << @intCast(len)) - 1;
        const word: Word = .{
            .index = @intCast(index / word_bits),
            .mask = ones << @intCast(low),
            .base_tsn = self.next_tsn -% bit,
        };
        self.next_tsn = if (self.reverse) self.next_tsn -% len else self.next_tsn +% len;
        self.remaining -= len;
        return word;
    }
};

/// Iterates the tsns acked by gap blocks, i.e. the set bits in (cumulative_tsn, highest_tsn_received], in tsn order.
pub const GapAckedIterator = struct {
    bit_set: *const TsnBitSet,
    words: WordIterator,
    /// set bits of the current word not yet returned
    bits: MaskInt,
    /// tsn of the current word's bit 0
    base_tsn: u32,

    pub fn next(self: *GapAckedIterator) ?u32 {
        while (self.bits == 0) {
            const word = self.words.next() orelse return null;
            self.bits = self.bit_set.acked_tsn.masks[word.index] & word.mask;
            self.base_tsn = word.base_tsn;
        }
        const tsn = self.base_tsn +% @ctz(self.bits);
        self.bits &= self.bits - 1;
        return tsn;
    }
};

pub const GapBlock = struct { start: u32, end: u32 };

/// Iterates the gap blocks, i.e. the runs of set bits in (cumulative_tsn, highest_tsn_received], in tsn order.
pub const GapBlockIterator = struct {
    bit_set: *const TsnBitSet,
    next_tsn: u32,

    pub fn next(self: *GapBlockIterator) ?GapBlock {
        const end_tsn = self.bit_set.highest_tsn_received;
        const start = self.bit_set.firstAcked(self.next_tsn, end_tsn) orelse {
            self.next_tsn = end_tsn +% 1;
            return null;
        };
        const end = if (self.bit_set.firstUnacked(start, end_tsn)) |tsn| tsn -% 1 else end_tsn;
        self.next_tsn = end +% 1;
        return .{ .start = start, .end = end };
    }
};

pub fn init(initial_tsn: u32) TsnBitSet {
    return .{
        .cumulative_tsn = initial_tsn -% 1,
        .highest_tsn_received = initial_tsn -% 1,
        .acked_tsn = .empty,
    };
}

/// Returns true if the tsn is acked, i.e. if it is less than or equal to the cumulative tsn or if its bit is set.
pub fn isAcked(self: *const TsnBitSet, tsn: u32) bool {
    if (Helper.tsnLte(tsn, self.cumulative_tsn)) return true;
    if (Helper.tsnGt(tsn, self.highest_tsn_received)) return false;
    return self.isBitSet(bitIndex(tsn));
}

/// Returns true if all tsns in the inclusive range are acked.
pub fn isRangeAcked(self: *const TsnBitSet, start_tsn: u32, end_tsn: u32) bool {
    if (Helper.tsnLte(end_tsn, self.cumulative_tsn)) return true;
    if (Helper.tsnGt(end_tsn, self.highest_tsn_received)) return false;
    const start = if (Helper.tsnLte(start_tsn, self.cumulative_tsn)) self.cumulative_tsn +% 1 else start_tsn;
    return self.firstUnacked(start, end_tsn) == null;
}

/// Returns true if all tsns up to the highest tsn sent are acked.
pub fn allAcked(self: *const TsnBitSet, max_tsn: u32) bool {
    return self.highest_tsn_received == max_tsn and self.isRangeAcked(self.cumulative_tsn, max_tsn);
}

/// Acks a tsn above the cumulative tsn, without advancing the cumulative tsn.
pub fn ack(self: *TsnBitSet, tsn: u32) void {
    if (Helper.tsnLte(tsn, self.cumulative_tsn)) return;
    self.acked_tsn.set(bitIndex(tsn));
    if (Helper.tsnGt(tsn, self.highest_tsn_received)) self.highest_tsn_received = tsn;
}

/// Reneges a tsn above the cumulative tsn, the highest tsn received falls back to the highest one still acked.
pub fn unack(self: *TsnBitSet, tsn: u32) void {
    if (Helper.tsnLte(tsn, self.cumulative_tsn)) return;
    self.acked_tsn.unset(bitIndex(tsn));
    if (tsn != self.highest_tsn_received) return;
    self.highest_tsn_received = self.lastAcked(self.cumulative_tsn +% 1, tsn) orelse self.cumulative_tsn;
}

/// Moves the cumulative tsn forward, clearing the bits it passes. An older cumulative tsn is ignored.
pub fn setCumulativeTsn(self: *TsnBitSet, cumulative_tsn: u32) void {
    if (!Helper.tsnGt(cumulative_tsn, self.cumulative_tsn)) return;
    self.setRange(self.cumulative_tsn +% 1, cumulative_tsn, false);
    self.cumulative_tsn = cumulative_tsn;
    if (Helper.tsnGt(cumulative_tsn, self.highest_tsn_received)) self.highest_tsn_received = cumulative_tsn;
}

/// Moves the cumulative tsn over the acked tsns directly following it.
pub fn advanceCumulativeTsn(self: *TsnBitSet) void {
    const next_tsn = self.cumulative_tsn +% 1;
    if (self.highest_tsn_received == next_tsn) {
        self.acked_tsn.unset(bitIndex(next_tsn));
        self.cumulative_tsn = next_tsn;
        return;
    }

    const end_tsn = self.highest_tsn_received;
    const first_unacked = self.firstUnacked(self.cumulative_tsn +% 1, end_tsn) orelse end_tsn +% 1;
    self.setCumulativeTsn(first_unacked -% 1);
}

/// Sets the bits of an inclusive tsn range, a range wider than the bitmap sets all of them.
pub fn setRange(self: *TsnBitSet, start_tsn: u32, end_tsn: u32, value: bool) void {
    if (Helper.tsnGt(start_tsn, end_tsn)) return;
    if (end_tsn -% start_tsn >= bitmap_bits) {
        self.acked_tsn = if (value) .full else .empty;
        return;
    }

    var it = WordIterator.init(start_tsn, end_tsn, false);
    while (it.next()) |word| {
        if (value) self.acked_tsn.masks[word.index] |= word.mask else self.acked_tsn.masks[word.index] &= ~word.mask;
    }
}

/// The number of set bits in an inclusive tsn range.
pub fn countAcked(self: *const TsnBitSet, start_tsn: u32, end_tsn: u32) u32 {
    var total: u32 = 0;
    var it = WordIterator.init(start_tsn, end_tsn, false);
    while (it.next()) |word| total += @popCount(self.acked_tsn.masks[word.index] & word.mask);
    return total;
}

/// The lowest set bit in an inclusive tsn range.
pub fn firstAcked(self: *const TsnBitSet, start_tsn: u32, end_tsn: u32) ?u32 {
    var it = WordIterator.init(start_tsn, end_tsn, false);
    while (it.next()) |word| {
        const bits = self.acked_tsn.masks[word.index] & word.mask;
        if (bits != 0) return word.firstTsn(bits);
    }
    return null;
}

/// The lowest unset bit in an inclusive tsn range.
pub fn firstUnacked(self: *const TsnBitSet, start_tsn: u32, end_tsn: u32) ?u32 {
    var it = WordIterator.init(start_tsn, end_tsn, false);
    while (it.next()) |word| {
        const bits = ~self.acked_tsn.masks[word.index] & word.mask;
        if (bits != 0) return word.firstTsn(bits);
    }
    return null;
}

/// The highest set bit in an inclusive tsn range.
pub fn lastAcked(self: *const TsnBitSet, start_tsn: u32, end_tsn: u32) ?u32 {
    var it = WordIterator.init(start_tsn, end_tsn, true);
    while (it.next()) |word| {
        const bits = self.acked_tsn.masks[word.index] & word.mask;
        if (bits != 0) return word.lastTsn(bits);
    }
    return null;
}

/// The highest unset bit in an inclusive tsn range.
pub fn lastUnacked(self: *const TsnBitSet, start_tsn: u32, end_tsn: u32) ?u32 {
    var it = WordIterator.init(start_tsn, end_tsn, true);
    while (it.next()) |word| {
        const bits = ~self.acked_tsn.masks[word.index] & word.mask;
        if (bits != 0) return word.lastTsn(bits);
    }
    return null;
}

pub fn iterateGapAcked(self: *const TsnBitSet) GapAckedIterator {
    return .{
        .bit_set = self,
        .words = .init(self.cumulative_tsn +% 1, self.highest_tsn_received, false),
        .bits = 0,
        .base_tsn = 0,
    };
}

pub fn iterateGapBlocks(self: *const TsnBitSet) GapBlockIterator {
    return .{ .bit_set = self, .next_tsn = self.cumulative_tsn +% 1 };
}

pub inline fn bitIndex(tsn: u32) u16 {
    return @intCast(tsn % bitmap_bits);
}

inline fn isBitSet(self: *const TsnBitSet, index: u16) bool {
    const mask = self.acked_tsn.masks[index / word_bits];
    return (mask >> @as(BitSet.ShiftInt, @truncate(index))) & 1 != 0;
}

const wrap_tsn: u32 = bitmap_bits * 5 - 3;
const u32_wrap_tsn: u32 = 0xFFFFFFF0;

test "WordIterator: splits a range at word boundaries" {
    {
        var it = WordIterator.init(3, 5, false);
        const word = it.next().?;
        try testing.expectEqual(0, word.index);
        try testing.expectEqual(0b111000, word.mask);
        try testing.expectEqual(0, word.base_tsn);
        try testing.expectEqual(null, it.next());
    }

    // wrapping around the bitmap
    {
        var it = WordIterator.init(bitmap_bits - 2, bitmap_bits + word_bits + 1, false);
        const first = it.next().?;
        try testing.expectEqual(bitmap_bits / word_bits - 1, first.index);
        try testing.expectEqual(@as(MaskInt, 0b11) << (word_bits - 2), first.mask);
        try testing.expectEqual(bitmap_bits - word_bits, first.base_tsn);

        const second = it.next().?;
        try testing.expectEqual(0, second.index);
        try testing.expectEqual(~@as(MaskInt, 0), second.mask);
        try testing.expectEqual(bitmap_bits, second.base_tsn);

        const third = it.next().?;
        try testing.expectEqual(1, third.index);
        try testing.expectEqual(0b11, third.mask);
        try testing.expectEqual(null, it.next());
    }

    // in reverse
    {
        var it = WordIterator.init(word_bits - 2, word_bits + 1, true);
        const first = it.next().?;
        try testing.expectEqual(1, first.index);
        try testing.expectEqual(0b11, first.mask);
        const second = it.next().?;
        try testing.expectEqual(0, second.index);
        try testing.expectEqual(@as(MaskInt, 0b11) << (word_bits - 2), second.mask);
        try testing.expectEqual(null, it.next());
    }

    // empty range
    {
        var it = WordIterator.init(5, 4, false);
        try testing.expectEqual(null, it.next());
    }
}

test "TsnBitSet.setRange" {
    {
        var t = TsnBitSet.init(1);
        t.setRange(wrap_tsn, wrap_tsn +% 200, true);
        try testing.expectEqual(201, t.acked_tsn.count());
        try testing.expectEqual(201, t.countAcked(wrap_tsn, wrap_tsn +% 200));

        t.setRange(wrap_tsn +% 10, wrap_tsn +% 100, false);
        try testing.expectEqual(10, t.countAcked(wrap_tsn, wrap_tsn +% 9));
        try testing.expectEqual(0, t.countAcked(wrap_tsn +% 10, wrap_tsn +% 100));
        try testing.expectEqual(100, t.countAcked(wrap_tsn +% 101, wrap_tsn +% 200));
    }

    // tsns wrapping around u32
    {
        var t = TsnBitSet.init(1);
        t.setRange(u32_wrap_tsn, 5, true);
        try testing.expectEqual(22, t.acked_tsn.count());
        try testing.expectEqual(22, t.countAcked(u32_wrap_tsn, 5));
    }

    // a range wider than the bitmap
    {
        var t = TsnBitSet.init(1);
        t.setRange(10, 10 + bitmap_bits, true);
        try testing.expectEqual(bitmap_bits, t.acked_tsn.count());
        t.setRange(10, 10 + 2 * bitmap_bits, false);
        try testing.expectEqual(0, t.acked_tsn.count());
    }

    // empty range
    {
        var t = TsnBitSet.init(1);
        t.setRange(10, 9, true);
        try testing.expectEqual(0, t.acked_tsn.count());
    }
}

test "TsnBitSet.firstAcked, firstUnacked, lastAcked and lastUnacked" {
    var t = TsnBitSet.init(1);
    t.setRange(wrap_tsn +% 2, wrap_tsn +% 100, true);
    const start = wrap_tsn;
    const end = wrap_tsn +% 200;

    try testing.expectEqual(wrap_tsn +% 2, t.firstAcked(start, end).?);
    try testing.expectEqual(wrap_tsn +% 100, t.lastAcked(start, end).?);
    try testing.expectEqual(wrap_tsn, t.firstUnacked(start, end).?);
    try testing.expectEqual(wrap_tsn +% 200, t.lastUnacked(start, end).?);
    try testing.expectEqual(wrap_tsn +% 101, t.firstUnacked(wrap_tsn +% 2, end).?);
    try testing.expectEqual(wrap_tsn +% 1, t.lastUnacked(start, wrap_tsn +% 100).?);

    try testing.expectEqual(null, t.firstAcked(wrap_tsn +% 101, end));
    try testing.expectEqual(null, t.lastAcked(start, wrap_tsn +% 1));
    try testing.expectEqual(null, t.firstUnacked(wrap_tsn +% 2, wrap_tsn +% 100));
    try testing.expectEqual(null, t.lastUnacked(wrap_tsn +% 2, wrap_tsn +% 100));
    try testing.expectEqual(null, t.firstAcked(end, start));
}

test "TsnBitSet.lastUnacked: ranges spanning words" {
    var t = TsnBitSet.init(1);
    t.setRange(3, 4, true);
    t.setRange(6, 6, true);
    t.setRange(61, 201, true);

    try testing.expectEqual(null, t.lastUnacked(3, 4));
    try testing.expectEqual(5, t.lastUnacked(3, 6));
    try testing.expectEqual(2, t.lastUnacked(2, 4));
    try testing.expectEqual(60, t.lastUnacked(10, 201));
    try testing.expectEqual(null, t.lastUnacked(61, 201));
    try testing.expectEqual(202, t.lastUnacked(61, 202));
}

test "TsnBitSet.isAcked and TsnBitSet.isRangeAcked" {
    var t = TsnBitSet.init(1);
    t.setCumulativeTsn(10);
    t.ack(12);
    t.ack(13);

    try testing.expect(t.isAcked(5));
    try testing.expect(!t.isAcked(11));
    try testing.expect(t.isAcked(12));
    try testing.expect(t.isRangeAcked(5, 10));
    try testing.expect(t.isRangeAcked(12, 13));
    try testing.expect(!t.isRangeAcked(5, 13));

    // bits above the highest tsn received are not acked
    t.highest_tsn_received = 12;
    try testing.expect(!t.isAcked(13));
    try testing.expect(!t.isRangeAcked(12, 13));
}

test "TsnBitSet.allAcked" {
    var t = TsnBitSet.init(1);
    try testing.expect(t.allAcked(0));

    t.ack(2);
    t.ack(3);
    try testing.expect(!t.allAcked(3));

    t.ack(1);
    try testing.expect(t.allAcked(3));
    try testing.expect(!t.allAcked(4));
}

test "TsnBitSet.setCumulativeTsn: clears the passed bits" {
    var t = TsnBitSet.init(1);
    t.ack(3);
    t.ack(6);
    t.setCumulativeTsn(4);

    try testing.expectEqual(4, t.cumulative_tsn);
    try testing.expectEqual(6, t.highest_tsn_received);
    try testing.expect(!t.isBitSet(3));
    try testing.expect(!t.isAcked(3 + bitmap_bits));

    // older cumulative tsn is ignored
    t.setCumulativeTsn(2);
    try testing.expectEqual(4, t.cumulative_tsn);

    t.setCumulativeTsn(10);
    try testing.expectEqual(10, t.highest_tsn_received);
}

test "TsnBitSet.advanceCumulativeTsn" {
    var t = TsnBitSet.init(u32_wrap_tsn);
    t.ack(u32_wrap_tsn +% 1);
    t.advanceCumulativeTsn();
    try testing.expectEqual(u32_wrap_tsn -% 1, t.cumulative_tsn);

    for (0..100) |offset| t.ack(u32_wrap_tsn +% @as(u32, @intCast(offset)));
    t.ack(u32_wrap_tsn +% 101);
    t.advanceCumulativeTsn();
    try testing.expectEqual(u32_wrap_tsn +% 99, t.cumulative_tsn);
    try testing.expectEqual(1, t.acked_tsn.count());

    t.ack(u32_wrap_tsn +% 100);
    t.advanceCumulativeTsn();
    try testing.expectEqual(u32_wrap_tsn +% 101, t.cumulative_tsn);
    try testing.expectEqual(0, t.acked_tsn.count());
}

test "TsnBitSet.unack" {
    var t = TsnBitSet.init(1);
    for ([_]u32{ 1, 2, 3, 5, 7 }) |tsn| t.ack(tsn);
    t.advanceCumulativeTsn();

    t.unack(7);
    try testing.expect(!t.isAcked(7));
    try testing.expectEqual(5, t.highest_tsn_received);

    t.unack(5);
    try testing.expectEqual(3, t.highest_tsn_received);

    // not the highest tsn received
    t.ack(6);
    t.ack(8);
    t.unack(6);
    try testing.expect(!t.isAcked(6));
    try testing.expectEqual(8, t.highest_tsn_received);
}

test "TsnBitSet.iterateGapAcked" {
    {
        var t = TsnBitSet.init(1);
        for ([_]u32{ 1, 3, 4, 6 }) |tsn| t.ack(tsn);
        t.advanceCumulativeTsn();

        var it = t.iterateGapAcked();
        try testing.expectEqual(3, it.next().?);
        try testing.expectEqual(4, it.next().?);
        try testing.expectEqual(6, it.next().?);
        try testing.expectEqual(null, it.next());
        try testing.expectEqual(null, it.next());
    }

    // tsns wrapping around u32 and the bitmap
    {
        var t = TsnBitSet.init(u32_wrap_tsn);
        for ([_]u32{ 1, 3, 4, 70, 200, 201, 202 }) |offset| t.ack(u32_wrap_tsn +% offset);

        var it = t.iterateGapAcked();
        for ([_]u32{ 1, 3, 4, 70, 200, 201, 202 }) |offset| try testing.expectEqual(u32_wrap_tsn +% offset, it.next().?);
        try testing.expectEqual(null, it.next());
    }
}

test "TsnBitSet.iterateGapBlocks" {
    {
        var t = TsnBitSet.init(wrap_tsn);
        for ([_]u32{ 1, 3, 4, 70, 200, 201, 202 }) |offset| t.ack(wrap_tsn +% offset);

        var it = t.iterateGapBlocks();
        for ([_][2]u32{ .{ 1, 1 }, .{ 3, 4 }, .{ 70, 70 }, .{ 200, 202 } }) |block| {
            try testing.expectEqual(GapBlock{ .start = wrap_tsn +% block[0], .end = wrap_tsn +% block[1] }, it.next().?);
        }
        try testing.expectEqual(null, it.next());
    }

    // nothing above the cumulative tsn
    {
        var t = TsnBitSet.init(1);
        t.setCumulativeTsn(6);
        var it = t.iterateGapBlocks();
        try testing.expectEqual(null, it.next());
    }
}
