const std = @import("std");
const testing = std.testing;
const message = @import("message.zig");
const Helper = @import("helper.zig");
const TsnBitSet = @import("tsn_bit_set.zig");

const SackHandler = @This();
const BitSet = std.bit_set.ArrayBitSet(usize, bitmap_bits);
const word_bits = @bitSizeOf(BitSet.MaskInt);
const bitIndex = TsnBitSet.bitIndex;

pub const bitmap_bits = TsnBitSet.bitmap_bits;

/// tsns acked by the cumulative tsn and the gap blocks.
acked: TsnBitSet,
/// 2-bit saturating miss indication counter per tsn, split into a low and a high bit plane.
miss_lo: BitSet,
miss_hi: BitSet,
/// tsns already fast retransmitted, they are not eligible for another fast retransmit.
fast_retransmitted: BitSet,
/// tsns abandoned by PR-SCTP (RFC 3758), skipped by the advanced peer ack point.
abandoned: BitSet,
/// RFC 3758 - the tsn up to which the peer is told to move its cumulative tsn with a FORWARD TSN chunk.
advanced_peer_ack_point: u32,

pub fn init(initial_tsn: u32) SackHandler {
    return .{
        .acked = .init(initial_tsn),
        .miss_lo = .empty,
        .miss_hi = .empty,
        .fast_retransmitted = .empty,
        .abandoned = .empty,
        .advanced_peer_ack_point = initial_tsn -% 1,
    };
}

pub fn validateSack(self: *const SackHandler, sack: *const message.Sack, highest_tsn_sent: u32) error{Discard}!void {
    if (Helper.tsnGt(self.acked.cumulative_tsn, sack.cumulative_tsn)) return error.Discard;
    if (Helper.tsnGt(sack.cumulative_tsn, highest_tsn_sent)) return error.Discard;

    var prev_end = sack.cumulative_tsn;
    var validate_it = sack.iterateGapAckBlocks();
    while (validate_it.nextBlock() catch return error.Discard) |block| {
        if (Helper.tsnLte(block.start, prev_end)) return error.Discard;
        if (Helper.tsnGt(block.end, highest_tsn_sent)) return error.Discard;
        prev_end = block.end;
    }
}

pub fn handleSack(self: *SackHandler, sack: *const message.Sack, fast_recovery: bool) bool {
    const cumulative_advanced = Helper.tsnGt(sack.cumulative_tsn, self.acked.cumulative_tsn);

    self.clearTsns(self.acked.cumulative_tsn +% 1, sack.cumulative_tsn);
    self.acked.setCumulativeTsn(sack.cumulative_tsn);
    self.acked.highest_tsn_received = sack.cumulative_tsn;

    var start_tsn = sack.cumulative_tsn +% 1;
    var htna: ?u32 = null;
    var it = sack.iterateGapAckBlocks();
    while (it.nextBlock() catch unreachable) |block| {
        self.acked.highest_tsn_received = block.end;
        self.acked.setRange(start_tsn, block.start -% 1, false);
        start_tsn = block.end +% 1;
        htna = self.acked.lastUnacked(block.start, block.end) orelse continue;
        self.acked.setRange(block.start, htna.?, true);
    }

    // RFC9260 - 7.2.4. miss indication counter incremented for all missing TSN in fast recovery mode.
    const end_tsn = if (cumulative_advanced and fast_recovery) self.acked.highest_tsn_received else htna orelse return false;
    return self.incrementMissCounts(sack.cumulative_tsn +% 1, end_tsn);
}

pub fn handleCumulativeTsn(self: *SackHandler, cumulative_tsn: u32) void {
    if (!Helper.tsnGt(cumulative_tsn, self.acked.cumulative_tsn)) return;
    self.clearTsns(self.acked.cumulative_tsn +% 1, cumulative_tsn);
    self.acked.setCumulativeTsn(cumulative_tsn);
}

pub fn isAcked(self: *const SackHandler, tsn: u32) bool {
    return self.acked.isAcked(tsn);
}

pub fn isRangeAcked(self: *const SackHandler, start_tsn: u32, end_tsn: u32) bool {
    return self.acked.isRangeAcked(start_tsn, end_tsn);
}

pub fn allAcked(self: *const SackHandler, highest_tsn_sent: u32) bool {
    return self.acked.allAcked(highest_tsn_sent);
}

pub fn iterateGapAcked(self: *const SackHandler) TsnBitSet.GapAckedIterator {
    return self.acked.iterateGapAcked();
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
    var it = TsnBitSet.WordIterator.init(start_tsn, self.acked.highest_tsn_received, false);
    while (it.next()) |word| {
        const candidates = self.miss_hi.masks[word.index] & self.miss_lo.masks[word.index] &
            ~self.acked.acked_tsn.masks[word.index] & ~self.fast_retransmitted.masks[word.index] &
            ~self.abandoned.masks[word.index];
        const bits = candidates & word.mask;
        if (bits != 0) return word.firstTsn(bits);
    }
    return null;
}

// RFC 3758
pub fn markAbandoned(self: *SackHandler, start_tsn: u32, end_tsn: u32) void {
    std.debug.assert(Helper.tsnGt(start_tsn, self.acked.cumulative_tsn));
    if (start_tsn == end_tsn) {
        self.abandoned.set(bitIndex(start_tsn));
        return;
    }
    var it = TsnBitSet.WordIterator.init(start_tsn, end_tsn, false);
    while (it.next()) |word| self.abandoned.masks[word.index] |= word.mask;
}

pub fn isAbandoned(self: *const SackHandler, tsn: u32) bool {
    return isBitSet(&self.abandoned, tsn);
}

pub fn moveAdvancedPeerAckPoint(self: *SackHandler, highest_tsn_sent: u32) void {
    if (Helper.tsnGt(self.acked.cumulative_tsn, self.advanced_peer_ack_point)) {
        self.advanced_peer_ack_point = self.acked.cumulative_tsn;
    }

    if (!Helper.tsnGt(highest_tsn_sent, self.advanced_peer_ack_point)) return;

    // Advance over the abandoned tsns, up to the tsn just below the first one that is not abandoned.
    var it = TsnBitSet.WordIterator.init(self.advanced_peer_ack_point +% 1, highest_tsn_sent, false);
    while (it.next()) |word| {
        const bits = ~self.abandoned.masks[word.index] & word.mask;
        if (bits != 0) {
            self.advanced_peer_ack_point = word.firstTsn(bits) -% 1;
            return;
        }
    }
    self.advanced_peer_ack_point = highest_tsn_sent;
}

pub fn firstUnackedNotAbandonedTsn(self: *const SackHandler, start_tsn: u32, end_tsn: u32) ?u32 {
    var it = TsnBitSet.WordIterator.init(start_tsn, end_tsn, false);
    while (it.next()) |word| {
        const bits = ~self.acked.acked_tsn.masks[word.index] & ~self.abandoned.masks[word.index] & word.mask;
        if (bits != 0) return word.firstTsn(bits);
    }
    return null;
}

pub const AbandonedUnackedIterator = struct {
    sack_handler: *const SackHandler,
    words: TsnBitSet.WordIterator,
    /// set bits of the current word not yet returned
    bits: BitSet.MaskInt,
    /// tsn of the current word's bit 0
    base_tsn: u32,

    pub fn next(self: *AbandonedUnackedIterator) ?u32 {
        while (true) {
            while (self.bits == 0) {
                const word = self.words.next() orelse return null;
                self.bits = self.sack_handler.abandoned.masks[word.index] & word.mask;
                self.base_tsn = word.base_tsn;
            }

            const tsn = self.base_tsn +% @ctz(self.bits);
            self.bits &= self.bits - 1;
            if (self.sack_handler.isAcked(tsn)) continue;
            return tsn;
        }
    }
};

pub fn iterateAbandonedUnacked(self: *const SackHandler, end_tsn: u32) AbandonedUnackedIterator {
    return .{
        .sack_handler = self,
        .base_tsn = 0,
        .bits = 0,
        .words = .init(self.acked.cumulative_tsn +% 1, end_tsn, false),
    };
}

fn incrementMissCounts(self: *SackHandler, start_tsn: u32, end_tsn: u32) bool {
    var fast_retransmit = false;
    var it = TsnBitSet.WordIterator.init(start_tsn, end_tsn, false);
    while (it.next()) |word| {
        const missing = ~self.acked.acked_tsn.masks[word.index] & word.mask;

        // 2-bit saturating increment of (hi, lo) where missing is set: 00 -> 01 -> 10 -> 11 -> 11.
        const lo = self.miss_lo.masks[word.index];
        const hi = self.miss_hi.masks[word.index];
        const new_hi = hi | (lo & missing);
        const new_lo = (lo ^ missing) | (hi & lo);
        self.miss_hi.masks[word.index] = new_hi;
        self.miss_lo.masks[word.index] = new_lo;
        fast_retransmit = fast_retransmit or (new_hi & new_lo & missing & ~self.fast_retransmitted.masks[word.index]) != 0;
    }

    return fast_retransmit;
}

/// Clears the per-tsn state of tsns passed by the cumulative tsn, their bits are reused by tsn + bitmap_bits.
fn clearTsns(self: *SackHandler, start_tsn: u32, end_tsn: u32) void {
    if (Helper.tsnGt(start_tsn, end_tsn)) return;
    if (end_tsn -% start_tsn >= bitmap_bits) {
        self.clearFastRetransmit();
        self.abandoned = .empty;
        return;
    }

    var it = TsnBitSet.WordIterator.init(start_tsn, end_tsn, false);
    while (it.next()) |word| {
        self.miss_lo.masks[word.index] &= ~word.mask;
        self.miss_hi.masks[word.index] &= ~word.mask;
        self.fast_retransmitted.masks[word.index] &= ~word.mask;
        self.abandoned.masks[word.index] &= ~word.mask;
    }
}

inline fn isBitSet(bit_set: *const BitSet, tsn: u32) bool {
    const index = bitIndex(tsn);
    return (bit_set.masks[index / word_bits] >> @as(BitSet.ShiftInt, @truncate(index))) & 1 != 0;
}

fn missCount(self: *const SackHandler, tsn: u32) u2 {
    return @as(u2, @intFromBool(isBitSet(&self.miss_hi, tsn))) << 1 | @intFromBool(isBitSet(&self.miss_lo, tsn));
}

fn testHandleSack(h: *SackHandler, sack: *const message.Sack, highest_tsn_sent: u32, fast_recovery: bool) error{Discard}!bool {
    try h.validateSack(sack, highest_tsn_sent);
    return h.handleSack(sack, fast_recovery);
}

fn testSack(cumulative_tsn: u32, blocks: []const u8) message.Sack {
    return .{
        .cumulative_tsn = cumulative_tsn,
        .a_rwnd = 0,
        .blocks = blocks,
        .num_gap_ack_blocks = @intCast(blocks.len / 4),
    };
}

test "GapAckedIterator: yields the gap acked tsns in order" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3, 0, 5, 0, 5 }), 100, false);

    var it = h.iterateGapAcked();
    try testing.expectEqual(3, it.next().?);
    try testing.expectEqual(4, it.next().?);
    try testing.expectEqual(6, it.next().?);
    try testing.expectEqual(null, it.next());
}

test "GapAckedIterator: tsns wrapping around u32" {
    const init_tsn: u32 = 0xFFFFFFFA;
    var h = SackHandler.init(init_tsn);
    _ = try testHandleSack(&h, &testSack(init_tsn, &.{ 0, 2, 0, 3, 0, 8, 0, 10 }), init_tsn +% 100, false);

    var it = h.iterateGapAcked();
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
    _ = try testHandleSack(&h, &testSack(init_tsn, &.{ 0, 1, 0, 1, 0, 3, 0, 4, 0, 70, 0, 70, 0, 200, 0, 202 }), init_tsn +% 500, false);

    var it = h.iterateGapAcked();
    for ([_]u32{ 1, 3, 4, 70, 200, 201, 202 }) |offset| {
        try testing.expectEqual(init_tsn +% offset, it.next().?);
    }
    try testing.expectEqual(null, it.next());
}

test "GapAckedIterator: nothing to iterate without gap blocks" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(5, &.{}), 100, false);

    var it = h.iterateGapAcked();
    try testing.expectEqual(null, it.next());
}

test "SackHandler.handleSack: gap ack blocks mark tsns as acked" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3 }), 100, false);

    try testing.expect(h.isAcked(1));
    try testing.expect(!h.isAcked(2));
    try testing.expect(h.isAcked(3));
    try testing.expect(h.isAcked(4));
    try testing.expect(!h.isAcked(5));
}

test "SackHandler.handleSack: a sack with an unchanged cumulative tsn still updates gap blocks" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 2 }), 100, false);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 4 }), 100, false);

    try testing.expect(h.isAcked(3));
    try testing.expect(h.isAcked(5));
}

test "SackHandler.handleSack: a sack with an older cumulative tsn is discarded" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(5, &.{}), 100, false);
    try testing.expectError(error.Discard, testHandleSack(&h, &testSack(4, &.{}), 100, false));
    try testing.expectEqual(5, h.acked.cumulative_tsn);
}

test "SackHandler.handleSack: bits acked by old gap blocks are cleared once the cumulative tsn passes them" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3 }), 100, false);
    _ = try testHandleSack(&h, &testSack(4, &.{}), 100, false);

    try testing.expect(!isBitSet(&h.acked.acked_tsn, 3));
    try testing.expect(!isBitSet(&h.acked.acked_tsn, 4));
    try testing.expect(!h.isAcked(3 + bitmap_bits));
}

test "SackHandler.handleSack: reneged gap blocks are no longer acked" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3 }), 100, false);
    _ = try testHandleSack(&h, &testSack(1, &.{}), 100, false);

    try testing.expect(!h.isAcked(3));
    try testing.expect(!h.isAcked(4));
}

test "SackHandler.handleSack: an invalid gap block discards the sack without changing state" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 2 }), 100, false);
    try testing.expectError(error.Discard, testHandleSack(&h, &testSack(3, &.{ 0, 4, 0, 1 }), 100, false));

    try testing.expectEqual(1, h.acked.cumulative_tsn);
    try testing.expect(h.isAcked(3));
}

test "SackHandler.handleSack: unordered or overlapping gap blocks discard the sack without changing state" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 2 }), 100, false);
    try testing.expectError(error.Discard, testHandleSack(&h, &testSack(3, &.{ 0, 5, 0, 6, 0, 2, 0, 3 }), 100, false));
    try testing.expectError(error.Discard, testHandleSack(&h, &testSack(3, &.{ 0, 2, 0, 4, 0, 4, 0, 6 }), 100, false));
    try testing.expectError(error.Discard, testHandleSack(&h, &testSack(3, &.{ 0, 2, 0, 4, 0, 2, 0, 4 }), 100, false));

    try testing.expectEqual(1, h.acked.cumulative_tsn);
    try testing.expect(h.isAcked(3));
}

test "SackHandler.handleSack: a sack acknowledging tsns that were never sent is discarded" {
    var h = SackHandler.init(1);
    try testing.expectError(error.Discard, testHandleSack(&h, &testSack(6, &.{}), 5, false));
    try testing.expectError(error.Discard, testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 5 }), 5, false));

    try testing.expectEqual(0, h.acked.cumulative_tsn);
    try testing.expect(!h.isAcked(3));
}

test "SackHandler.handleSack: missing tsns below the highest newly acked tsn get a miss indication" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3, 0, 5, 0, 5 }), 100, false);

    try testing.expectEqual(1, h.missCount(2));
    try testing.expectEqual(1, h.missCount(5));
    for ([_]u32{ 3, 4, 6, 7 }) |tsn| try testing.expectEqual(0, h.missCount(tsn));
}

test "SackHandler.handleSack: miss indications follow htna and saturate at 3" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3 }), 100, false);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3, 0, 7, 0, 7 }), 100, false);
    try testing.expectEqual(2, h.missCount(2));
    for ([_]u32{ 5, 6, 7 }) |tsn| try testing.expectEqual(1, h.missCount(tsn));

    // nothing newly acked
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3, 0, 7, 0, 7 }), 100, false);
    try testing.expectEqual(2, h.missCount(2));

    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3, 0, 7, 0, 8 }), 100, false);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3, 0, 7, 0, 9 }), 100, false);
    try testing.expectEqual(3, h.missCount(2));
    for ([_]u32{ 5, 6, 7 }) |tsn| try testing.expectEqual(3, h.missCount(tsn));
}

test "SackHandler.handleSack: missing tsns above htna get no miss indication" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 4, 0, 4 }), 100, false);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 2, 0, 4, 0, 4 }), 100, false);

    try testing.expectEqual(2, h.missCount(2));
    try testing.expectEqual(1, h.missCount(4));
}

test "SackHandler.handleSack: miss indications in fast recovery mode" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 4, 0, 4 }), 100, false);

    // cumulative tsn not advanced, follows htna
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 2, 0, 4, 0, 4 }), 100, true);
    try testing.expectEqual(2, h.missCount(2));
    try testing.expectEqual(1, h.missCount(4));

    // not in fast recovery, follows htna
    _ = try testHandleSack(&h, &testSack(2, &.{ 0, 1, 0, 1, 0, 3, 0, 3 }), 100, false);
    try testing.expectEqual(1, h.missCount(4));

    _ = try testHandleSack(&h, &testSack(3, &.{ 0, 2, 0, 2 }), 100, true);
    try testing.expectEqual(2, h.missCount(4));
}

test "SackHandler.handleSack: miss indications wrapping around the bitmap and spanning words" {
    const init_tsn: u32 = bitmap_bits * 5 - 3;
    var h = SackHandler.init(init_tsn);
    _ = try testHandleSack(&h, &testSack(init_tsn, &.{ 0, 70, 0, 70 }), init_tsn +% 100, false);

    for (1..70) |offset| try testing.expectEqual(1, h.missCount(init_tsn +% @as(u32, @intCast(offset))));
    try testing.expectEqual(0, h.missCount(init_tsn +% 70));
    try testing.expectEqual(0, h.missCount(init_tsn +% 71));
}

test "SackHandler.handleSack: returns true when a missing tsn has 3 miss indications" {
    var h = SackHandler.init(1);
    try testing.expect(!try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 3 }), 100, false));
    try testing.expect(!try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 4 }), 100, false));
    try testing.expect(try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 5 }), 100, false));

    h.markFastRetransmitted(2);
    h.markFastRetransmitted(3);
    try testing.expect(!try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 6 }), 100, false));
    try testing.expect(!try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 6 }), 100, false));
    try testing.expect(!try testHandleSack(&h, &testSack(3, &.{ 0, 4, 0, 4 }), 100, false));
}

test "SackHandler: miss counts are cleared once the cumulative tsn passes them" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 2, 0, 5, 0, 5 }), 100, false);
    try testing.expectEqual(1, h.missCount(2));
    try testing.expectEqual(1, h.missCount(5));

    _ = try testHandleSack(&h, &testSack(3, &.{ 0, 3, 0, 3 }), 100, false);
    try testing.expectEqual(0, h.missCount(2));
    try testing.expectEqual(0, h.missCount(2 + bitmap_bits));
    try testing.expectEqual(1, h.missCount(5));

    h.handleCumulativeTsn(6);
    try testing.expectEqual(0, h.missCount(5));
    try testing.expectEqual(0, h.missCount(5 + bitmap_bits));
}

test "SackHandler.handleCumulativeTsn: cumulative tsn clears the gap bits it passes" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3, 0, 5, 0, 5 }), 100, false);
    h.handleCumulativeTsn(4);

    try testing.expectEqual(4, h.acked.cumulative_tsn);
    try testing.expect(!isBitSet(&h.acked.acked_tsn, 3));
    try testing.expect(!isBitSet(&h.acked.acked_tsn, 4));
    try testing.expect(h.isAcked(6));
    try testing.expect(!h.isAcked(3 + bitmap_bits));
}

test "SackHandler.handleCumulativeTsn: cumulative tsn past the highest tsn updates it" {
    var h = SackHandler.init(1);
    h.handleCumulativeTsn(10);

    try testing.expectEqual(10, h.acked.highest_tsn_received);
    _ = try testHandleSack(&h, &testSack(10, &.{ 0, 2, 0, 2 }), 100, false);
    try testing.expect(h.isAcked(12));
    try testing.expect(!h.isAcked(11));
}

test "SackHandler.handleCumulativeTsn: older cumulative tsn is ignored" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(5, &.{}), 100, false);
    h.handleCumulativeTsn(3);

    try testing.expectEqual(5, h.acked.cumulative_tsn);
}

test "SackHandler.isRangeAcked: ranges spanning multiple words" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3, 0, 5, 0, 5, 0, 14, 0, 200 }), 1000, false);

    try testing.expect(h.isRangeAcked(3, 4));
    try testing.expect(h.isRangeAcked(70, 189));
    try testing.expect(!h.isRangeAcked(3, 5));
}

test "SackHandler.isRangeAcked: an unacked start tsn" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 4 }), 1000, false);

    try testing.expect(!h.isRangeAcked(3, 5));
    try testing.expect(h.isRangeAcked(4, 5));
}

test "SackHandler.isRangeAcked: range ending on the last bit of a word" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 60, 0, 70 }), 1000, false);

    try testing.expect(h.isRangeAcked(61, 63));
    try testing.expect(h.isRangeAcked(61, 71));
}

test "SackHandler.isRangeAcked: tsns wrapping around u32" {
    const init_tsn: u32 = 0xFFFFFFF0;
    var h = SackHandler.init(init_tsn);
    _ = try testHandleSack(&h, &testSack(init_tsn, &.{ 0, 30, 0, 30 }), init_tsn +% 100, false);

    try testing.expect(!h.isRangeAcked(0xFFFFFFF5, 5));
    try testing.expect(h.isRangeAcked(init_tsn +% 30, init_tsn +% 30));
}

test "SackHandler.isRangeAcked: ranges starting at or below the cumulative tsn" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(10, &.{ 0, 1, 0, 2 }), 1000, false);

    try testing.expect(h.isRangeAcked(5, 10));
    try testing.expect(h.isRangeAcked(5, 12));
    try testing.expect(!h.isRangeAcked(5, 13));
}

test "SackHandler.allAcked" {
    var h = SackHandler.init(1);
    try testing.expect(h.allAcked(0));

    // a hole
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 5 }), 6, false);
    try testing.expect(!h.allAcked(6));

    // a trailing unacked tsn
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 1, 0, 4 }), 6, false);
    try testing.expect(!h.allAcked(6));

    // contiguous gap blocks reach the highest tsn sent
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 1, 0, 3, 0, 4, 0, 5 }), 6, false);
    try testing.expect(h.allAcked(6));

    // reneged gap blocks
    _ = try testHandleSack(&h, &testSack(1, &.{}), 6, false);
    try testing.expect(!h.allAcked(6));

    h.handleCumulativeTsn(5);
    try testing.expect(!h.allAcked(6));
    h.handleCumulativeTsn(6);
    try testing.expect(h.allAcked(6));

    // the cumulative tsn reaches the highest tsn sent
    _ = try testHandleSack(&h, &testSack(10, &.{}), 10, false);
    try testing.expect(h.allAcked(10));
    try testing.expect(!h.allAcked(12));
}

test "SackHandler.markFastRetransmitted: a fast retransmitted tsn is skipped until the cumulative tsn passes it" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 3 }), 100, false);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 4 }), 100, false);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 5 }), 100, false);

    h.markFastRetransmitted(2);
    try testing.expectEqual(3, h.firstTsnWithThreeMisses(2).?);
    h.markFastRetransmitted(3);
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(2));

    // still missing, a new sack does not make them eligible again
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 6 }), 100, false);
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(2));

    h.handleCumulativeTsn(3);
    try testing.expect(!isBitSet(&h.fast_retransmitted, 2));
    try testing.expect(!isBitSet(&h.fast_retransmitted, 3));
}

test "SackHandler.clearFastRetransmit" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 3 }), 100, false);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 4 }), 100, false);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 5 }), 100, false);
    h.markFastRetransmitted(2);
    h.markAbandoned(3, 3);

    h.clearFastRetransmit();
    try testing.expectEqual(0, h.missCount(2));
    try testing.expectEqual(0, h.missCount(3));
    try testing.expect(!isBitSet(&h.fast_retransmitted, 2));
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(2));
    try testing.expect(h.isAcked(4));
    try testing.expect(isBitSet(&h.abandoned, 3));
}

test "SackHandler.markAbandoned: abandoned bits are cleared once the cumulative tsn passes them" {
    {
        var h = SackHandler.init(1);
        h.markAbandoned(3, 3);
        h.markAbandoned(5, 5);
        h.handleCumulativeTsn(4);
        try testing.expect(!isBitSet(&h.abandoned, 3));
        try testing.expect(isBitSet(&h.abandoned, 5));
    }

    // cumulative tsn jumping over the whole bitmap
    {
        var h = SackHandler.init(1);
        h.markAbandoned(3, 3);
        h.handleCumulativeTsn(2 * bitmap_bits);
        try testing.expect(!isBitSet(&h.abandoned, 3));
    }
}

test "SackHandler.moveAdvancedPeerAckPoint" {
    // stops before the first tsn that is not abandoned
    {
        var h = SackHandler.init(1);
        h.markAbandoned(1, 2);
        h.markAbandoned(4, 4);
        h.moveAdvancedPeerAckPoint(10);
        try testing.expectEqual(2, h.advanced_peer_ack_point);
    }

    // a gap acked tsn that is not abandoned stops it
    {
        var h = SackHandler.init(1);
        h.acked.ack(2);
        h.markAbandoned(1, 1);
        h.markAbandoned(3, 3);
        h.moveAdvancedPeerAckPoint(10);
        try testing.expectEqual(1, h.advanced_peer_ack_point);
    }

    // all sent tsns abandoned
    {
        var h = SackHandler.init(1);
        h.markAbandoned(1, 6);
        h.moveAdvancedPeerAckPoint(5);
        try testing.expectEqual(5, h.advanced_peer_ack_point);
    }

    // starts from the cumulative tsn when it is ahead
    {
        var h = SackHandler.init(1);
        h.handleCumulativeTsn(3);
        h.markAbandoned(4, 4);
        h.moveAdvancedPeerAckPoint(10);
        try testing.expectEqual(4, h.advanced_peer_ack_point);
    }

    // nothing sent above the cumulative tsn
    {
        var h = SackHandler.init(1);
        h.moveAdvancedPeerAckPoint(0);
        try testing.expectEqual(0, h.advanced_peer_ack_point);
    }

    // tsns wrapping around u32 and spanning words
    {
        const init_tsn: u32 = 0xFFFFFFF0;
        var h = SackHandler.init(init_tsn);
        h.markAbandoned(init_tsn, init_tsn +% 99);
        h.moveAdvancedPeerAckPoint(init_tsn +% 200);
        try testing.expectEqual(init_tsn +% 99, h.advanced_peer_ack_point);
    }
}

test "SackHandler.firstTsnWithThreeMisses" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 3 }), 100, false);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 4 }), 100, false);
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(2));

    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 3, 0, 5 }), 100, false);
    try testing.expectEqual(2, h.firstTsnWithThreeMisses(2).?);
    try testing.expectEqual(3, h.firstTsnWithThreeMisses(3).?);
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(4));

    // a gap acked tsn is skipped even with 3 misses
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 1, 0, 1, 0, 3, 0, 6 }), 100, false);
    try testing.expectEqual(3, h.firstTsnWithThreeMisses(2).?);
}

test "SackHandler.firstTsnWithThreeMisses: tsns wrapping around the bitmap and spanning words" {
    const init_tsn: u32 = bitmap_bits * 5 - 3;
    var h = SackHandler.init(init_tsn);
    _ = try testHandleSack(&h, &testSack(init_tsn, &.{ 0, 1, 0, 69, 0, 71, 0, 71 }), init_tsn +% 100, false);
    _ = try testHandleSack(&h, &testSack(init_tsn, &.{ 0, 1, 0, 69, 0, 71, 0, 72 }), init_tsn +% 100, false);
    _ = try testHandleSack(&h, &testSack(init_tsn, &.{ 0, 1, 0, 69, 0, 71, 0, 73 }), init_tsn +% 100, false);

    try testing.expectEqual(init_tsn +% 70, h.firstTsnWithThreeMisses(init_tsn +% 1).?);
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(init_tsn +% 71));
}

test "SackHandler.firstTsnWithThreeMisses: nothing to search without gap blocks" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(5, &.{}), 100, false);
    try testing.expectEqual(null, h.firstTsnWithThreeMisses(h.acked.cumulative_tsn +% 1));
}

test "SackHandler.handleSack: lastUnacked" {
    var h = SackHandler.init(1);
    _ = try testHandleSack(&h, &testSack(1, &.{ 0, 2, 0, 3, 0, 5, 0, 5, 0, 60, 0, 200 }), 1000, false);

    // bits set: 3, 4, 6, 61..201
    try testing.expectEqual(null, h.acked.lastUnacked(3, 4));
    try testing.expectEqual(5, h.acked.lastUnacked(3, 6));
    try testing.expectEqual(2, h.acked.lastUnacked(2, 4));
    try testing.expectEqual(60, h.acked.lastUnacked(10, 201));
    try testing.expectEqual(null, h.acked.lastUnacked(61, 201));
    try testing.expectEqual(202, h.acked.lastUnacked(61, 202));
}

test "SackHandler.handleSack: lastUnacked: tsns wrapping around the bitmap and u32" {
    const init_tsn: u32 = 0xFFFFFFF0;
    var h = SackHandler.init(init_tsn);
    _ = try testHandleSack(&h, &testSack(init_tsn, &.{ 0, 5, 0, 30 }), init_tsn +% 100, false);

    try testing.expectEqual(null, h.acked.lastUnacked(init_tsn +% 5, init_tsn +% 30));
    try testing.expectEqual(init_tsn +% 4, h.acked.lastUnacked(init_tsn +% 1, init_tsn +% 30));
}
