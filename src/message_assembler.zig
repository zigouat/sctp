const std = @import("std");
const testing = std.testing;
const message = @import("message.zig");

const MessageAssembler = @This();
const UserMessage = message.UserMessage;
const SackGenerator = @import("sack_generator.zig");
const TsnBitSet = @import("tsn_bit_set.zig");
const Helper = @import("helper.zig");
const BitSet = std.bit_set.StaticBitSet(bitmap_bits);
const bitmap_bits = SackGenerator.bitmap_bits;

const StreamState = struct {
    next_ssn: u16,
    complete_messages: std.AutoArrayHashMapUnmanaged(u16, TsnRange),

    const init = StreamState{ .next_ssn = 0, .complete_messages = .empty };

    fn deinit(self: *StreamState, allocator: std.mem.Allocator) void {
        self.complete_messages.deinit(allocator);
    }
};

pub const AllocError = std.mem.Allocator.Error;
pub const ReceiveError = AllocError || error{NoUserData};
pub const ForwardTsnError = AllocError || error{ProtocolViolation};

pub const sack_delay = 200;

const TsnRange = struct {
    start: u32,
    end: u32,
};

const Ready = union(enum) {
    range: TsnRange,
    message: UserMessage, // single chunk messages
};

sack_generator: SackGenerator,
/// start and end fragments of the messages not yet complete, single chunk messages are not tracked.
boundary: BitSet,
/// the start of the oldest message not yet complete, the tsn after the cumulative tsn when there is none.
/// It only moves once that message completes, a message abandoned without a FORWARD-TSN holds it forever.
lowest_held_tsn: u32,
send_sack: bool,
sack_deadline: i64,
/// chunks of the messages not yet drained.
pending: std.AutoArrayHashMapUnmanaged(u32, message.Data),
streams: std.AutoHashMapUnmanaged(u16, StreamState),
ready: std.Deque(Ready),
/// fragmented messages are assembled here when drained, it's as large as the largest complete message received.
assembly_buffer: []u8,
/// the single chunk message returned by the last drain, freed by the next one.
drained_chunk: []const u8,
capacity: u32,
buffered_data: u32,
advertised_wnd: u32,

pub fn init(capacity: u32) MessageAssembler {
    return .{
        .sack_generator = .init(0),
        .boundary = .empty,
        .lowest_held_tsn = 0,
        // RFC9260 - 5.1. the first DATA chunk is acknowledged immediately
        .send_sack = true,
        .sack_deadline = std.math.maxInt(i64),
        .pending = .empty,
        .streams = .empty,
        .ready = .empty,
        .assembly_buffer = &.{},
        .drained_chunk = &.{},
        .capacity = capacity,
        .buffered_data = 0,
        .advertised_wnd = capacity,
    };
}

pub fn deinit(self: *MessageAssembler, allocator: std.mem.Allocator) void {
    for (self.pending.values()) |d| allocator.free(d.user_data);
    self.pending.deinit(allocator);

    var streams_it = self.streams.valueIterator();
    while (streams_it.next()) |state| state.deinit(allocator);
    self.streams.deinit(allocator);

    while (self.ready.popFront()) |ready| switch (ready) {
        .range => {},
        .message => |msg| msg.deinit(allocator),
    };
    self.ready.deinit(allocator);

    allocator.free(self.assembly_buffer);
    allocator.free(self.drained_chunk);
}

pub fn reset(self: *MessageAssembler, allocator: std.mem.Allocator) void {
    self.deinit(allocator);
    self.* = .init(self.capacity);
}

pub fn setInitTsn(self: *MessageAssembler, init_tsn: u32) void {
    self.sack_generator = .init(init_tsn);
    self.lowest_held_tsn = init_tsn;
}

pub fn receiveData(self: *MessageAssembler, allocator: std.mem.Allocator, d: message.Data) ReceiveError!bool {
    if (d.user_data.len == 0) {
        @branchHint(.cold);
        return error.NoUserData;
    }
    self.sack_generator.checkTsn(d.tsn) catch |err| return err == error.Duplicate;

    if (d.tsn -% self.lowest_held_tsn >= bitmap_bits) {
        @branchHint(.cold);
        return false;
    }

    const window_full = self.advertised_wnd == 0;
    if (window_full) {
        @branchHint(.unlikely);
        if (!self.dropChunkWithHighestTsn(allocator, d)) return true;
    }

    const stream_state = if (d.flags.unordered) null else try self.getStreamState(allocator, d.stream_id);
    try self.ready.ensureUnusedCapacity(allocator, if (stream_state) |state| state.complete_messages.count() + 1 else 1);

    if (d.isComplete() and (stream_state == null or d.stream_seq == stream_state.?.next_ssn)) {
        try self.deliverSingleChunk(allocator, stream_state, d);
    } else try self.storeFragment(allocator, stream_state, d);

    return window_full or d.flags.immediate;
}

pub fn handleForwardTsn(self: *MessageAssembler, allocator: std.mem.Allocator, fwd_tsn: *const message.ForwardTsn) ForwardTsnError!void {
    if (Helper.tsnLte(fwd_tsn.new_cumulative_tsn, self.getCumulativeTsn())) return;
    // a message abandoned while being fragmented ends at its last sent fragment, but its sent fragments are abandoned together
    if (self.pending.getPtr(fwd_tsn.new_cumulative_tsn)) |chunk| if (!chunk.flags.end_fragment) {
        const next = self.pending.getPtr(fwd_tsn.new_cumulative_tsn +% 1);
        if (next != null and !next.?.flags.start_fragment) return error.ProtocolViolation;
    };

    var streams_it = fwd_tsn.iterateStreams();
    var max_ready_counts: u32 = 0;
    while (streams_it.next()) |stream| {
        const stream_state = try self.getStreamState(allocator, stream.@"0");
        max_ready_counts += @intCast(stream_state.complete_messages.count());
    }
    try self.ready.ensureUnusedCapacity(allocator, max_ready_counts);

    const received = &self.sack_generator.received;
    const last_tsn = if (Helper.tsnLte(fwd_tsn.new_cumulative_tsn, received.highest_tsn_received))
        fwd_tsn.new_cumulative_tsn
    else
        received.highest_tsn_received;

    // fragments of a message take consecutive tsns, so walking from the edge of a received run never reaches a complete message
    self.dropFragmentsUntil(allocator, received.cumulative_tsn);
    var blocks = received.iterateGapBlocks();
    while (blocks.next()) |block| {
        if (Helper.tsnGt(block.start, last_tsn)) break;
        self.dropFragmentsFrom(allocator, block.start);
        self.dropFragmentsUntil(allocator, if (Helper.tsnLte(block.end, last_tsn)) block.end else last_tsn);
    }

    self.sack_generator.setCumulativeTsn(fwd_tsn.new_cumulative_tsn);
    self.moveLowestHeldTsn();

    var it = fwd_tsn.iterateStreams();
    while (it.next()) |stream| {
        const stream_id, const stream_seq = stream;
        const stream_state = self.streams.getPtr(stream_id).?;
        if (@as(i16, @bitCast(stream_seq -% stream_state.next_ssn)) < 0) continue;

        while (stream_state.next_ssn != stream_seq +% 1) : (stream_state.next_ssn +%= 1) {
            const entry = stream_state.complete_messages.fetchSwapRemove(stream_state.next_ssn) orelse continue;
            self.ready.pushBackAssumeCapacity(.{ .range = entry.value });
        }

        if (stream_state.complete_messages.fetchSwapRemove(stream_state.next_ssn)) |entry| {
            self.deliverInOrder(stream_state, .{ .range = entry.value });
        }
    }
}

/// Acknowledges the chunk's tsn but drops its data.
///
/// Returns `true` if a sack should be sent immediately.
pub fn acknowledge(self: *MessageAssembler, d: message.Data) bool {
    self.sack_generator.checkTsn(d.tsn) catch |err| return err == error.Duplicate;
    self.sack_generator.receiveTsn(d.tsn);
    return d.flags.immediate;
}

pub fn scheduleSack(self: *MessageAssembler, immediate: bool, now: i64) bool {
    if (self.send_sack or immediate or self.hasGaps()) return true;
    self.send_sack = true;
    self.sack_deadline = now + sack_delay;
    return false;
}

pub fn clearDelayedSack(self: *MessageAssembler) void {
    self.send_sack = false;
    self.sack_deadline = std.math.maxInt(i64);
}

pub fn writeSack(self: *MessageAssembler, buffer: []u8) u32 {
    const written = self.sack_generator.writeSack(buffer, self.advertised_wnd);
    self.sack_generator.clearDuplicates();
    return written;
}

pub fn drainReady(self: *MessageAssembler, allocator: std.mem.Allocator) ?UserMessage {
    allocator.free(self.drained_chunk);
    self.drained_chunk = &.{};

    const msg = switch (self.ready.popFront() orelse return null) {
        .message => |msg| blk: {
            self.drained_chunk = msg.data;
            break :blk msg;
        },
        .range => |range| blk: {
            var msg = self.pending.get(range.start).?.toUserMessage();
            if (range.start == range.end) {
                _ = self.pending.swapRemove(range.start);
                self.drained_chunk = msg.data;
            } else msg.data = self.assembleMessage(allocator, range);
            break :blk msg;
        },
    };

    self.buffered_data -= @intCast(msg.data.len);
    self.updateAdvertisedWindow();
    return msg;
}

pub fn getCumulativeTsn(self: *const MessageAssembler) u32 {
    return self.sack_generator.received.cumulative_tsn;
}

pub fn hasGaps(self: *const MessageAssembler) bool {
    return self.sack_generator.received.cumulative_tsn != self.sack_generator.received.highest_tsn_received;
}

/// Restart the incoming stream sequence number of `streams` at 0, an empty slice resets all the streams.
pub fn resetStreams(self: *MessageAssembler, allocator: std.mem.Allocator, streams: []const u16) void {
    if (streams.len == 0) {
        var it = self.streams.valueIterator();
        while (it.next()) |state| self.resetStream(allocator, state);
        return;
    }

    for (streams) |stream| {
        if (self.streams.getPtr(stream)) |state| self.resetStream(allocator, state);
    }
}

/// Restarts the stream's ssn at 0 and frees the completed messages still waiting for an earlier ssn.
/// A well behaving peer has all its messages delivered before the reset, so there are none to free.
fn resetStream(self: *MessageAssembler, allocator: std.mem.Allocator, state: *StreamState) void {
    state.next_ssn = 0;
    for (state.complete_messages.values()) |range| self.dropMessage(allocator, range);
    state.complete_messages.clearRetainingCapacity();
}

fn dropMessage(self: *MessageAssembler, allocator: std.mem.Allocator, range: TsnRange) void {
    var tsn = range.start;
    while (true) : (tsn +%= 1) {
        _ = self.dropChunk(allocator, tsn);
        if (tsn == range.end) break;
    }
}

fn dropChunkWithHighestTsn(self: *MessageAssembler, allocator: std.mem.Allocator, d: message.Data) bool {
    if (!self.sack_generator.received.inGapRange(d.tsn)) return false;
    var high = self.sack_generator.received.highest_tsn_received;
    while (Helper.tsnGt(high, d.tsn)) : (high -%= 1) {
        const chunk = self.pending.get(high) orelse continue;
        // a ready message can't be withdrawn, the stream's next ssn already moved past it
        if (self.isReady(high)) continue;
        self.removeCompleted(chunk);
        _ = self.dropChunk(allocator, high);
        self.sack_generator.reneg(high);
        return true;
    }

    return false;
}

fn isReady(self: *const MessageAssembler, tsn: u32) bool {
    var it = self.ready.iterator();
    while (it.next()) |ready| {
        const range = switch (ready) {
            // not in pending, never a candidate for reneg
            .message => continue,
            .range => |range| range,
        };
        if (Helper.tsnLte(range.start, tsn) and Helper.tsnLte(tsn, range.end)) return true;
    }
    return false;
}

fn deliverSingleChunk(self: *MessageAssembler, allocator: std.mem.Allocator, stream_state: ?*StreamState, d: message.Data) std.mem.Allocator.Error!void {
    var msg = d.toUserMessage();
    msg.data = try allocator.dupe(u8, d.user_data);
    self.commitChunk(d);
    if (d.tsn == self.lowest_held_tsn) self.moveLowestHeldTsn();

    if (stream_state) |state| {
        self.deliverInOrder(state, .{ .message = msg });
    } else self.ready.pushBackAssumeCapacity(.{ .message = msg });
}

fn deliverInOrder(self: *MessageAssembler, stream_state: *StreamState, ready: Ready) void {
    self.ready.pushBackAssumeCapacity(ready);
    stream_state.next_ssn +%= 1;

    while (stream_state.complete_messages.fetchSwapRemove(stream_state.next_ssn)) |complete| {
        self.ready.pushBackAssumeCapacity(.{ .range = complete.value });
        stream_state.next_ssn +%= 1;
    }
}

fn storeFragment(self: *MessageAssembler, allocator: std.mem.Allocator, stream_state: ?*StreamState, d: message.Data) AllocError!void {
    if (stream_state) |state| try state.complete_messages.ensureUnusedCapacity(allocator, 1);

    // nothing below fails once the buffer is large enough for the message and the chunk is stored
    const tsns = self.completedMessage(d);
    if (tsns) |range| {
        // other messages lie between the boundaries, the peer abandoned this one
        const size = self.messageSize(range, d) orelse return self.dropAbandoned(allocator, d);
        if (range.start != range.end) try self.growAssemblyBuffer(allocator, size);
    }
    try self.storeChunk(allocator, d);
    self.commitChunk(d);

    if (tsns) |range| {
        self.completeMessage(allocator, stream_state, range, d.stream_seq);
    } else if (d.flags.start_fragment or d.flags.end_fragment) self.boundary.set(TsnBitSet.bitIndex(d.tsn));
}

fn completeMessage(self: *MessageAssembler, allocator: std.mem.Allocator, stream_state: ?*StreamState, range: TsnRange, ssn: u16) void {
    self.setBoundary(range, false);
    if (range.start == self.lowest_held_tsn) self.moveLowestHeldTsn();
    const state = stream_state orelse return self.ready.pushBackAssumeCapacity(.{ .range = range });
    if (ssn == state.next_ssn) return self.deliverInOrder(state, .{ .range = range });

    // a misbehaving peer sent another message with the ssn of a waiting one, keep the first
    const entry = state.complete_messages.getOrPutAssumeCapacity(ssn);
    if (entry.found_existing) {
        @branchHint(.cold);
        self.dropMessage(allocator, range);
    } else entry.value_ptr.* = range;
}

fn commitChunk(self: *MessageAssembler, d: message.Data) void {
    self.buffered_data += @intCast(d.user_data.len);
    self.advertised_wnd -|= @intCast(d.user_data.len);
    self.sack_generator.receiveTsn(d.tsn);
}

/// Removes the completed message `chunk` belongs to, its boundaries are set again so it completes once the chunk is received again.
fn removeCompleted(self: *MessageAssembler, chunk: message.Data) void {
    if (chunk.flags.unordered) return;
    const stream_state = self.streams.getPtr(chunk.stream_id) orelse return;
    const entry = stream_state.complete_messages.fetchSwapRemove(chunk.stream_seq) orelse return;
    self.setBoundary(entry.value, true);
}

fn setBoundary(self: *MessageAssembler, range: TsnRange, value: bool) void {
    if (range.start == range.end) return;
    self.boundary.setValue(TsnBitSet.bitIndex(range.start), value);
    self.boundary.setValue(TsnBitSet.bitIndex(range.end), value);
}

/// The tsn range of the message `d` completes, null if some of its fragments are still missing.
/// `d` is not received yet, its own flags stand for its boundary.
fn completedMessage(self: *const MessageAssembler, d: message.Data) ?TsnRange {
    const start_tsn = if (d.flags.start_fragment) d.tsn else self.findFragmentStartTsn(d.tsn) orelse return null;
    const end_tsn = if (d.flags.end_fragment) d.tsn else self.findFragmentEndTsn(d.tsn) orelse return null;
    return .{ .start = start_tsn, .end = end_tsn };
}

/// The nearest boundary before `tsn`, if no gap lies between them.
fn findFragmentStartTsn(self: *const MessageAssembler, tsn: u32) ?u32 {
    const received = &self.sack_generator.received;
    var it = TsnBitSet.WordIterator.init(tsn -% bitmap_bits +% 1, tsn -% 1, true);
    while (it.next()) |word| {
        const boundaries = self.boundary.masks[word.index] & word.mask;
        const gaps = ~received.acked_tsn.masks[word.index] & word.mask;
        // bits up to the cumulative tsn are cleared but received, @clz(0) loses against any set bit
        if (gaps != 0 and Helper.tsnGt(word.lastTsn(gaps), received.cumulative_tsn) and @clz(gaps) <= @clz(boundaries)) return null;
        if (boundaries != 0) return word.lastTsn(boundaries);
    }
    return null;
}

/// The nearest boundary after `tsn`, if no gap lies between them.
fn findFragmentEndTsn(self: *const MessageAssembler, tsn: u32) ?u32 {
    // tsns up to the cumulative tsn are received and the one after it is not, so no gap check is needed below it
    const received = &self.sack_generator.received;
    const check_gaps = Helper.tsnGt(tsn, received.cumulative_tsn);
    const end_tsn = if (check_gaps) received.highest_tsn_received else received.cumulative_tsn;

    var it = TsnBitSet.WordIterator.init(tsn +% 1, end_tsn, false);
    while (it.next()) |word| {
        const boundaries = self.boundary.masks[word.index] & word.mask;
        const gaps = if (check_gaps) ~received.acked_tsn.masks[word.index] & word.mask else 0;
        if (gaps != 0 and @ctz(gaps) <= @ctz(boundaries)) return null;
        if (boundaries != 0) return word.firstTsn(boundaries);
    }
    return null;
}

fn moveLowestHeldTsn(self: *MessageAssembler) void {
    const cumulative_tsn = self.sack_generator.received.cumulative_tsn;
    self.lowest_held_tsn = cumulative_tsn +% 1;
    // tsns up to the cumulative tsn are received, a chunk held there that is not an end belongs to an incomplete message
    const chunk = self.pending.getPtr(cumulative_tsn) orelse return;
    if (chunk.flags.end_fragment) return;
    if (chunk.flags.start_fragment) {
        self.lowest_held_tsn = cumulative_tsn;
    } else if (self.findFragmentStartTsn(cumulative_tsn)) |start_tsn| self.lowest_held_tsn = start_tsn;
}

fn dropChunk(self: *MessageAssembler, allocator: std.mem.Allocator, tsn: u32) bool {
    if (self.pending.fetchSwapRemove(tsn)) |d| {
        allocator.free(d.value.user_data);
        self.buffered_data -= @intCast(d.value.user_data.len);
        self.updateAdvertisedWindow();
        if (d.value.flags.start_fragment or d.value.flags.end_fragment) {
            self.boundary.unset(TsnBitSet.bitIndex(tsn));
        }
        return true;
    }

    return false;
}

fn dropFragmentsFrom(self: *MessageAssembler, allocator: std.mem.Allocator, start_tsn: u32) void {
    var tsn = start_tsn;
    while (self.pending.getPtr(tsn)) |chunk| : (tsn +%= 1) {
        if (chunk.flags.start_fragment) return;
        const end = chunk.flags.end_fragment;
        _ = self.dropChunk(allocator, tsn);
        if (end) return;
    }
}

fn dropFragmentsUntil(self: *MessageAssembler, allocator: std.mem.Allocator, end_tsn: u32) void {
    var tsn = end_tsn;
    while (self.pending.getPtr(tsn)) |chunk| : (tsn -%= 1) {
        if (chunk.flags.end_fragment) return;
        const start = chunk.flags.start_fragment;
        _ = self.dropChunk(allocator, tsn);
        if (start) return;
    }
}

fn dropAbandoned(self: *MessageAssembler, allocator: std.mem.Allocator, d: message.Data) void {
    if (!d.flags.start_fragment) self.dropFragmentsUntil(allocator, d.tsn -% 1);
    if (!d.flags.end_fragment) self.dropFragmentsFrom(allocator, d.tsn +% 1);
    self.sack_generator.receiveTsn(d.tsn);
}

fn storeChunk(self: *MessageAssembler, allocator: std.mem.Allocator, d: message.Data) !void {
    try self.pending.ensureUnusedCapacity(allocator, 1);
    var copy = d;
    copy.user_data = try allocator.dupe(u8, d.user_data);
    self.pending.putAssumeCapacity(d.tsn, copy);
}

fn updateAdvertisedWindow(self: *MessageAssembler) void {
    const avail = self.capacity -| self.buffered_data;
    const thresh = @min(message.PMDCS - message.data_chunk_header_size, self.capacity / 2);

    self.advertised_wnd = @min(avail, self.advertised_wnd);
    if (avail -| self.advertised_wnd >= thresh) {
        self.advertised_wnd = avail;
    }
}

/// The size of the message in `range`, null if its fragments are not the ones of a single message.
fn messageSize(self: *const MessageAssembler, range: TsnRange, d: message.Data) ?usize {
    var size: usize = 0;
    var tsn = range.start;
    while (true) : (tsn +%= 1) {
        const chunk = if (tsn == d.tsn) d else (self.pending.getPtr(tsn) orelse return null).*;
        if (chunk.flags.start_fragment != (tsn == range.start) or chunk.flags.end_fragment != (tsn == range.end)) return null;
        if (!sameMessage(chunk, d)) return null;
        size += chunk.user_data.len;
        if (tsn == range.end) return size;
    }
}

fn growAssemblyBuffer(self: *MessageAssembler, allocator: std.mem.Allocator, size: usize) AllocError!void {
    if (size <= self.assembly_buffer.len) return;
    const buffer = try allocator.alloc(u8, size);
    allocator.free(self.assembly_buffer);
    self.assembly_buffer = buffer;
}

fn assembleMessage(self: *MessageAssembler, allocator: std.mem.Allocator, range: TsnRange) []const u8 {
    var offset: usize = 0;
    var tsn = range.start;
    while (true) : (tsn +%= 1) {
        const fragment = self.pending.fetchSwapRemove(tsn).?.value.user_data;
        @memcpy(self.assembly_buffer[offset .. offset + fragment.len], fragment);
        offset += fragment.len;
        allocator.free(fragment);
        if (tsn == range.end) break;
    }
    return self.assembly_buffer[0..offset];
}

fn getStreamState(self: *MessageAssembler, allocator: std.mem.Allocator, stream_id: u16) !*StreamState {
    const state = try self.streams.getOrPut(allocator, stream_id);
    if (!state.found_existing) state.value_ptr.* = .init;
    return state.value_ptr;
}

fn sameMessage(a: message.Data, b: message.Data) bool {
    if (a.stream_id != b.stream_id or a.flags.unordered != b.flags.unordered) return false;
    return a.flags.unordered or a.stream_seq == b.stream_seq;
}

fn testData(tsn: u32, stream_id: u16, stream_seq: u16, flags: message.Data.Flags, payload: []const u8) message.Data {
    return .{
        .flags = flags,
        .tsn = tsn,
        .stream_id = stream_id,
        .stream_seq = stream_seq,
        .ppid = 0,
        .user_data = payload,
    };
}

test "MessageAssembler.deinit: frees pending fragments, buffered reorder entries and ready messages" {
    var r: MessageAssembler = .init(4000);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .start_frag, "partial"));
    _ = try r.receiveData(testing.allocator, testData(2, 0, 1, .complete, "buffered"));
    _ = try r.receiveData(testing.allocator, testData(3, 1, 0, .complete, "ready"));

    r.deinit(testing.allocator);
}

test "MessageAssembler.receiveData: single-chunk unordered message is delivered immediately" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .unordered_complete, "hello"));

    const msg = r.drainReady(testing.allocator).?;
    try testing.expectEqualStrings("hello", msg.data);
    try testing.expect(r.drainReady(testing.allocator) == null);
}

test "MessageAssembler.receiveData: single-chunk ordered message with ssn 0 is delivered immediately" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .complete, "hello"));

    const msg = r.drainReady(testing.allocator).?;
    try testing.expectEqualStrings("hello", msg.data);
    try testing.expect(r.drainReady(testing.allocator) == null);
}

test "MessageAssembler.receiveData: unordered message bypasses a pending ordered reorder buffer" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 1, .complete, "ordered-second"));
    try testing.expect(r.drainReady(testing.allocator) == null);

    _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .unordered_complete, "unordered"));
    const msg = r.drainReady(testing.allocator).?;
    try testing.expectEqualStrings("unordered", msg.data);

    // the ordered ssn-1 message is still pending, unaffected by the unordered delivery
    try testing.expect(r.drainReady(testing.allocator) == null);
}

test "MessageAssembler.receiveData: zero receive window drops or reneg chunks" {
    var r = MessageAssembler.init(100);
    defer r.deinit(testing.allocator);
    const tsn = 0x12345678;
    r.setInitTsn(tsn);

    const payload: [100]u8 = @splat(0xAA);

    // ssn 1 waits for ssn 0
    try testing.expect(!try r.receiveData(testing.allocator, testData(tsn +% 1, 1, 1, .complete, payload[0..60])));
    try testing.expect(r.drainReady(testing.allocator) == null);
    try testing.expect(r.pending.contains(tsn +% 1));
    try testing.expectEqual(40, r.advertised_wnd);

    // unordered message is delivered right away and closes the window
    try testing.expect(!try r.receiveData(testing.allocator, testData(tsn +% 3, 1, 0, .unordered_complete, payload[0..40])));
    try testing.expectEqual(0, r.advertised_wnd);
    try testing.expectEqual(tsn +% 3, r.sack_generator.received.highest_tsn_received);

    // a tsn above the highest one is dropped
    try testing.expect(try r.receiveData(testing.allocator, testData(tsn +% 4, 1, 0, .complete, payload[0..40])));
    try testing.expectEqual(tsn +% 3, r.sack_generator.received.highest_tsn_received);

    // the higher tsns are already delivered or never received, nothing to reneg
    try testing.expect(try r.receiveData(testing.allocator, testData(tsn +% 2, 1, 0, .complete, payload[0..40])));
    try testing.expectEqual(tsn +% 3, r.sack_generator.received.highest_tsn_received);
    try testing.expectEqual(tsn -% 1, r.sack_generator.received.cumulative_tsn);

    // ssn 1 held for reordering is reneged to accept ssn 0
    try testing.expect(try r.receiveData(testing.allocator, testData(tsn, 1, 0, .complete, payload[0..10])));
    try testing.expect(!r.pending.contains(tsn +% 1));
    try testing.expectEqual(tsn, r.sack_generator.received.cumulative_tsn);
    try testing.expectEqual(tsn +% 3, r.sack_generator.received.highest_tsn_received);
}

test "MessageAssembler.drainReady: re-assemble messages" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    {
        _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .start_frag, "hel"));
        try testing.expect(r.drainReady(testing.allocator) == null);

        _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .end_frag, "lo"));
        const msg = r.drainReady(testing.allocator).?;
        try testing.expectEqualStrings("hello", msg.data);
    }

    {
        _ = try r.receiveData(testing.allocator, testData(4, 0, 1, .end_frag, "lo"));
        try testing.expect(r.drainReady(testing.allocator) == null);

        _ = try r.receiveData(testing.allocator, testData(3, 0, 1, .start_frag, "hel"));
        const msg = r.drainReady(testing.allocator).?;
        try testing.expectEqualStrings("hello", msg.data);
    }

    {
        _ = try r.receiveData(testing.allocator, testData(5, 1, 0, .start_frag, "he"));
        _ = try r.receiveData(testing.allocator, testData(7, 1, 0, .end_frag, "lo"));
        try testing.expect(r.drainReady(testing.allocator) == null);

        _ = try r.receiveData(testing.allocator, testData(6, 1, 0, .middle_frag, "l"));
        const msg = r.drainReady(testing.allocator).?;
        try testing.expectEqualStrings("hello", msg.data);
    }

    {
        _ = try r.receiveData(testing.allocator, testData(8, 2, 0, .unordered_start_frag, "hel"));
        try testing.expect(r.drainReady(testing.allocator) == null);

        _ = try r.receiveData(testing.allocator, testData(9, 2, 0, .unordered_end_frag, "lo"));
        const msg = r.drainReady(testing.allocator).?;
        try testing.expectEqualStrings("hello", msg.data);
    }
}

test "MessageAssembler.drainReady: re-ordered messages are drained in order" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, testData(3, 0, 2, .complete, "third"));
    _ = try r.receiveData(testing.allocator, testData(2, 0, 1, .complete, "second"));
    try testing.expect(r.drainReady(testing.allocator) == null);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .complete, "first"));

    for ([_][]const u8{ "first", "second", "third" }) |expected| {
        const msg = r.drainReady(testing.allocator).?;
        try testing.expectEqualStrings(expected, msg.data);
    }

    try testing.expect(r.drainReady(testing.allocator) == null);
}

test "MessageAssembler: independent streams track ssn separately" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, testData(1, 1, 0, .complete, "stream1"));
    _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .complete, "stream0"));

    const a = r.drainReady(testing.allocator).?;
    try testing.expectEqualStrings("stream1", a.data);

    const b = r.drainReady(testing.allocator).?;
    try testing.expectEqualStrings("stream0", b.data);
}

fn testExpectReady(r: *MessageAssembler, expected: []const []const u8) !void {
    for (expected) |data| {
        const msg = r.drainReady(testing.allocator).?;
        try testing.expectEqualStrings(data, msg.data);
    }
    try testing.expectEqual(null, r.drainReady(testing.allocator));
}

test "MessageAssembler.resetStreams: restarts the incoming ssn at 0" {
    {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);

        _ = try r.receiveData(testing.allocator, testData(1, 1, 0, .complete, "a"));
        _ = try r.receiveData(testing.allocator, testData(2, 2, 0, .complete, "b"));
        try testExpectReady(&r, &.{ "a", "b" });

        // Streams that are not reset keep their ssn
        r.resetStreams(testing.allocator, &.{ 1, 3 });
        _ = try r.receiveData(testing.allocator, testData(3, 1, 0, .complete, "c"));
        _ = try r.receiveData(testing.allocator, testData(4, 2, 1, .complete, "d"));
        try testExpectReady(&r, &.{ "c", "d" });
    }

    // An empty slice resets all the streams
    {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);

        _ = try r.receiveData(testing.allocator, testData(1, 1, 0, .complete, "a"));
        _ = try r.receiveData(testing.allocator, testData(2, 2, 0, .complete, "b"));
        try testExpectReady(&r, &.{ "a", "b" });

        r.resetStreams(testing.allocator, &.{});
        _ = try r.receiveData(testing.allocator, testData(3, 1, 0, .complete, "c"));
        _ = try r.receiveData(testing.allocator, testData(4, 2, 0, .complete, "d"));
        try testExpectReady(&r, &.{ "c", "d" });
    }
}

test "MessageAssembler: ssn wraparound at 0xFFFF advances to 0" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    try r.streams.put(testing.allocator, 0, .{ .next_ssn = 0xFFFF, .complete_messages = .empty });

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0xFFFF, .complete, "last"));
    const first = r.drainReady(testing.allocator).?;
    try testing.expectEqualStrings("last", first.data);
    try testing.expectEqual(@as(u16, 0), r.streams.get(0).?.next_ssn);

    _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .complete, "wrapped"));
    const second = r.drainReady(testing.allocator).?;
    try testing.expectEqualStrings("wrapped", second.data);
}

test "MessageAssembler: advertised window" {
    var r = MessageAssembler.init(4000);
    defer r.deinit(testing.allocator);

    try testing.expectEqual(4000, r.advertised_wnd);

    const data_chunk: [1000]u8 = @splat(0xBB);
    _ = try r.receiveData(testing.allocator, testData(0, 0, 0, .unordered_start_frag, &data_chunk));
    try testing.expectEqual(3000, r.advertised_wnd);

    _ = try r.receiveData(testing.allocator, testData(3, 0, 1, .start_frag, &data_chunk));
    try testing.expectEqual(2000, r.advertised_wnd);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .unordered_end_frag, &data_chunk));
    try testing.expectEqual(1000, r.advertised_wnd);

    _ = r.drainReady(testing.allocator).?;
    try testing.expectEqual(3000, r.advertised_wnd);

    _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .complete, data_chunk[0..100]));
    try testing.expectEqual(2900, r.advertised_wnd);

    _ = r.drainReady(testing.allocator).?;
    try testing.expectEqual(2900, r.advertised_wnd);
}

test "MessageAssembler.receiveData: allocation failure" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        pub fn run(alloc: std.mem.Allocator) !void {
            var r: MessageAssembler = .init(4000);
            defer r.deinit(alloc);

            _ = try r.receiveData(alloc, testData(1, 0, 0, .start_frag, "hel"));
            _ = try r.receiveData(alloc, testData(2, 0, 0, .end_frag, "lo"));
        }
    }.run, .{});

    try testing.checkAllAllocationFailures(testing.allocator, struct {
        pub fn run(alloc: std.mem.Allocator) !void {
            var r: MessageAssembler = .init(4000);
            defer r.deinit(alloc);

            _ = try r.receiveData(alloc, testData(2, 0, 0, .complete, "Hello Zig!"));
        }
    }.run, .{});

    try testing.checkAllAllocationFailures(testing.allocator, struct {
        pub fn run(alloc: std.mem.Allocator) !void {
            var r: MessageAssembler = .init(4000);
            defer r.deinit(alloc);

            _ = try r.receiveData(alloc, testData(3, 0, 1, .complete, "second"));
            _ = try r.receiveData(alloc, testData(2, 0, 0, .end_frag, "lo"));
            _ = try r.receiveData(alloc, testData(1, 0, 0, .start_frag, "hel"));
            _ = try r.receiveData(alloc, testData(4, 1, 0, .unordered_start_frag, "un"));
            _ = try r.receiveData(alloc, testData(5, 1, 0, .unordered_end_frag, "ordered"));
        }
    }.run, .{});
}

test "MessageAssembler.receiveData: lazy reassembly of ordered messages" {
    var r = MessageAssembler.init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .start_frag, "hel"));
    _ = try r.receiveData(testing.allocator, testData(3, 0, 1, .complete, "hola"));
    try testing.expectEqual(0, r.ready.len);

    _ = try r.receiveData(testing.allocator, testData(4, 0, 0, .unordered_complete, "Marhaba"));
    try testing.expectEqual(1, r.ready.len);

    _ = try r.receiveData(testing.allocator, testData(6, 0, 0, .unordered_end_frag, "jour"));
    try testing.expectEqual(1, r.ready.len);
    _ = try r.receiveData(testing.allocator, testData(5, 0, 0, .unordered_start_frag, "bon"));
    try testing.expectEqual(2, r.ready.len);

    // completes ssn 0 and releases ssn 1
    _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .end_frag, "lo"));
    try testing.expectEqual(4, r.ready.len);

    _ = try r.receiveData(testing.allocator, testData(7, 0, 0, .unordered_start_frag, "sa"));
    try testing.expectEqual(4, r.ready.len);
    _ = try r.receiveData(testing.allocator, testData(8, 0, 0, .unordered_end_frag, "lut"));
    try testing.expectEqual(5, r.ready.len);

    _ = try r.receiveData(testing.allocator, testData(10, 0, 3, .complete, "ciao"));
    try testing.expectEqual(5, r.ready.len);
    _ = try r.receiveData(testing.allocator, testData(9, 0, 2, .complete, "hej"));
    try testing.expectEqual(7, r.ready.len);

    // chunks are only assembled when drained, "Marhaba" and "hej" are delivered on arrival without going through pending
    try testing.expectEqual(8, r.pending.count());
    for ([_][]const u8{ "Marhaba", "bonjour", "hello", "hola", "salut", "hej", "ciao" }) |expected| {
        const msg = r.drainReady(testing.allocator).?;
        try testing.expectEqualStrings(expected, msg.data);
    }
    try testing.expectEqual(0, r.pending.count());
}

test "MessageAssembler.dropChunkWithHighestTsn: withdraws completed messages but not ready ones" {
    var r = MessageAssembler.init(100);
    defer r.deinit(testing.allocator);

    const payload: [30]u8 = @splat('a');
    // ssn 1 of stream 0 waits for ssn 0, stream 1 is ready
    _ = try r.receiveData(testing.allocator, testData(2, 0, 1, .start_frag, &payload));
    _ = try r.receiveData(testing.allocator, testData(3, 0, 1, .end_frag, &payload));
    _ = try r.receiveData(testing.allocator, testData(4, 1, 0, .complete, &@as([40]u8, @splat('b'))));
    try testing.expectEqual(0, r.advertised_wnd);

    // tsn 4 is ready so tsn 3 is dropped, withdrawing ssn 1
    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .complete, "c"));
    try testing.expect(!r.pending.contains(3));
    try testing.expect(!r.sack_generator.received.isAcked(3));
    try testing.expect(r.sack_generator.received.isAcked(4));
    try testing.expectEqual(0, r.streams.getPtr(0).?.complete_messages.count());
    try testing.expectEqual(2, TsnBitSet.firstSet(&r.boundary, 1, 3));
    try testing.expectEqual(2, r.ready.len);

    for ([_][]const u8{ &@as([40]u8, @splat('b')), "c" }) |expected| {
        const msg = r.drainReady(testing.allocator).?;
        try testing.expectEqualStrings(expected, msg.data);
    }

    // ssn 1 completes again once the dropped chunk is received again
    _ = try r.receiveData(testing.allocator, testData(3, 0, 1, .end_frag, &payload));
    const msg = r.drainReady(testing.allocator).?;
    try testing.expectEqualStrings(&(payload ++ payload), msg.data);
}

fn testForwardTsn(new_cumulative_tsn: u32, streams: []const u8) message.ForwardTsn {
    return .{ .new_cumulative_tsn = new_cumulative_tsn, .streams = streams };
}

test "MessageAssembler.handleForwardTsn: moves the next ssn of the skipped streams" {
    // the message waiting at the skipped ssn + 1 is delivered
    {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, testData(2, 0, 1, .complete, "second"));
        try r.handleForwardTsn(testing.allocator, &testForwardTsn(1, &.{ 0, 0, 0, 0 }));
        try testExpectReady(&r, &.{"second"});
    }
    // waiting messages up to the skipped ssn are delivered, then the ones following it
    {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, testData(2, 0, 1, .complete, "b"));
        _ = try r.receiveData(testing.allocator, testData(4, 0, 3, .complete, "d"));
        _ = try r.receiveData(testing.allocator, testData(5, 0, 4, .complete, "e"));
        try r.handleForwardTsn(testing.allocator, &testForwardTsn(3, &.{ 0, 0, 0, 2 }));
        try testExpectReady(&r, &.{ "b", "d", "e" });
        try testing.expectEqual(5, r.streams.get(0).?.next_ssn);
    }
    // a stream never received from
    {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);
        try r.handleForwardTsn(testing.allocator, &testForwardTsn(1, &.{ 0, 3, 0, 0 }));
        _ = try r.receiveData(testing.allocator, testData(2, 3, 1, .complete, "next"));
        try testExpectReady(&r, &.{"next"});
    }
    // a stale ssn doesn't move the next ssn back
    {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .complete, "a"));
        _ = try r.receiveData(testing.allocator, testData(2, 0, 1, .complete, "b"));
        try r.handleForwardTsn(testing.allocator, &testForwardTsn(3, &.{ 0, 0, 0, 0 }));
        try testing.expectEqual(2, r.streams.get(0).?.next_ssn);
    }
}

test "MessageAssembler.handleForwardTsn: drops incomplete messages and keeps complete ones" {
    // complete ordered and unordered messages are kept
    {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .start_frag, "hel"));
        _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .end_frag, "lo"));
        _ = try r.receiveData(testing.allocator, testData(3, 1, 0, .unordered_start_frag, "wor"));
        _ = try r.receiveData(testing.allocator, testData(4, 1, 0, .unordered_end_frag, "ld"));
        try r.handleForwardTsn(testing.allocator, &testForwardTsn(5, &.{ 0, 0, 0, 1 }));
        try testExpectReady(&r, &.{ "hello", "world" });
    }
    // fragments after a gap are dropped, a complete message after them is kept
    {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .middle_frag, "m"));
        _ = try r.receiveData(testing.allocator, testData(3, 0, 0, .end_frag, "e"));
        _ = try r.receiveData(testing.allocator, testData(4, 0, 0, .unordered_start_frag, "ok"));
        _ = try r.receiveData(testing.allocator, testData(5, 0, 0, .unordered_end_frag, "!"));
        try r.handleForwardTsn(testing.allocator, &testForwardTsn(5, &.{}));
        try testExpectReady(&r, &.{"ok!"});
        try testing.expectEqual(0, r.pending.count());
        try testing.expectEqual(5, r.getCumulativeTsn());
    }
    // a message crossing the old cumulative tsn is dropped
    {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, testData(0, 0, 0, .unordered_start_frag, "s"));
        _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .unordered_middle_frag, "m"));
        _ = try r.receiveData(testing.allocator, testData(3, 0, 0, .unordered_middle_frag, "m"));
        _ = try r.receiveData(testing.allocator, testData(4, 0, 0, .unordered_end_frag, "e"));
        try r.handleForwardTsn(testing.allocator, &testForwardTsn(4, &.{}));
        try testing.expectEqual(0, r.pending.count());
        try testing.expectEqual(0, r.boundary.count());
    }
    // an incomplete message at the end of a block is dropped, the waiting one before it is delivered
    {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, testData(3, 0, 1, .start_frag, "w"));
        _ = try r.receiveData(testing.allocator, testData(4, 0, 1, .end_frag, "ait"));
        _ = try r.receiveData(testing.allocator, testData(5, 0, 2, .start_frag, "x"));
        _ = try r.receiveData(testing.allocator, testData(6, 0, 2, .middle_frag, "x"));
        try r.handleForwardTsn(testing.allocator, &testForwardTsn(7, &.{ 0, 0, 0, 2 }));
        try testExpectReady(&r, &.{"wait"});
        try testing.expectEqual(0, r.pending.count());
        try testing.expectEqual(0, r.boundary.count());
    }
}

test "MessageAssembler.handleForwardTsn: message abandoned while being fragmented" {
    // the new cumulative tsn ends at the last sent fragment, tsn 0 is lost
    {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .unordered_start_frag, "a"));
        _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .unordered_middle_frag, "a"));
        try r.handleForwardTsn(testing.allocator, &testForwardTsn(2, &.{}));
        try testing.expectEqual(0, r.pending.count());
        try testing.expectEqual(0, r.boundary.count());
    }
    // a stored message cut in two
    {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .unordered_middle_frag, "a"));
        _ = try r.receiveData(testing.allocator, testData(3, 0, 0, .unordered_middle_frag, "b"));
        _ = try r.receiveData(testing.allocator, testData(4, 0, 0, .unordered_end_frag, "c"));
        try testing.expectError(error.ProtocolViolation, r.handleForwardTsn(testing.allocator, &testForwardTsn(3, &.{})));
        try testing.expectEqual(3, r.pending.count());
    }
}

test "MessageAssembler.handleForwardTsn: allocation failure" {
    var failing = testing.FailingAllocator.init(testing.allocator, .{ .fail_index = 0 });
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);
    try testing.expectError(error.OutOfMemory, r.handleForwardTsn(failing.allocator(), &testForwardTsn(1, &.{ 0, 3, 0, 0 })));
    try testing.expectEqual(0xFFFFFFFF, r.getCumulativeTsn());

    try testing.checkAllAllocationFailures(testing.allocator, struct {
        fn run(allocator: std.mem.Allocator) !void {
            var assembler: MessageAssembler = .init(4000);
            defer assembler.deinit(allocator);
            _ = try assembler.receiveData(allocator, testData(2, 0, 1, .complete, "b"));
            try assembler.handleForwardTsn(allocator, &testForwardTsn(1, &.{ 0, 0, 0, 0, 0, 3, 0, 0 }));
        }
    }.run, .{});
}

test "MessageAssembler.receiveData: a message abandoned while being fragmented does not block the next one" {
    // A: start 0, middle 1, its other fragments never sent, B: start 2, end 3
    const chunks = [_]message.Data{
        testData(0, 0, 0, .unordered_start_frag, "a"),
        testData(1, 0, 0, .unordered_middle_frag, "a"),
        testData(2, 0, 0, .unordered_start_frag, "b"),
        testData(3, 0, 0, .unordered_end_frag, "b"),
    };
    const orders = [_][4]u8{
        .{ 0, 1, 2, 3 }, .{ 0, 2, 1, 3 }, .{ 0, 2, 3, 1 }, .{ 2, 3, 0, 1 }, .{ 2, 0, 1, 3 },
        .{ 1, 0, 2, 3 }, .{ 1, 2, 0, 3 }, .{ 3, 2, 1, 0 }, .{ 2, 1, 3, 0 }, .{ 3, 1, 0, 2 },
    };
    for (orders) |order| {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);
        for (order) |i| _ = try r.receiveData(testing.allocator, chunks[i]);
        try testExpectReady(&r, &.{"bb"});
        try testing.expectEqual(3, r.getCumulativeTsn());
    }
}

test "MessageAssembler.receiveData: boundary window" {
    // a message completing a window above a waiting start keeps it
    {
        var r: MessageAssembler = .init(1 << 20);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, testData(0, 1, 0, .unordered_complete, "x"));
        _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .start_frag, "a"));
        _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .middle_frag, "b"));
        var tsn: u32 = 4;
        while (tsn < 1024) : (tsn += 1) _ = try r.receiveData(testing.allocator, testData(tsn, 1, 0, .unordered_complete, "x"));
        _ = try r.receiveData(testing.allocator, testData(1024, 1, 0, .unordered_start_frag, "y"));
        _ = try r.receiveData(testing.allocator, testData(1025, 1, 0, .unordered_end_frag, "z"));
        while (r.drainReady(testing.allocator)) |_| {}
        _ = try r.receiveData(testing.allocator, testData(3, 0, 0, .end_frag, "c"));
        try testExpectReady(&r, &.{"abc"});
        // 1025 was refused, its retransmission completes the message
        _ = try r.receiveData(testing.allocator, testData(1025, 1, 0, .unordered_end_frag, "z"));
        try testExpectReady(&r, &.{"yz"});
    }
    // a fragment a window above a received middle of a waiting message
    {
        var r: MessageAssembler = .init(1 << 20);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, testData(0, 1, 0, .unordered_complete, "x"));
        _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .start_frag, "a"));
        _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .middle_frag, "b"));
        _ = try r.receiveData(testing.allocator, testData(3, 0, 0, .middle_frag, "c"));
        _ = try r.receiveData(testing.allocator, testData(5, 0, 0, .end_frag, "e"));
        var tsn: u32 = 6;
        while (tsn < 1026) : (tsn += 1) _ = try r.receiveData(testing.allocator, testData(tsn, 1, 0, .unordered_complete, "x"));
        _ = try r.receiveData(testing.allocator, testData(1026, 1, 0, .unordered_start_frag, "y"));
        while (r.drainReady(testing.allocator)) |_| {}
        _ = try r.receiveData(testing.allocator, testData(4, 0, 0, .middle_frag, "d"));
        try testExpectReady(&r, &.{"abcde"});
    }
}
