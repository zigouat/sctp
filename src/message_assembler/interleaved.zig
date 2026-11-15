const std = @import("std");
const testing = std.testing;
const message = @import("../message.zig");

const MessageAssembler = @This();
const UserMessage = message.UserMessage;
const SackGenerator = @import("../sack_generator.zig");
const TsnBitSet = @import("../tsn_bit_set.zig");
const Helper = @import("../helper.zig");
const BitSet = std.bit_set.StaticBitSet(bitmap_bits);
const bitmap_bits = SackGenerator.bitmap_bits;

pub const AllocError = std.mem.Allocator.Error;
pub const ReceiveError = AllocError || error{ NoUserData, ProtocolViolation };
pub const ForwardTsnError = AllocError || error{ProtocolViolation};

pub const sack_delay = 200;

sack_generator: SackGenerator,
send_sack: bool,
sack_deadline: i64,
/// chunks of the messages not yet drained.
pending: IDMap(PendingChunk),
streams: std.HashMapUnmanaged(u16, StreamState, StreamIDContext, std.hash_map.default_max_load_percentage),
ready: std.Deque(Ready),
/// fragmented messages are assembled here when drained, it's as large as the largest complete message received.
assembly_buffer: []u8,
/// the single chunk message returned by the last drain, freed by the next one.
drained_chunk: []const u8,
capacity: u32,
buffered_data: u32,
advertised_wnd: u32,

const Fragment = struct { fsn: u32, tsn: u32 };

/// Keyed by tsn or message id, both are consecutive so they index the table directly.
fn IDMap(comptime V: type) type {
    return std.ArrayHashMapUnmanaged(u32, V, IDContext, false);
}

const IDContext = struct {
    pub fn hash(_: IDContext, key: u32) u32 {
        return key;
    }

    pub fn eql(_: IDContext, a: u32, b: u32, _: usize) bool {
        return a == b;
    }
};

const StreamIDContext = struct {
    pub fn hash(_: StreamIDContext, stream_id: u16) u64 {
        return stream_id;
    }

    pub fn eql(_: StreamIDContext, a: u16, b: u16) bool {
        return a == b;
    }
};

const PendingChunk = struct {
    ptr: [*]const u8,
    ppid: u32,
    /// a chunk's length is a u16
    len: u16,
    stream_id: u16,

    fn data(chunk: *const PendingChunk) []const u8 {
        return chunk.ptr[0..chunk.len];
    }
};

const Ready = union(enum) {
    fragments: struct {
        list: std.ArrayList(Fragment),
        ppid: u32,
        stream_id: u16,
        unordered: bool,
    },
    message: UserMessage, // single chunk messages
};

const PartialMessage = struct {
    /// empty for a single chunk message
    fragments: std.ArrayList(Fragment),
    end_fsn: ?u32,
    size: u32,
    /// the tsn of a single chunk message
    single_tsn: u32,

    const init: PartialMessage = .{ .fragments = .empty, .end_fsn = null, .size = 0, .single_tsn = 0 };

    fn deinit(self: *PartialMessage, allocator: std.mem.Allocator) void {
        self.fragments.deinit(allocator);
    }

    fn insert(partial_message: *PartialMessage, allocator: std.mem.Allocator, d: *const message.IData) !bool {
        const items = partial_message.fragments.items;
        if (d.isComplete()) {
            // a single chunk message keeps its tsn instead of allocating a fragment list
            if (partial_message.end_fsn != null or items.len != 0) {
                @branchHint(.cold);
                return error.ProtocolViolation;
            }
            partial_message.end_fsn = 0;
            partial_message.single_tsn = d.tsn;
            partial_message.size = @intCast(d.user_data.len);
            return true;
        }

        const fsn = if (d.flags.start_fragment) 0 else d.fsn;
        // nothing follows the end fragment, or a single chunk message
        if (partial_message.end_fsn) |end_fsn| if (fsn > end_fsn or items.len == 0) {
            @branchHint(.cold);
            return error.ProtocolViolation;
        };

        // fragments are sent in fsn order, so their tsns increase with the fsn
        if (items.len == 0 or fsn > items[items.len - 1].fsn) {
            if (items.len != 0 and !Helper.tsnGt(d.tsn, items[items.len - 1].tsn)) {
                @branchHint(.cold);
                return error.ProtocolViolation;
            }
            try partial_message.fragments.append(allocator, .{ .fsn = fsn, .tsn = d.tsn });
            if (d.flags.end_fragment) partial_message.end_fsn = fsn;
        } else {
            // the end fragment has the highest fsn
            if (d.flags.end_fragment) {
                @branchHint(.cold);
                return error.ProtocolViolation;
            }
            const end = @min(@as(usize, fsn) + 1, items.len);
            const index = std.sort.lowerBound(Fragment, items[0..end], fsn, compare);
            if (index < end and items[index].fsn == fsn) return error.ProtocolViolation;
            // the last fragment has a higher fsn, so there's always one after it
            if (!Helper.tsnGt(items[index].tsn, d.tsn) or (index != 0 and !Helper.tsnGt(d.tsn, items[index - 1].tsn))) {
                return error.ProtocolViolation;
            }
            try partial_message.fragments.insert(allocator, index, .{ .fsn = fsn, .tsn = d.tsn });
        }

        partial_message.size +|= @intCast(d.user_data.len);
        return partial_message.isComplete();
    }

    fn compare(key: u32, item: Fragment) std.math.Order {
        return std.math.order(key, item.fsn);
    }

    fn isComplete(partial_message: *const PartialMessage) bool {
        const end_fsn = partial_message.end_fsn orelse return false;
        const len = partial_message.fragmentCount();
        return len == 0 or len - 1 == end_fsn;
    }

    fn fragmentCount(partial_message: *const PartialMessage) usize {
        return partial_message.fragments.items.len;
    }

    fn pop(partial_message: *PartialMessage, size: u16) void {
        const fragment = partial_message.fragments.pop().?;
        if (fragment.fsn == partial_message.end_fsn) partial_message.end_fsn = null;
        partial_message.size -= size;
    }
};

/// Sorts held messages by their distance from the next mid, the order they are delivered in.
const MidOrder = struct {
    keys: []const u32,
    next_mid: u32,

    pub fn lessThan(ctx: MidOrder, a: usize, b: usize) bool {
        return ctx.keys[a] -% ctx.next_mid < ctx.keys[b] -% ctx.next_mid;
    }
};

const StreamState = struct {
    next_mid: u32,
    ordered_messages: IDMap(PartialMessage),
    unordered_messages: IDMap(PartialMessage),

    const init = StreamState{
        .next_mid = 0,
        .ordered_messages = .empty,
        .unordered_messages = .empty,
    };

    fn deinit(self: *StreamState, allocator: std.mem.Allocator) void {
        for (self.ordered_messages.values()) |*partial_message| partial_message.deinit(allocator);
        self.ordered_messages.deinit(allocator);
        for (self.unordered_messages.values()) |*partial_message| partial_message.deinit(allocator);
        self.unordered_messages.deinit(allocator);
    }

    fn handleChunk(stream_state: *StreamState, allocator: std.mem.Allocator, d: *const message.IData) !?PartialMessage {
        const map = if (d.flags.unordered) &stream_state.unordered_messages else &stream_state.ordered_messages;
        try map.ensureUnusedCapacity(allocator, 1);

        const partial_message = map.getOrPutAssumeCapacity(d.mid);
        if (!partial_message.found_existing) partial_message.value_ptr.* = .init;
        if (try partial_message.value_ptr.insert(allocator, d)) {
            if (d.flags.unordered) return map.fetchSwapRemove(d.mid).?.value;

            if (d.mid == stream_state.next_mid) {
                stream_state.next_mid +%= 1;
                return map.fetchSwapRemove(d.mid).?.value;
            }

            return null;
        }

        return null;
    }

    fn nextMessage(stream_state: *StreamState) ?PartialMessage {
        const partial_message = stream_state.ordered_messages.get(stream_state.next_mid) orelse return null;
        if (partial_message.isComplete()) {
            stream_state.next_mid +%= 1;
            return stream_state.ordered_messages.fetchSwapRemove(stream_state.next_mid -% 1).?.value;
        }
        return null;
    }

    fn dropChunkWithHighestTsn(stream_state: *StreamState, allocator: std.mem.Allocator, tsn: u32, size: u16) bool {
        inline for (&.{ &stream_state.unordered_messages, &stream_state.ordered_messages }) |map| {
            for (map.values(), 0..) |*partial_message, idx| {
                const count = partial_message.fragmentCount();
                // an emptied message is removed, so no fragments means an ordered single chunk message waiting its turn
                const last_tsn = if (count == 0) partial_message.single_tsn else partial_message.fragments.items[count - 1].tsn;
                if (last_tsn != tsn) continue;
                if (count <= 1) {
                    partial_message.deinit(allocator);
                    map.swapRemoveAt(idx);
                } else partial_message.pop(size);
                return true;
            }
        }

        return false;
    }

    /// Restarts the mids at 0, the held messages are already dropped.
    fn reset(stream_state: *StreamState) void {
        stream_state.next_mid = 0;
        stream_state.ordered_messages.clearRetainingCapacity();
        stream_state.unordered_messages.clearRetainingCapacity();
    }
};

pub fn init(capacity: u32) MessageAssembler {
    return .{
        .sack_generator = .init(0),
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
    for (self.pending.values()) |*chunk| allocator.free(chunk.data());
    self.pending.deinit(allocator);

    var streams_it = self.streams.valueIterator();
    while (streams_it.next()) |state| state.deinit(allocator);
    self.streams.deinit(allocator);

    while (self.ready.popFront()) |ready| switch (ready) {
        .fragments => |fragments| {
            var list = fragments.list;
            list.deinit(allocator);
        },
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
}

pub fn receiveData(self: *MessageAssembler, allocator: std.mem.Allocator, d: *const message.IData) ReceiveError!bool {
    if (d.user_data.len == 0) {
        @branchHint(.cold);
        return error.NoUserData;
    }
    self.sack_generator.checkTsn(d.tsn) catch |err| return err == error.Duplicate;

    const window_full = self.advertised_wnd == 0;
    if (window_full) {
        @branchHint(.unlikely);
        if (!self.dropChunkWithHighestTsn(allocator, d)) return true;
    }

    const stream_state = try self.getStreamState(allocator, d.stream_id);
    const held = if (d.flags.unordered) 0 else stream_state.ordered_messages.count();
    try self.ready.ensureUnusedCapacity(allocator, held + 1);

    var delivered = false;
    if (d.isComplete() and (d.flags.unordered or d.mid == stream_state.next_mid)) {
        try self.deliverSingleChunk(allocator, stream_state, d);
        delivered = true;
    } else {
        // a chunk a misbehaving peer left below the cumulative tsn, its tsn came around again
        if (self.pending.contains(d.tsn)) {
            @branchHint(.cold);
            if (!self.dropStaleMessage(allocator, d.tsn)) return false;
        }
        try self.pending.ensureUnusedCapacity(allocator, 1);

        // pending owns the data once stored, the errdefer must not reach past it
        const completed = blk: {
            const data = try allocator.dupe(u8, d.user_data);
            errdefer allocator.free(data);

            const completed = try stream_state.handleChunk(allocator, d);
            self.commitChunk(d);
            self.pending.putAssumeCapacity(d.tsn, .{ .ptr = data.ptr, .ppid = d.fsn, .len = @intCast(data.len), .stream_id = d.stream_id });
            break :blk completed;
        };
        if (completed) |partial_message| {
            try self.pushReady(allocator, partial_message, d.stream_id, d.flags.unordered);
            delivered = true;
        }
    }

    if (delivered and !d.flags.unordered and stream_state.ordered_messages.count() != 0) {
        while (stream_state.nextMessage()) |partial_message| try self.pushReady(allocator, partial_message, d.stream_id, false);
    }

    return window_full or d.flags.immediate;
}

/// Acknowledges the chunk's tsn but drops its data.
///
/// Returns `true` if a sack should be sent immediately.
pub fn acknowledge(self: *MessageAssembler, d: message.IData) bool {
    self.sack_generator.checkTsn(d.tsn) catch |err| return err == error.Duplicate;
    self.sack_generator.receiveTsn(d.tsn);
    return d.flags.immediate;
}

pub fn handleForwardTsn(self: *MessageAssembler, allocator: std.mem.Allocator, fwd_tsn: *const message.IForwardTsn) ForwardTsnError!void {
    if (Helper.tsnLte(fwd_tsn.new_cumulative_tsn, self.getCumulativeTsn())) return;

    self.sack_generator.setCumulativeTsn(fwd_tsn.new_cumulative_tsn);
    var it = fwd_tsn.iterateStreams();
    while (it.next()) |stream| {
        // an ordered stream never received from still moves its next mid past the skipped ones
        const stream_state = if (stream.unordered)
            self.streams.getPtr(stream.stream_id) orelse continue
        else
            try self.getStreamState(allocator, stream.stream_id);

        if (stream.unordered) {
            const map = &stream_state.unordered_messages;
            var i = map.count();
            while (i > 0) {
                i -= 1;
                if (Helper.tsnGt(map.keys()[i], stream.mid)) continue;
                self.dropMessage(allocator, &map.values()[i]);
                map.swapRemoveAt(i);
            }
        } else {
            const map = &stream_state.ordered_messages;
            const next_mid = stream_state.next_mid;
            const stale = Helper.tsnGt(next_mid, stream.mid);

            // the peer picks the mids, so scan the held messages instead of walking the skipped mids
            var complete: usize = 0;
            var i = map.count();
            while (i > 0) {
                i -= 1;
                const mid = map.keys()[i];
                // late chunks of messages already delivered or skipped, the peer lists their mid again once it skips their tsn
                if (!Helper.tsnGt(next_mid, mid)) {
                    if (stale or Helper.tsnGt(mid, stream.mid)) continue;
                    if (map.values()[i].isComplete()) {
                        complete += 1;
                        continue;
                    }
                }
                self.dropMessage(allocator, &map.values()[i]);
                map.swapRemoveAt(i);
            }
            if (stale) continue;

            try self.ready.ensureUnusedCapacity(allocator, map.count());
            // the complete skipped messages are the only ones left at or below the skipped mid, delivered in mid order
            switch (complete) {
                0 => {},
                1 => {
                    const index = for (map.keys(), 0..) |mid, index| {
                        if (Helper.tsnLte(mid, stream.mid)) break index;
                    } else unreachable;
                    try self.pushReady(allocator, map.values()[index], stream.stream_id, false);
                    map.swapRemoveAt(index);
                },
                else => {
                    map.sortUnstable(MidOrder{ .keys = map.keys(), .next_mid = next_mid });
                    var delivered: usize = 0;
                    while (delivered < map.count() and Helper.tsnLte(map.keys()[delivered], stream.mid)) : (delivered += 1) {
                        try self.pushReady(allocator, map.values()[delivered], stream.stream_id, false);
                    }
                    // backwards so a swap only moves kept messages into the removed slots
                    while (delivered > 0) {
                        delivered -= 1;
                        map.swapRemoveAt(delivered);
                    }
                },
            }
            stream_state.next_mid = stream.mid +% 1;
            while (stream_state.nextMessage()) |partial_message| {
                try self.pushReady(allocator, partial_message, stream.stream_id, false);
            }
        }
    }
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
        .fragments => |ready| blk: {
            var fragments = ready.list;
            defer fragments.deinit(allocator);

            var offset: usize = 0;
            for (fragments.items) |fragment| {
                const chunk = self.pending.fetchSwapRemove(fragment.tsn).?.value;
                @memcpy(self.assembly_buffer[offset..][0..chunk.len], chunk.data());
                offset += chunk.len;
                allocator.free(chunk.data());
            }

            break :blk UserMessage{
                .ppid = ready.ppid,
                .stream_id = ready.stream_id,
                .stream_seq = 0,
                .unordered = ready.unordered,
                .data = self.assembly_buffer[0..offset],
            };
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

/// Restart the incoming message ids of `streams` at 0, an empty slice resets all the streams.
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

fn resetStream(self: *MessageAssembler, allocator: std.mem.Allocator, stream_state: *StreamState) void {
    for (stream_state.ordered_messages.values()) |*partial_message| self.dropMessage(allocator, partial_message);
    for (stream_state.unordered_messages.values()) |*partial_message| self.dropMessage(allocator, partial_message);
    stream_state.reset();
}

fn getStreamState(self: *MessageAssembler, allocator: std.mem.Allocator, stream_id: u16) !*StreamState {
    const state = try self.streams.getOrPut(allocator, stream_id);
    if (!state.found_existing) state.value_ptr.* = .init;
    return state.value_ptr;
}

fn deliverSingleChunk(self: *MessageAssembler, allocator: std.mem.Allocator, stream_state: *StreamState, d: *const message.IData) std.mem.Allocator.Error!void {
    var msg = d.toUserMessage();
    msg.data = try allocator.dupe(u8, d.user_data);
    self.commitChunk(d);
    self.ready.pushBackAssumeCapacity(.{ .message = msg });
    if (!d.flags.unordered) stream_state.next_mid +%= 1;
}

fn commitChunk(self: *MessageAssembler, d: *const message.IData) void {
    self.buffered_data += @intCast(d.user_data.len);
    self.advertised_wnd -|= @intCast(d.user_data.len);
    self.sack_generator.receiveTsn(d.tsn);
}

fn pushReady(self: *MessageAssembler, allocator: std.mem.Allocator, partial_message: PartialMessage, stream_id: u16, unordered: bool) AllocError!void {
    if (partial_message.fragmentCount() == 0) {
        // a single chunk message hands over its stored data
        const chunk = self.pending.fetchSwapRemove(partial_message.single_tsn).?.value;
        self.ready.pushBackAssumeCapacity(.{ .message = .{
            .stream_id = stream_id,
            .stream_seq = 0,
            .ppid = chunk.ppid,
            .unordered = unordered,
            .data = chunk.data(),
        } });
        return;
    }
    self.ready.pushBackAssumeCapacity(.{ .fragments = .{
        .list = partial_message.fragments,
        .ppid = self.pending.get(partial_message.fragments.items[0].tsn).?.ppid,
        .stream_id = stream_id,
        .unordered = unordered,
    } });
    try self.growAssemblyBuffer(allocator, partial_message.size);
}

fn growAssemblyBuffer(self: *MessageAssembler, allocator: std.mem.Allocator, size: usize) AllocError!void {
    if (size <= self.assembly_buffer.len) return;
    self.assembly_buffer = try allocator.realloc(self.assembly_buffer, size);
}

fn updateAdvertisedWindow(self: *MessageAssembler) void {
    const avail = self.capacity -| self.buffered_data;
    const thresh = @min(message.PMDCS - message.idata_chunk_header_size, self.capacity / 2);

    self.advertised_wnd = @min(avail, self.advertised_wnd);
    if (avail -| self.advertised_wnd >= thresh) {
        self.advertised_wnd = avail;
    }
}

fn dropChunkWithHighestTsn(self: *MessageAssembler, allocator: std.mem.Allocator, d: *const message.IData) bool {
    if (!self.sack_generator.received.inGapRange(d.tsn)) return false;
    var high = self.sack_generator.received.highest_tsn_received;
    while (Helper.tsnGt(high, d.tsn)) : (high -%= 1) {
        const chunk = self.pending.get(high) orelse continue;
        const stream_state = self.streams.getPtr(chunk.stream_id).?;
        if (!stream_state.dropChunkWithHighestTsn(allocator, high, chunk.len)) continue;

        self.dropChunk(allocator, high);
        self.sack_generator.reneg(high);
        return true;
    }

    return false;
}

fn dropStaleMessage(self: *MessageAssembler, allocator: std.mem.Allocator, tsn: u32) bool {
    const stream_state = self.streams.getPtr(self.pending.get(tsn).?.stream_id).?;
    inline for (.{ &stream_state.unordered_messages, &stream_state.ordered_messages }) |map| {
        for (map.values(), 0..) |*partial_message, idx| {
            const fragments = partial_message.fragments.items;
            const holds = if (fragments.len == 0) partial_message.single_tsn == tsn else for (fragments) |fragment| {
                if (fragment.tsn == tsn) break true;
            } else false;
            if (!holds) continue;

            self.dropMessage(allocator, partial_message);
            map.swapRemoveAt(idx);
            return true;
        }
    }
    return false;
}

fn dropMessage(self: *MessageAssembler, allocator: std.mem.Allocator, partial_message: *PartialMessage) void {
    if (partial_message.fragmentCount() == 0) self.dropChunk(allocator, partial_message.single_tsn);
    for (partial_message.fragments.items) |fragment| self.dropChunk(allocator, fragment.tsn);
    partial_message.deinit(allocator);
}

fn dropChunk(self: *MessageAssembler, allocator: std.mem.Allocator, tsn: u32) void {
    const entry = self.pending.fetchSwapRemove(tsn).?;
    allocator.free(entry.value.data());
    self.buffered_data -= @intCast(entry.value.len);
    self.updateAdvertisedWindow();
}

/// The first fragment carries the ppid, 0 here, in place of the fsn.
fn testData(tsn: u32, stream_id: u16, mid: u32, fsn: u32, flags: message.Data.Flags, payload: []const u8) message.IData {
    return .{
        .flags = flags,
        .tsn = tsn,
        .stream_id = stream_id,
        .mid = mid,
        .fsn = if (flags.start_fragment) 0 else fsn,
        .user_data = payload,
    };
}

fn testExpectReady(r: *MessageAssembler, expected: []const []const u8) !void {
    for (expected) |data| {
        const msg = r.drainReady(testing.allocator).?;
        try testing.expectEqualStrings(data, msg.data);
    }
    try testing.expectEqual(null, r.drainReady(testing.allocator));
}

test "MessageAssembler.deinit: frees pending fragments, buffered reorder entries and ready messages" {
    var r: MessageAssembler = .init(4000);

    _ = try r.receiveData(testing.allocator, &testData(1, 0, 0, 0, .start_frag, "partial"));
    _ = try r.receiveData(testing.allocator, &testData(2, 0, 1, 0, .complete, "buffered"));
    _ = try r.receiveData(testing.allocator, &testData(3, 1, 0, 0, .complete, "ready"));

    r.deinit(testing.allocator);
}

test "MessageAssembler.receiveData: single-chunk unordered message is delivered immediately" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, &testData(1, 0, 0, 0, .unordered_complete, "hello"));
    try testExpectReady(&r, &.{"hello"});
}

test "MessageAssembler.receiveData: single-chunk ordered message with mid 0 is delivered immediately" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, &testData(1, 0, 0, 0, .complete, "hello"));
    try testExpectReady(&r, &.{"hello"});
}

test "MessageAssembler.receiveData: unordered message bypasses a pending ordered reorder buffer" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, &testData(1, 0, 1, 0, .complete, "ordered-second"));
    try testing.expect(r.drainReady(testing.allocator) == null);

    _ = try r.receiveData(testing.allocator, &testData(2, 0, 0, 0, .unordered_complete, "unordered"));
    // the ordered mid 1 message is still pending, unaffected by the unordered delivery
    try testExpectReady(&r, &.{"unordered"});
}

test "MessageAssembler.receiveData: a fragment repeated under another tsn is a protocol violation" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, &testData(1, 0, 0, 0, .start_frag, "a"));
    _ = try r.receiveData(testing.allocator, &testData(2, 0, 0, 1, .middle_frag, "b"));
    try testing.expectError(error.ProtocolViolation, r.receiveData(testing.allocator, &testData(3, 0, 0, 1, .middle_frag, "b")));
}

test "MessageAssembler.receiveData: zero receive window drops or reneg chunks" {
    var r = MessageAssembler.init(100);
    defer r.deinit(testing.allocator);
    const tsn = 0x12345678;
    r.setInitTsn(tsn);

    const payload: [100]u8 = @splat(0xAA);

    // mid 1 waits for mid 0
    try testing.expect(!try r.receiveData(testing.allocator, &testData(tsn +% 1, 1, 1, 0, .complete, payload[0..60])));
    try testing.expect(r.drainReady(testing.allocator) == null);
    try testing.expect(r.pending.contains(tsn +% 1));
    try testing.expectEqual(40, r.advertised_wnd);

    // unordered message is delivered right away and closes the window
    try testing.expect(!try r.receiveData(testing.allocator, &testData(tsn +% 3, 1, 0, 0, .unordered_complete, payload[0..40])));
    try testing.expectEqual(0, r.advertised_wnd);
    try testing.expectEqual(tsn +% 3, r.sack_generator.received.highest_tsn_received);

    // a tsn above the highest one is dropped
    try testing.expect(try r.receiveData(testing.allocator, &testData(tsn +% 4, 1, 0, 0, .complete, payload[0..40])));
    try testing.expectEqual(tsn +% 3, r.sack_generator.received.highest_tsn_received);

    // the higher tsns are already delivered or never received, nothing to reneg
    try testing.expect(try r.receiveData(testing.allocator, &testData(tsn +% 2, 1, 0, 0, .complete, payload[0..40])));
    try testing.expectEqual(tsn +% 3, r.sack_generator.received.highest_tsn_received);
    try testing.expectEqual(tsn -% 1, r.sack_generator.received.cumulative_tsn);

    // mid 1 held for reordering is reneged to accept mid 0
    try testing.expect(try r.receiveData(testing.allocator, &testData(tsn, 1, 0, 0, .complete, payload[0..10])));
    try testing.expect(!r.pending.contains(tsn +% 1));
    try testing.expectEqual(0, r.streams.getPtr(1).?.ordered_messages.count());
    try testing.expectEqual(tsn, r.sack_generator.received.cumulative_tsn);
    try testing.expectEqual(tsn +% 3, r.sack_generator.received.highest_tsn_received);
}

test "MessageAssembler.drainReady: re-assemble messages" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    {
        _ = try r.receiveData(testing.allocator, &testData(1, 0, 0, 0, .start_frag, "hel"));
        try testing.expect(r.drainReady(testing.allocator) == null);

        _ = try r.receiveData(testing.allocator, &testData(2, 0, 0, 1, .end_frag, "lo"));
        try testExpectReady(&r, &.{"hello"});
    }

    {
        _ = try r.receiveData(testing.allocator, &testData(4, 0, 1, 1, .end_frag, "lo"));
        try testing.expect(r.drainReady(testing.allocator) == null);

        _ = try r.receiveData(testing.allocator, &testData(3, 0, 1, 0, .start_frag, "hel"));
        try testExpectReady(&r, &.{"hello"});
    }

    {
        _ = try r.receiveData(testing.allocator, &testData(5, 1, 0, 0, .start_frag, "he"));
        _ = try r.receiveData(testing.allocator, &testData(7, 1, 0, 2, .end_frag, "lo"));
        try testing.expect(r.drainReady(testing.allocator) == null);

        _ = try r.receiveData(testing.allocator, &testData(6, 1, 0, 1, .middle_frag, "l"));
        try testExpectReady(&r, &.{"hello"});
    }

    {
        _ = try r.receiveData(testing.allocator, &testData(8, 2, 0, 0, .unordered_start_frag, "hel"));
        try testing.expect(r.drainReady(testing.allocator) == null);

        _ = try r.receiveData(testing.allocator, &testData(9, 2, 0, 1, .unordered_end_frag, "lo"));
        try testExpectReady(&r, &.{"hello"});
    }
}

test "MessageAssembler.drainReady: re-ordered messages are drained in order" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, &testData(3, 0, 2, 0, .complete, "third"));
    _ = try r.receiveData(testing.allocator, &testData(2, 0, 1, 0, .complete, "second"));
    try testing.expect(r.drainReady(testing.allocator) == null);

    _ = try r.receiveData(testing.allocator, &testData(1, 0, 0, 0, .complete, "first"));
    try testExpectReady(&r, &.{ "first", "second", "third" });
}

test "MessageAssembler: independent streams track mid separately" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, &testData(1, 1, 0, 0, .complete, "stream1"));
    _ = try r.receiveData(testing.allocator, &testData(2, 0, 0, 0, .complete, "stream0"));
    try testExpectReady(&r, &.{ "stream1", "stream0" });
}

test "MessageAssembler: mid wraparound at 0xFFFFFFFF advances to 0" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    var state: StreamState = .init;
    state.next_mid = 0xFFFFFFFF;
    try r.streams.put(testing.allocator, 0, state);

    _ = try r.receiveData(testing.allocator, &testData(1, 0, 0xFFFFFFFF, 0, .complete, "last"));
    try testExpectReady(&r, &.{"last"});
    try testing.expectEqual(0, r.streams.get(0).?.next_mid);

    _ = try r.receiveData(testing.allocator, &testData(2, 0, 0, 0, .complete, "wrapped"));
    try testExpectReady(&r, &.{"wrapped"});
}

test "MessageAssembler: advertised window" {
    var r = MessageAssembler.init(4000);
    defer r.deinit(testing.allocator);

    try testing.expectEqual(4000, r.advertised_wnd);

    const data_chunk: [1000]u8 = @splat(0xBB);
    _ = try r.receiveData(testing.allocator, &testData(0, 0, 0, 0, .unordered_start_frag, &data_chunk));
    try testing.expectEqual(3000, r.advertised_wnd);

    _ = try r.receiveData(testing.allocator, &testData(3, 0, 1, 0, .start_frag, &data_chunk));
    try testing.expectEqual(2000, r.advertised_wnd);

    _ = try r.receiveData(testing.allocator, &testData(1, 0, 0, 1, .unordered_end_frag, &data_chunk));
    try testing.expectEqual(1000, r.advertised_wnd);

    _ = r.drainReady(testing.allocator).?;
    try testing.expectEqual(3000, r.advertised_wnd);

    _ = try r.receiveData(testing.allocator, &testData(2, 0, 0, 0, .complete, data_chunk[0..100]));
    try testing.expectEqual(2900, r.advertised_wnd);

    _ = r.drainReady(testing.allocator).?;
    try testing.expectEqual(2900, r.advertised_wnd);
}

test "MessageAssembler.receiveData: allocation failure" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        pub fn run(alloc: std.mem.Allocator) !void {
            var r: MessageAssembler = .init(4000);
            defer r.deinit(alloc);

            _ = try r.receiveData(alloc, &testData(1, 0, 0, 0, .start_frag, "hel"));
            _ = try r.receiveData(alloc, &testData(2, 0, 0, 1, .end_frag, "lo"));
        }
    }.run, .{});

    try testing.checkAllAllocationFailures(testing.allocator, struct {
        pub fn run(alloc: std.mem.Allocator) !void {
            var r: MessageAssembler = .init(4000);
            defer r.deinit(alloc);

            _ = try r.receiveData(alloc, &testData(2, 0, 0, 0, .complete, "Hello Zig!"));
        }
    }.run, .{});

    try testing.checkAllAllocationFailures(testing.allocator, struct {
        pub fn run(alloc: std.mem.Allocator) !void {
            var r: MessageAssembler = .init(4000);
            defer r.deinit(alloc);

            _ = try r.receiveData(alloc, &testData(3, 0, 1, 0, .complete, "second"));
            _ = try r.receiveData(alloc, &testData(2, 0, 0, 1, .end_frag, "lo"));
            _ = try r.receiveData(alloc, &testData(1, 0, 0, 0, .start_frag, "hel"));
            _ = try r.receiveData(alloc, &testData(4, 1, 0, 0, .unordered_start_frag, "un"));
            _ = try r.receiveData(alloc, &testData(5, 1, 0, 1, .unordered_end_frag, "ordered"));
        }
    }.run, .{});
}

test "MessageAssembler.receiveData: lazy reassembly of ordered messages" {
    var r = MessageAssembler.init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, &testData(1, 0, 0, 0, .start_frag, "hel"));
    _ = try r.receiveData(testing.allocator, &testData(3, 0, 1, 0, .complete, "hola"));
    try testing.expectEqual(0, r.ready.len);

    _ = try r.receiveData(testing.allocator, &testData(4, 0, 0, 0, .unordered_complete, "Marhaba"));
    try testing.expectEqual(1, r.ready.len);

    _ = try r.receiveData(testing.allocator, &testData(6, 0, 1, 1, .unordered_end_frag, "jour"));
    try testing.expectEqual(1, r.ready.len);
    _ = try r.receiveData(testing.allocator, &testData(5, 0, 1, 0, .unordered_start_frag, "bon"));
    try testing.expectEqual(2, r.ready.len);

    // completes mid 0 and releases mid 1
    _ = try r.receiveData(testing.allocator, &testData(2, 0, 0, 1, .end_frag, "lo"));
    try testing.expectEqual(4, r.ready.len);

    _ = try r.receiveData(testing.allocator, &testData(7, 0, 2, 0, .unordered_start_frag, "sa"));
    try testing.expectEqual(4, r.ready.len);
    _ = try r.receiveData(testing.allocator, &testData(8, 0, 2, 1, .unordered_end_frag, "lut"));
    try testing.expectEqual(5, r.ready.len);

    _ = try r.receiveData(testing.allocator, &testData(10, 0, 3, 0, .complete, "ciao"));
    try testing.expectEqual(5, r.ready.len);
    _ = try r.receiveData(testing.allocator, &testData(9, 0, 2, 0, .complete, "hej"));
    try testing.expectEqual(7, r.ready.len);

    // fragments are only assembled when drained, single chunk messages leave pending once released
    try testing.expectEqual(6, r.pending.count());
    try testExpectReady(&r, &.{ "Marhaba", "bonjour", "hello", "hola", "salut", "hej", "ciao" });
    try testing.expectEqual(0, r.pending.count());
}

test "MessageAssembler.dropChunkWithHighestTsn: withdraws the last fragment of a held message but not ready ones" {
    const payload: [30]u8 = @splat('a');
    const ready: [20]u8 = @splat('b');

    // mid 1 of stream 0 waits for mid 0, the fragmented message of stream 1 is ready but not drained
    {
        var r = MessageAssembler.init(100);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, &testData(2, 0, 1, 0, .start_frag, &payload));
        _ = try r.receiveData(testing.allocator, &testData(3, 0, 1, 1, .end_frag, &payload));
        _ = try r.receiveData(testing.allocator, &testData(4, 1, 0, 0, .start_frag, &ready));
        _ = try r.receiveData(testing.allocator, &testData(5, 1, 0, 1, .end_frag, &ready));
        try testing.expectEqual(0, r.advertised_wnd);

        // tsns 4 and 5 are ready so tsn 3 is dropped
        _ = try r.receiveData(testing.allocator, &testData(1, 0, 0, 0, .complete, "c"));
        try testing.expect(!r.pending.contains(3));
        try testing.expect(!r.sack_generator.received.isAcked(3));
        try testing.expect(r.sack_generator.received.isAcked(5));
        try testing.expectEqual(1, r.streams.getPtr(0).?.ordered_messages.get(1).?.fragmentCount());
        try testExpectReady(&r, &.{ &(ready ++ ready), "c" });

        // mid 1 completes again once the dropped chunk is received again
        _ = try r.receiveData(testing.allocator, &testData(3, 0, 1, 1, .end_frag, &payload));
        try testExpectReady(&r, &.{&(payload ++ payload)});
    }

    // the highest tsn is the last fragment of a message after another one of the stream
    {
        var r = MessageAssembler.init(120);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, &testData(2, 0, 0, 1, .middle_frag, &payload));
        _ = try r.receiveData(testing.allocator, &testData(3, 0, 1, 0, .start_frag, &payload));
        _ = try r.receiveData(testing.allocator, &testData(4, 0, 0, 2, .middle_frag, &payload));
        _ = try r.receiveData(testing.allocator, &testData(5, 0, 1, 1, .middle_frag, &payload));
        try testing.expectEqual(0, r.advertised_wnd);

        _ = try r.receiveData(testing.allocator, &testData(1, 0, 0, 0, .start_frag, &payload));
        try testing.expect(!r.pending.contains(5));
        try testing.expectEqual(3, r.streams.getPtr(0).?.ordered_messages.get(0).?.fragmentCount());
        try testing.expectEqual(1, r.streams.getPtr(0).?.ordered_messages.get(1).?.fragmentCount());
    }

    // the highest tsn is an ordered single chunk message waiting its turn
    {
        var r = MessageAssembler.init(120);
        defer r.deinit(testing.allocator);
        _ = try r.receiveData(testing.allocator, &testData(2, 0, 0, 1, .middle_frag, &payload));
        _ = try r.receiveData(testing.allocator, &testData(3, 0, 0, 2, .middle_frag, &payload));
        _ = try r.receiveData(testing.allocator, &testData(4, 0, 0, 3, .middle_frag, &payload));
        _ = try r.receiveData(testing.allocator, &testData(5, 0, 1, 0, .complete, &payload));
        try testing.expectEqual(0, r.advertised_wnd);

        _ = try r.receiveData(testing.allocator, &testData(1, 0, 0, 0, .start_frag, &payload));
        try testing.expect(!r.pending.contains(5));
        try testing.expect(r.pending.contains(1));
        try testing.expect(!r.streams.getPtr(0).?.ordered_messages.contains(1));
    }
}

test "MessageAssembler.receiveData: a message missing its last fragments does not block the next one" {
    // A: start 0, middle 1, its other fragments never sent, B: start 2, end 3
    const chunks = [_]message.IData{
        testData(0, 0, 0, 0, .unordered_start_frag, "a"),
        testData(1, 0, 0, 1, .unordered_middle_frag, "a"),
        testData(2, 0, 1, 0, .unordered_start_frag, "b"),
        testData(3, 0, 1, 1, .unordered_end_frag, "b"),
    };
    const orders = [_][4]u8{
        .{ 0, 1, 2, 3 }, .{ 0, 2, 1, 3 }, .{ 0, 2, 3, 1 }, .{ 2, 3, 0, 1 }, .{ 2, 0, 1, 3 },
        .{ 1, 0, 2, 3 }, .{ 1, 2, 0, 3 }, .{ 3, 2, 1, 0 }, .{ 2, 1, 3, 0 }, .{ 3, 1, 0, 2 },
    };
    for (orders) |order| {
        var r: MessageAssembler = .init(4000);
        defer r.deinit(testing.allocator);
        for (order) |i| _ = try r.receiveData(testing.allocator, &chunks[i]);
        try testExpectReady(&r, &.{"bb"});
        try testing.expectEqual(3, r.getCumulativeTsn());
    }
}
