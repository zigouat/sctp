const std = @import("std");
const message = @import("message.zig");
const Helper = @import("helper.zig");

const MessageQueue = @This();
const SackHandler = @import("sack_handler.zig");
const StreamScheduler = @import("message_scheduler/round_roubin.zig");

const StreamHashMap = std.HashMapUnmanaged(u16, StreamState, StreamIdContext, std.hash_map.default_max_load_percentage);
const AllocError = std.mem.Allocator.Error;

const min_fragment_payload = 4;

const StreamIdContext = struct {
    pub fn hash(_: StreamIdContext, stream_id: u16) u64 {
        return stream_id;
    }

    pub fn eql(_: StreamIdContext, a: u16, b: u16) bool {
        return a == b;
    }
};

pub const Event = union(enum) {
    buffered_amount_low_threshold: u16,
};

pub const Message = struct {
    id: u32,
    ptr: [*]const u8,
    len: u32,
    ppid: u32,
    stream_id: u16,
    flags: packed struct(u8) {
        unordered: bool,
        policy: std.meta.Tag(message.Policy),
        /// Abandoned before any of its data was sent.
        abandoned_unsent: bool = false,
        _: u4 = 0,
    },
    policy_value: i64,

    pub fn init(payload: []const u8, config: message.UserMessageConfig) Message {
        return .{
            .id = 0,
            .ptr = payload.ptr,
            .policy_value = switch (config.policy) {
                .reliable => 0,
                .max_retransmit => |count| count,
                .deadline => |deadline| deadline,
            },
            .stream_id = config.stream_id,
            .len = @intCast(payload.len),
            .ppid = config.ppid,
            .flags = .{ .unordered = config.unordered, .policy = config.policy },
        };
    }

    pub fn deinit(msg: *const Message, allocator: std.mem.Allocator) void {
        allocator.free(msg.data());
    }

    pub fn data(msg: *const Message) []const u8 {
        return msg.ptr[0..msg.len];
    }

    pub fn policy(msg: *const Message) message.Policy {
        return switch (msg.flags.policy) {
            .reliable => .reliable,
            .max_retransmit => .{ .max_retransmit = @intCast(msg.policy_value) },
            .deadline => .{ .deadline = msg.policy_value },
        };
    }
};

const StreamState = struct {
    messages: std.Deque(Message),
    front_seq: u32,
    message_id: u32,
    unordered_message_id: u32,
    fsn: u32,
    message_offset: u32,
    curr_message_index: u32,
    buffered_low_thresh: u32,
    buffered: u32,

    const init = StreamState{
        .messages = .empty,
        .front_seq = 0,
        .message_id = 0,
        .unordered_message_id = 0,
        .fsn = 0,
        .curr_message_index = 0,
        .message_offset = 0,
        .buffered_low_thresh = 0,
        .buffered = 0,
    };

    fn deinit(stream_state: *StreamState, allocator: std.mem.Allocator) void {
        while (stream_state.messages.popFront()) |msg| msg.deinit(allocator);
        stream_state.messages.deinit(allocator);
    }

    fn pushMessage(stream_state: *StreamState, allocator: std.mem.Allocator, msg: *const Message) AllocError!void {
        try stream_state.messages.pushBack(allocator, msg.*);
        stream_state.buffered +|= msg.len;
    }

    fn dropMessage(stream_state: *StreamState, allocator: std.mem.Allocator) void {
        const msg = stream_state.messages.popFront().?;
        msg.deinit(allocator);
        stream_state.front_seq +%= 1;
        stream_state.curr_message_index -= 1;

        while (stream_state.curr_message_index > 0 and stream_state.messages.front().?.flags.abandoned_unsent) {
            _ = stream_state.messages.popFront();
            stream_state.front_seq +%= 1;
            stream_state.curr_message_index -= 1;
        }
    }

    fn subBuffered(self: *StreamState, amount: u32) bool {
        const old_value = self.buffered;
        self.buffered -|= amount;
        return old_value > self.buffered_low_thresh and self.buffered <= self.buffered_low_thresh;
    }

    fn nextChunkSize(stream_state: *StreamState, buffer_size: usize, can_fragment: bool) !?u32 {
        if (stream_state.curr_message_index >= stream_state.messages.len) return null;
        const msg = stream_state.messages.atPtr(stream_state.curr_message_index);
        const curr_payload = msg.len - stream_state.message_offset;
        if (std.mem.alignForward(usize, message.idata_chunk_header_size + curr_payload, 4) <= buffer_size) {
            return @intCast(curr_payload);
        }

        // A message is never fragmented to fit a short buffer, and a message that fits in one packet is not fragmented
        // at all, it's sent whole on a later call to nextChunk() with a big enough buffer.
        if (!can_fragment or buffer_size < message.idata_chunk_header_size + min_fragment_payload or
            (stream_state.message_offset == 0 and msg.len <= message.PMDCS - message.idata_chunk_header_size))
            return error.WriteFailed;

        return @intCast(std.mem.alignBackward(usize, buffer_size - message.idata_chunk_header_size, 4));
    }

    fn nextChunk(stream_state: *StreamState, buffer: []u8, tsn: u32, immediate: bool) struct { MessageChunk, u32 } {
        const msg = stream_state.messages.atPtr(stream_state.curr_message_index);
        const max_payload = std.mem.alignBackward(usize, buffer.len - message.idata_chunk_header_size, 4);
        const payload_size: u32 = @intCast(@min(msg.len - stream_state.message_offset, max_payload));

        const message_chunk = MessageChunk{
            .message_seq = stream_state.front_seq +% stream_state.curr_message_index,
            .offset = stream_state.message_offset,
            .fsn = stream_state.fsn,
            .length = @intCast(payload_size),
            .stream_id = msg.stream_id,
            .flags = .{
                .end_fragment = stream_state.message_offset + payload_size == msg.len,
                .abandoned = false,
            },
        };

        if (stream_state.message_offset == 0) {
            if (msg.flags.unordered) {
                msg.id = stream_state.unordered_message_id;
                stream_state.unordered_message_id +%= 1;
            } else {
                msg.id = stream_state.message_id;
                stream_state.message_id +%= 1;
            }
        }

        const written = stream_state.writeMessageChunk(&message_chunk, tsn, buffer, immediate);

        if (message_chunk.flags.end_fragment) {
            stream_state.curr_message_index += 1;
            stream_state.message_offset = 0;
            stream_state.fsn = 0;
        } else {
            stream_state.message_offset += payload_size;
            stream_state.fsn +%= 1;
        }

        return .{ message_chunk, written };
    }

    fn writeMessageChunk(stream_state: *const StreamState, msg_chunk: *const MessageChunk, tsn: u32, buffer: []u8, immediate: bool) u32 {
        const msg = stream_state.messages.atPtr(msg_chunk.message_seq -% stream_state.front_seq);
        const chunk = message.IData{
            .flags = .{
                .start_fragment = msg_chunk.offset == 0,
                .end_fragment = msg_chunk.flags.end_fragment,
                .unordered = msg.flags.unordered,
                .immediate = immediate,
            },
            .tsn = tsn,
            .stream_id = msg.stream_id,
            .mid = msg.id,
            // the first fragment carries the ppid in place of the fsn, its fsn is implicitly 0
            .fsn = if (msg_chunk.offset == 0) msg.ppid else msg_chunk.fsn,
            .user_data = msg.data()[msg_chunk.offset..][0..msg_chunk.length],
        };
        return @intCast(chunk.write(buffer));
    }

    fn releaseMessage(stream_state: *StreamState, allocator: std.mem.Allocator) bool {
        const msg = stream_state.messages.atPtr(stream_state.curr_message_index);
        const size = msg.len - stream_state.message_offset;
        const sent = stream_state.message_offset != 0;

        msg.deinit(allocator);
        msg.len = 0;

        stream_state.message_offset = 0;
        stream_state.fsn = 0;

        // A partly sent message stays until the ack of its end fragment chunk drops it.
        if (!sent and stream_state.curr_message_index == 0) {
            _ = stream_state.messages.popFront();
            stream_state.front_seq +%= 1;
        } else {
            msg.flags.abandoned_unsent = !sent;
            stream_state.curr_message_index +%= 1;
        }

        return stream_state.subBuffered(size);
    }

    fn reset(stream_state: *StreamState) void {
        stream_state.message_id = 0;
        stream_state.unordered_message_id = 0;
    }
};

const MessageChunk = struct {
    message_seq: u32,
    offset: u32,
    fsn: u32,
    length: u16,
    stream_id: u16,
    flags: packed struct(u8) {
        end_fragment: bool = false,
        abandoned: bool = false,
        _: u6 = 0,
    },
    retransmit: u16 = 0,

    fn getSize(self: *const MessageChunk) u32 {
        return std.mem.alignForward(u32, message.idata_chunk_header_size + self.length, 4);
    }
};

sack_handler: SackHandler,
stream_scheduler: StreamScheduler,
streams: StreamHashMap,
chunks: std.Deque(MessageChunk),
events: std.Deque(Event),
earliest_tsn: u32,
curr_tsn: u32,
flight_size: u32,
rwnd: u32,
retransmit_index: ?u32,
/// RFC 3758 - the tsn up to which the peer is told to move its cumulative tsn with a FORWARD TSN chunk.
advanced_peer_ack_point: u32,
pending_forward_tsn: bool,

pub fn init(initial_tsn: u32) MessageQueue {
    return .{
        .sack_handler = .init(initial_tsn),
        .stream_scheduler = .empty,
        .streams = .empty,
        .chunks = .empty,
        .events = .empty,
        .earliest_tsn = initial_tsn,
        .curr_tsn = initial_tsn,
        .retransmit_index = null,
        .flight_size = 0,
        .rwnd = 0,
        .advanced_peer_ack_point = initial_tsn -% 1,
        .pending_forward_tsn = false,
    };
}

pub fn deinit(self: *MessageQueue, allocator: std.mem.Allocator) void {
    var it = self.streams.valueIterator();
    while (it.next()) |stream_state| stream_state.deinit(allocator);
    self.streams.deinit(allocator);
    self.chunks.deinit(allocator);
    self.events.deinit(allocator);
    self.stream_scheduler.deinit(allocator);
}

pub fn close(self: *MessageQueue, allocator: std.mem.Allocator, initial_tsn: u32) void {
    var it = self.streams.valueIterator();
    while (it.next()) |stream_state| stream_state.deinit(allocator);
    self.streams.clearAndFree(allocator);
    while (self.chunks.popFront()) |_| {}
    while (self.events.popFront()) |_| {}
    self.earliest_tsn = initial_tsn;
    self.curr_tsn = initial_tsn;
    self.retransmit_index = null;
    self.advanced_peer_ack_point = initial_tsn -% 1;
    self.pending_forward_tsn = false;
    self.sack_handler = .init(initial_tsn);
    self.stream_scheduler.clear();
}

pub fn pushData(self: *MessageQueue, allocator: std.mem.Allocator, data: []const u8, config: message.UserMessageConfig) !void {
    const data_copy = try allocator.dupe(u8, data);
    errdefer allocator.free(data_copy);

    const msg: Message = .init(data_copy, config);
    return try self.pushUserMessage(allocator, &msg);
}

pub fn pushUserMessage(self: *MessageQueue, allocator: std.mem.Allocator, msg: *const Message) !void {
    try self.streams.ensureUnusedCapacity(allocator, 1);
    try self.stream_scheduler.ensureUnusedCapacity(allocator, 1);

    const result = try self.streams.getOrPut(allocator, msg.stream_id);
    if (!result.found_existing) result.value_ptr.* = .init;
    const no_messages = result.value_ptr.curr_message_index >= result.value_ptr.messages.len;
    try result.value_ptr.pushMessage(allocator, msg);
    if (no_messages) self.stream_scheduler.pushBack(allocator, msg.stream_id) catch unreachable;
}

pub fn dropMessages(self: *MessageQueue, allocator: std.mem.Allocator, max_streams: u16) AllocError![]u16 {
    std.debug.assert(self.chunks.len == 0);

    var dropped_streams: std.ArrayList(u16) = .empty;
    errdefer dropped_streams.deinit(allocator);

    var it = self.streams.iterator();
    while (it.next()) |entry| {
        if (entry.key_ptr.* >= max_streams and entry.value_ptr.messages.len != 0) {
            try dropped_streams.append(allocator, entry.key_ptr.*);
        }
    }

    const streams = try dropped_streams.toOwnedSlice(allocator);

    // a removal only marks a tombstone, the other entries don't move so the iterator stays valid
    it = self.streams.iterator();
    while (it.next()) |entry| if (entry.key_ptr.* >= max_streams) {
        entry.value_ptr.deinit(allocator);
        self.streams.removeByPtr(entry.key_ptr);
    };
    self.stream_scheduler.dropStreams(max_streams);

    return streams;
}

pub const SackResult = struct {
    acked_data: u32,
    duplicate_data: u32,
    has_fast_retransmit: bool,
    cumulative_ack_advanced: bool,
};

pub fn handleSack(self: *MessageQueue, allocator: std.mem.Allocator, sack: message.Sack, fast_recovery: bool) AllocError!?SackResult {
    self.sack_handler.validateSack(&sack, self.highestTsnSent()) catch return null;

    const prev_cumulative_tsn = self.sack_handler.acked.cumulative_tsn;
    var acked_data: u32 = 0;
    var duplicate_data: u32 = 0;

    const old_gap_size = self.updateGapAckedFlightSize(true);
    const has_fast_retransmit = self.sack_handler.handleSack(&sack, fast_recovery);

    var duplicate_it = sack.iterateDuplicateTsns();
    while (duplicate_it.next()) |tsn| {
        if (Helper.tsnLte(tsn, prev_cumulative_tsn)) continue;
        if (!self.sack_handler.isAcked(tsn)) continue;
        duplicate_data += self.getChunkLengthAt(tsn);
    }

    acked_data += self.dropAcknowledged(allocator);
    acked_data += self.updateGapAckedFlightSize(false);
    acked_data -|= old_gap_size;
    self.rwnd = sack.a_rwnd -| self.flight_size;

    // set pending forward conditionally to avoid calling moveAdvancedPeerAckPoint() in nextForwardTsn()
    const front_abandoned = if (self.chunks.front()) |chunk| chunk.flags.abandoned else false;
    if (front_abandoned or Helper.tsnGt(self.advanced_peer_ack_point, self.sack_handler.acked.cumulative_tsn)) {
        self.pending_forward_tsn = true;
    }

    return .{
        .acked_data = acked_data,
        .duplicate_data = duplicate_data,
        .has_fast_retransmit = has_fast_retransmit,
        .cumulative_ack_advanced = prev_cumulative_tsn != self.sack_handler.acked.cumulative_tsn,
    };
}

pub fn resetStreams(self: *MessageQueue, streams: []const u16) void {
    if (streams.len == 0) {
        var it = self.streams.valueIterator();
        while (it.next()) |stream_state| stream_state.reset();
        return;
    }

    for (streams) |stream| if (self.streams.getPtr(stream)) |stream_state| stream_state.reset();
}

pub fn bufferedAmount(self: *const MessageQueue, stream_id: u16) ?u32 {
    return if (self.streams.getPtr(stream_id)) |stream| stream.buffered else null;
}

pub fn setBufferedAmountLowThreshold(self: *MessageQueue, allocator: std.mem.Allocator, stream_id: u16, threshold: u32) AllocError!void {
    const entry = try self.streams.getOrPut(allocator, stream_id);
    if (!entry.found_existing) entry.value_ptr.* = .init;
    entry.value_ptr.buffered_low_thresh = threshold;
}

pub const CumulativeAck = enum { invalid, unchanged, advanced };

pub fn handleShutdownChunk(self: *MessageQueue, allocator: std.mem.Allocator, cumulative_tsn: u32) CumulativeAck {
    if (Helper.tsnGt(cumulative_tsn, self.highestTsnSent())) return .invalid;
    if (!Helper.tsnGt(cumulative_tsn, self.sack_handler.acked.cumulative_tsn)) return .unchanged;
    _ = self.updateGapAckedFlightSize(true);
    self.sack_handler.handleCumulativeTsn(cumulative_tsn);
    _ = self.dropAcknowledged(allocator);
    _ = self.updateGapAckedFlightSize(false);
    return .advanced;
}

pub fn handleTimeout(self: *MessageQueue) void {
    self.rwnd +|= self.flight_size;
    self.retransmit_index = 0;
    self.flight_size = 0;
    self.sack_handler.clearFastRetransmit();
    self.pending_forward_tsn = true;
}

pub fn popEvent(self: *MessageQueue) ?Event {
    return self.events.popFront();
}

pub const NextChunkResult = packed struct {
    written: u14,
    zero_wnd_probe: bool = false,
    retransmit: bool = false,
};

pub const NextChunkError = error{WriteFailed} || std.mem.Allocator.Error;

pub const ChunkOptions = packed struct(u8) {
    can_fragment: bool = true,
    /// sets the I bit so the peer does not delay its SACK.
    immediate: bool = false,
    _pad: u6 = 0,
};

pub fn nextChunk(
    self: *MessageQueue,
    allocator: std.mem.Allocator,
    buffer: []u8,
    options: ChunkOptions,
    now: i64,
) NextChunkError!?NextChunkResult {
    const size = try self.nextRetransmit(allocator, buffer, options.immediate, now) orelse {
        try self.dropExpiredMessages(allocator, now);

        var zero_wnd_probe = false;
        const stream_state = self.nextStreamState() orelse return null;
        const next_chunk_size = try stream_state.nextChunkSize(buffer.len, options.can_fragment) orelse return null;
        if (self.rwnd < next_chunk_size) {
            if (self.chunks.len != 0) return null;
            zero_wnd_probe = true;
        }

        const written = try self.nextNewChunk(allocator, stream_state, buffer, options.immediate);
        return NextChunkResult{
            .written = @intCast(written),
            .zero_wnd_probe = zero_wnd_probe,
        };
    };

    return NextChunkResult{ .written = @intCast(size), .retransmit = true };
}

pub const FastRetransmit = struct {
    tsn: u32,
    size: u32,
};

pub fn nextFastRetransmit(self: *MessageQueue, allocator: std.mem.Allocator, buffer: []u8, options: ChunkOptions, now: i64) NextChunkError!?FastRetransmit {
    while (true) {
        const tsn = self.sack_handler.firstTsnWithThreeMisses(self.lowestOutstandingTsn()) orelse return null;
        try self.events.ensureUnusedCapacity(allocator, 1);
        const chunk = self.chunks.atPtr(tsn -% self.earliest_tsn);
        if (chunk.flags.abandoned) {
            self.sack_handler.markFastRetransmitted(tsn);
            continue;
        }

        const stream_state = self.streams.getPtr(chunk.stream_id).?;
        if (try self.maybeAbandonMessage(allocator, stream_state, tsn -% self.earliest_tsn, now)) continue;
        if (chunk.getSize() > buffer.len) return error.WriteFailed;
        chunk.retransmit +|= 1;
        self.sack_handler.markFastRetransmitted(tsn);

        return .{
            .tsn = tsn,
            .size = stream_state.writeMessageChunk(chunk, tsn, buffer, options.immediate),
        };
    }
}

pub fn nextForwardTsn(self: *MessageQueue, buffer: []u8) error{WriteFailed}!?u32 {
    if (!self.pending_forward_tsn) return null;
    self.pending_forward_tsn = false;
    self.moveAdvancedPeerAckPoint();
    if (self.advanced_peer_ack_point == self.sack_handler.acked.cumulative_tsn) return null;

    if (buffer.len < 16) {
        self.pending_forward_tsn = true;
        return error.WriteFailed;
    }
    buffer[0] = @backingInt(message.ChunkType.iforward_tsn);
    buffer[1] = 0;

    const end_index = self.advanced_peer_ack_point -% self.earliest_tsn;
    var offset: u32 = 8;
    for (0..end_index + 1) |index| {
        const chunk = self.chunks.atPtr(index);
        const stream_state = self.streams.getPtr(chunk.stream_id).?;
        const msg = stream_state.messages.atPtr(chunk.message_seq -% stream_state.front_seq);
        var slice = buffer[8..offset];
        if (findTsnEntry(slice, chunk.stream_id, msg.flags.unordered)) |off| {
            std.mem.writeInt(u32, slice[off..][0..4], msg.id, .big);
        } else {
            if (buffer.len < offset + 8) {
                self.advanced_peer_ack_point = self.earliest_tsn +% @as(u32, @intCast(index)) -% 1;
                break;
            }

            std.mem.writeInt(u16, buffer[offset..][0..2], msg.stream_id, .big);
            std.mem.writeInt(u16, buffer[offset + 2 ..][0..2], @intFromBool(msg.flags.unordered), .big);
            std.mem.writeInt(u32, buffer[offset + 4 ..][0..4], msg.id, .big);
            offset += 8;
        }
    }

    std.mem.writeInt(u32, buffer[4..8], self.advanced_peer_ack_point, .big);
    std.mem.writeInt(u16, buffer[2..4], @intCast(offset), .big);
    return offset;
}

pub fn lowestOutstandingTsn(self: *const MessageQueue) u32 {
    return self.sack_handler.acked.cumulative_tsn +% 1;
}

// pub fn isAllAcked(self: *const MessageQueue) bool {
//     return self.sack_handler.allAcked(self.highestTsnSent());
// }

pub fn highestTsnSent(self: *const MessageQueue) u32 {
    return self.curr_tsn -% 1;
}

// pub fn isAcked(self: *const MessageQueue, tsn: u32) bool {
//     return self.sack_handler.isAcked(tsn);
// }

// pub fn isCumulativelyAcked(self: *const MessageQueue, tsn: u32) bool {
//     return Helper.tsnLte(tsn, self.sack_handler.acked.cumulative_tsn);
// }

fn dropAcknowledged(self: *MessageQueue, allocator: std.mem.Allocator) u32 {
    if (self.chunks.len == 0) return 0;
    var data_size: u32 = 0;

    const retransmit_index = self.retransmit_index orelse self.chunks.len;
    const dropped = self.sack_handler.acked.cumulative_tsn -% self.earliest_tsn +% 1;
    // Consecutive messages are often from the same stream, the lookup is skipped for them. The pointer stays valid
    // since the streams map isn't modified in the loop.
    var stream_id: ?u16 = null;
    var stream_state: *StreamState = undefined;
    for (0..dropped) |index| {
        const chunk = self.chunks.popFront().?;
        if (!chunk.flags.abandoned) {
            if (index < retransmit_index) self.flight_size -= chunk.length;
            data_size +|= chunk.length;
        }

        if (chunk.flags.end_fragment) {
            if (stream_id != chunk.stream_id) {
                stream_id = chunk.stream_id;
                stream_state = self.streams.getPtr(chunk.stream_id).?;
            }
            stream_state.dropMessage(allocator);
        }
    }

    self.earliest_tsn +%= dropped;
    self.subRetransmitIndex(dropped);
    return data_size;
}

fn subRetransmitIndex(self: *MessageQueue, count: u32) void {
    if (self.retransmit_index) |index| {
        self.retransmit_index = if (index -| count < self.chunks.len) index -| count else null;
    }
}

fn updateFlightSize(self: *MessageQueue, tsn: u32, comptime inc: bool) void {
    const chunk_length = self.getChunkLengthAt(tsn);
    if (inc) self.flight_size += chunk_length else self.flight_size -= chunk_length;
}

fn getChunkLengthAt(self: *const MessageQueue, tsn: u32) u32 {
    return self.chunks.atPtr(tsn -% self.earliest_tsn).length;
}

fn nextRetransmit(self: *MessageQueue, allocator: std.mem.Allocator, buffer: []u8, immediate: bool, now: i64) NextChunkError!?u32 {
    while (true) {
        const start_tsn = self.getCurrentRetransmitTsn() orelse return null;
        try self.events.ensureUnusedCapacity(allocator, 1);
        const highest_received = self.sack_handler.acked.highest_tsn_received;
        const tsn = if (Helper.tsnGt(start_tsn, highest_received))
            start_tsn
        else
            self.sack_handler.acked.firstUnacked(start_tsn, highest_received) orelse highest_received +% 1;
        if (Helper.tsnGt(tsn, self.highestTsnSent())) {
            self.retransmit_index = null;
            return null;
        }

        self.retransmit_index = tsn -% self.earliest_tsn;
        if (self.chunks.atPtr(self.retransmit_index.?).flags.abandoned) {
            self.nextRetransmitIndex();
            continue;
        }

        const chunk = self.chunks.atPtr(self.retransmit_index.?);
        const stream_state = self.streams.getPtr(chunk.stream_id).?;
        if (try self.maybeAbandonMessage(allocator, stream_state, self.retransmit_index.?, now)) continue;
        if (chunk.getSize() > buffer.len) return error.WriteFailed;
        chunk.retransmit +|= 1;
        self.flight_size += chunk.length;
        self.rwnd -|= chunk.length;

        self.nextRetransmitIndex();
        return stream_state.writeMessageChunk(chunk, tsn, buffer, immediate);
    }
}

/// The state of the stream the scheduler picks next, null when no new chunk can be sent.
fn nextStreamState(self: *MessageQueue) ?*StreamState {
    if (self.chunks.len >= SackHandler.bitmap_bits - 1) return null;
    const stream_id = self.stream_scheduler.peek() orelse return null;
    return self.streams.getPtr(stream_id).?;
}

/// `stream_state` is the one returned by nextStreamState().
fn nextNewChunk(
    self: *MessageQueue,
    allocator: std.mem.Allocator,
    stream_state: *StreamState,
    buffer: []u8,
    immediate: bool,
) std.mem.Allocator.Error!usize {
    try self.chunks.ensureUnusedCapacity(allocator, 1);
    try self.events.ensureUnusedCapacity(allocator, 1);
    const stream_id = self.stream_scheduler.nextStream().?;

    const chunk, const written = stream_state.nextChunk(buffer, self.curr_tsn, immediate);

    self.chunks.pushBackAssumeCapacity(chunk);
    self.curr_tsn +%= 1;

    self.flight_size += chunk.length;
    self.rwnd -|= chunk.length;

    // The buffered amount only counts data not sent yet, like the W3C `bufferedAmount`.
    if (stream_state.subBuffered(chunk.length)) {
        self.events.pushBackAssumeCapacity(.{ .buffered_amount_low_threshold = chunk.stream_id });
    }

    if (stream_state.curr_message_index < stream_state.messages.len) {
        self.stream_scheduler.pushBack(allocator, stream_id) catch unreachable;
    }

    return written;
}

fn dropExpiredMessages(self: *MessageQueue, allocator: std.mem.Allocator, now: i64) AllocError!void {
    while (true) {
        const stream_id = self.stream_scheduler.peek() orelse return;
        const stream_state = self.streams.getPtr(stream_id).?;
        if (stream_state.curr_message_index >= stream_state.messages.len) {
            _ = self.stream_scheduler.nextStream();
            continue;
        }
        const msg = stream_state.messages.atPtr(stream_state.curr_message_index);
        if (msg.flags.policy != .deadline or now < msg.policy_value) {
            @branchHint(.likely);
            return;
        }

        try self.events.ensureUnusedCapacity(allocator, 1);
        if (stream_state.message_offset != 0) {
            try self.chunks.ensureUnusedCapacity(allocator, 1);
            self.abandonChunks(stream_id, stream_state.front_seq +% stream_state.curr_message_index, @intCast(self.chunks.len -| 1));
            self.abandonCurrentMessage(allocator, stream_state, stream_id);
            self.pending_forward_tsn = true;
            continue;
        }

        if (stream_state.releaseMessage(allocator)) {
            self.events.pushBackAssumeCapacity(.{ .buffered_amount_low_threshold = stream_id });
        }
    }
}

fn updateGapAckedFlightSize(self: *MessageQueue, comptime inc: bool) u32 {
    var it = self.sack_handler.iterateGapAcked();
    var acked_data: u32 = 0;
    const max_tsn = if (self.getCurrentRetransmitTsn()) |tsn| tsn -% 1 else self.sack_handler.acked.highest_tsn_received;
    while (it.next()) |tsn| {
        if (self.chunks.atPtr(tsn -% self.earliest_tsn).flags.abandoned) continue;
        if (Helper.tsnLte(tsn, max_tsn)) self.updateFlightSize(tsn, inc);
        acked_data += self.getChunkLengthAt(tsn);
    }

    return acked_data;
}

fn getCurrentRetransmitTsn(self: *const MessageQueue) ?u32 {
    if (self.retransmit_index) |index| return self.earliest_tsn +% index;
    return null;
}

fn nextRetransmitIndex(self: *MessageQueue) void {
    if (self.retransmit_index.? + 1 >= self.chunks.len) {
        self.retransmit_index = null;
    } else {
        self.retransmit_index = self.retransmit_index.? + 1;
    }
}

fn maybeAbandonMessage(self: *MessageQueue, allocator: std.mem.Allocator, stream_state: *StreamState, chunk_id: u32, now: i64) AllocError!bool {
    const chunk = self.chunks.atPtr(chunk_id);
    const stream_id = chunk.stream_id;
    const message_seq = chunk.message_seq;

    const msg_index = message_seq -% stream_state.front_seq;
    const msg = stream_state.messages.atPtr(msg_index);
    switch (msg.flags.policy) {
        .reliable => return false,
        .max_retransmit => if (chunk.retransmit < msg.policy_value) return false,
        .deadline => if (now < msg.policy_value) return false,
    }

    try self.chunks.ensureUnusedCapacity(allocator, 1);
    try self.events.ensureUnusedCapacity(allocator, 1);

    self.abandonChunks(stream_id, message_seq, chunk_id);

    const curr_msg = msg_index == stream_state.curr_message_index;
    if (curr_msg)
        self.abandonCurrentMessage(allocator, stream_state, stream_id)
    else {
        msg.deinit(allocator);
        msg.len = 0;
    }

    self.pending_forward_tsn = true;
    return true;
}

fn abandonChunks(self: *MessageQueue, stream_id: u16, message_seq: u32, start_index: u32) void {
    if (self.chunks.len == 0) return;
    const retransmit_index: u32 = @intCast(self.retransmit_index orelse self.chunks.len);

    // A stream sends its messages one after another, so the scans stop at another message of the same stream, or at
    // the first and last fragments.
    var index = start_index + 1;
    var reached_end = false;
    while (index > 0) {
        index -= 1;
        const chunk = self.chunks.atPtr(index);
        if (chunk.stream_id != stream_id) continue;
        if (chunk.message_seq != message_seq) break;

        if (index == start_index) reached_end = chunk.flags.end_fragment;
        self.abandonChunk(index, retransmit_index);
        if (chunk.offset == 0) break;
    }

    if (reached_end) return;
    index = start_index + 1;
    while (index < self.chunks.len) : (index += 1) {
        const chunk = self.chunks.atPtr(index);
        if (chunk.stream_id != stream_id) continue;
        if (chunk.message_seq != message_seq) break;

        self.abandonChunk(index, retransmit_index);
        if (chunk.flags.end_fragment) break;
    }
}

fn abandonChunk(self: *MessageQueue, index: u32, retransmit_index: u32) void {
    const chunk = self.chunks.atPtr(index);
    chunk.flags.abandoned = true;
    // abandoned chunks are skipped when acked, they leave the flight size now unless already out of it
    if (index < retransmit_index and !self.sack_handler.isAcked(self.earliest_tsn +% index)) {
        self.flight_size -= chunk.length;
    }
}

/// Abandons the message being fragmented, the chunks capacity must be reserved.
fn abandonCurrentMessage(self: *MessageQueue, allocator: std.mem.Allocator, stream_state: *StreamState, stream_id: u16) void {
    // Consume a tsn so the forward tsn moves past the last sent fragment, even when it's already acked by the peer.
    self.chunks.pushBackAssumeCapacity(.{
        .length = 0,
        .offset = stream_state.message_offset,
        .fsn = stream_state.fsn,
        .stream_id = stream_id,
        .message_seq = stream_state.front_seq +% stream_state.curr_message_index,
        .flags = .{ .end_fragment = true, .abandoned = true },
    });
    self.curr_tsn +%= 1;

    if (stream_state.releaseMessage(allocator)) {
        self.events.pushBackAssumeCapacity(.{ .buffered_amount_low_threshold = stream_id });
    }
}

fn moveAdvancedPeerAckPoint(self: *MessageQueue) void {
    const cumulative_tsn = self.sack_handler.acked.cumulative_tsn;
    if (Helper.tsnGt(cumulative_tsn, self.advanced_peer_ack_point)) self.advanced_peer_ack_point = cumulative_tsn;

    var index = self.advanced_peer_ack_point +% 1 -% self.earliest_tsn;
    while (index < self.chunks.len and self.chunks.atPtr(index).flags.abandoned) : (index += 1) {}
    self.advanced_peer_ack_point = self.earliest_tsn +% index -% 1;
}

/// Returns the offset in `entries` of the message id of the I-FORWARD-TSN entry of a stream and ordering.
fn findTsnEntry(entries: []const u8, stream_id: u16, unordered: bool) ?usize {
    const block_size = 64;
    const threshold = 192; // This value is selected after benchmarking
    const has_simd = std.simd.suggestVectorLength(u32) != null;

    var entry_index: usize = 0;
    if (has_simd and entries.len >= threshold) {
        // the stream id and the U bit make the first 4 bytes of an entry
        const key = std.mem.nativeToBig(u32, @as(u32, stream_id) << 16 | @intFromBool(unordered));
        const lanes = block_size / 8;
        while (entry_index + block_size <= entries.len) : (entry_index += block_size) {
            // loading the bytes as a byte vector first, a direct array bitcast is lowered byte by byte
            const bytes: @Vector(block_size, u8) = entries[entry_index..][0..block_size].*;
            const words: @Vector(lanes, u64) = @bitCast(bytes);
            const keys: @Vector(lanes, u32) = if (@import("builtin").cpu.arch.endian() == .little)
                @truncate(words)
            else
                @truncate(words >> @splat(32));

            const mask: u8 = @bitCast(keys == @as(@Vector(lanes, u32), @splat(key)));
            if (mask != 0) return entry_index + @as(u32, @ctz(mask)) * 8 + 4;
        }
    }

    while (entry_index < entries.len) : (entry_index += 8) {
        const entry = entries[entry_index..][0..8];
        if (std.mem.readInt(u16, entry[0..2], .big) == stream_id and (entry[3] & 0x01 != 0) == unordered) {
            return entry_index + 4;
        }
    }

    return null;
}

const testing = std.testing;

fn testSendChunks(msg_queue: *MessageQueue, count: usize) !void {
    var buffer: [64]u8 = undefined;
    for (0..count) |_| {
        try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 0, .ppid = 0 });
        _ = try msg_queue.testNextNewChunk(testing.allocator, &buffer);
    }
}

fn testNextNewChunk(msg_queue: *MessageQueue, allocator: std.mem.Allocator, buffer: []u8) !usize {
    return msg_queue.nextNewChunk(allocator, msg_queue.nextStreamState().?, buffer, false);
}

fn testAckUpTo(msg_queue: *MessageQueue, allocator: std.mem.Allocator, cumulative_tsn: u32) !void {
    _ = try msg_queue.handleSack(allocator, message.Sack{
        .cumulative_tsn = cumulative_tsn,
        .a_rwnd = 1000,
        .blocks = &.{},
        .num_gap_ack_blocks = 0,
    }, false);
}

test "MessageQueue.pushData: increment buffered size" {
    var msg_queue = MessageQueue.init(0);
    defer msg_queue.deinit(testing.allocator);
    msg_queue.rwnd = 100_000;

    try msg_queue.pushData(testing.allocator, "Hello", .{
        .ppid = 42,
        .stream_id = 100,
        .unordered = false,
    });
    try testing.expectEqual(5, msg_queue.streams.get(100).?.buffered);

    try msg_queue.pushData(testing.allocator, "World", .{
        .ppid = 43,
        .stream_id = 100,
        .unordered = true,
    });
    try testing.expectEqual(10, msg_queue.streams.get(100).?.buffered);
}

test "MessageQueue.nextChunk: ordered and unordered messages have separate message ids" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);
    msg_queue.rwnd = 10_000;

    try msg_queue.pushData(testing.allocator, "a", .{ .stream_id = 0, .ppid = 0 });
    try msg_queue.pushData(testing.allocator, "b", .{ .stream_id = 0, .ppid = 0, .unordered = true });
    try msg_queue.pushData(testing.allocator, "c", .{ .stream_id = 0, .ppid = 0 });

    var buffer: [64]u8 = undefined;
    for ([_]u32{ 0, 0, 1 }) |mid| {
        _ = (try msg_queue.nextChunk(testing.allocator, &buffer, .{}, 0)).?;
        try testing.expectEqual(mid, std.mem.readInt(u32, buffer[12..16], .big));
    }
    try testing.expectEqual(2, msg_queue.streams.get(0).?.message_id);
    try testing.expectEqual(1, msg_queue.streams.get(0).?.unordered_message_id);
}

test "MessageQueue.dropMessages: drops the messages of the streams above the limit" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    for ([_]u16{ 0, 5, 1, 5, 7 }) |stream_id| {
        try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = stream_id, .ppid = 0 });
    }

    const streams = try msg_queue.dropMessages(testing.allocator, 2);
    defer testing.allocator.free(streams);
    try testing.expectEqualSlices(u16, &.{ 5, 7 }, streams);
    try testing.expectEqual(2, msg_queue.streams.count());
    try testing.expectEqual(1, msg_queue.streams.get(0).?.messages.len);
    try testing.expectEqual(1, msg_queue.streams.get(1).?.messages.len);
    try testing.expect(msg_queue.streams.get(5) == null);
    try testing.expect(msg_queue.streams.get(7) == null);
    try testing.expectEqual(4, msg_queue.streams.get(1).?.buffered);

    // nothing left to drop
    try testing.expectEqual(0, (try msg_queue.dropMessages(testing.allocator, 2)).len);
}

test "MessageQueue.nextRetransmit: retransmitting the last chunk clears the retransmit index" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try testSendChunks(&msg_queue, 2);
    msg_queue.retransmit_index = 0;
    msg_queue.flight_size = 0;

    var buffer: [64]u8 = undefined;
    _ = (try msg_queue.nextRetransmit(testing.allocator, &buffer, false, 0)).?;
    _ = (try msg_queue.nextRetransmit(testing.allocator, &buffer, false, 0)).?;

    try testing.expectEqual(null, msg_queue.retransmit_index);
    try testing.expectEqual(null, msg_queue.getCurrentRetransmitTsn());
    try testing.expectEqual(8, msg_queue.flight_size);
}

test "MessageQueue.nextChunk: a message that fits in one packet is not fragmented" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);
    msg_queue.rwnd = 10_000;

    const data: [500]u8 = @splat('a');
    try msg_queue.pushData(testing.allocator, &data, .{ .stream_id = 0, .ppid = 0 });

    var buffer: [message.PMDCS]u8 = undefined;
    try testing.expectError(error.WriteFailed, msg_queue.nextChunk(testing.allocator, buffer[0..200], .{}, 0));
    const result = (try msg_queue.nextChunk(testing.allocator, &buffer, .{}, 0)).?;
    try testing.expectEqual(message.idata_chunk_header_size + data.len, result.written);
}

test "MessageQueue.nextChunk: data chunk padding fits in a buffer not aligned to 4 bytes" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);
    msg_queue.rwnd = 10_000;

    const data: [2000]u8 = @splat('a');
    try msg_queue.pushData(testing.allocator, &data, .{ .stream_id = 0, .ppid = 0 });

    var buffer: [130]u8 = undefined;
    const result = (try msg_queue.nextChunk(testing.allocator, &buffer, .{}, 0)).?;
    try testing.expectEqual(0, result.written % 4);
    try testing.expect(result.written <= buffer.len);
}

test "MessageQueue.nextChunk: outstanding tsns stay within the peer's tsn window" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);
    msg_queue.rwnd = std.math.maxInt(u32);

    for (0..SackHandler.bitmap_bits + 10) |_| {
        try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 0, .ppid = 0 });
    }

    var buffer: [64]u8 = undefined;
    while (try msg_queue.nextChunk(testing.allocator, &buffer, .{}, 0)) |_| {}
    // the peer accepts tsns up to its cumulative tsn + bitmap_bits - 1
    try testing.expectEqual(SackHandler.bitmap_bits - 1, msg_queue.chunks.len);
}

test "MessageQueue.nextChunk: abandon expired new messages" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);
    msg_queue.rwnd = 10_000;

    var big_message: [2 * message.PMDCS]u8 = undefined;

    try msg_queue.pushData(testing.allocator, "Hello", .{
        .ppid = 0,
        .stream_id = 0,
        .policy = .{ .deadline = 100 },
    });
    try msg_queue.pushData(testing.allocator, "World", .{ .ppid = 0, .stream_id = 0 });
    try msg_queue.pushData(testing.allocator, "World2", .{
        .ppid = 0,
        .stream_id = 0,
        .policy = .{ .deadline = 200 },
    });
    try msg_queue.pushData(testing.allocator, &big_message, .{
        .ppid = 0,
        .stream_id = 1,
        .policy = .{ .deadline = 300 },
    });

    try msg_queue.pushData(testing.allocator, "next", .{ .ppid = 0, .stream_id = 1 });

    var buffer: [message.PMDCS]u8 = undefined;
    _ = try msg_queue.nextChunk(testing.allocator, &buffer, .{ .can_fragment = false }, 110) orelse return error.TestFailure;
    try testing.expectEqual(2, msg_queue.streams.get(0).?.messages.len); // First message dropped
    try testing.expectEqualStrings("World", msg_queue.streams.get(0).?.messages.at(0).data());
    try testing.expect(!msg_queue.pending_forward_tsn);

    // tsn 2 and 3 are the first two fragments of the big message, stream 0 is checked in between
    _ = try msg_queue.nextChunk(testing.allocator, &buffer, .{}, 201) orelse return error.TestFailure;
    _ = try msg_queue.nextChunk(testing.allocator, &buffer, .{}, 201) orelse return error.TestFailure;
    // Second message abandoned, but not deleted while the message in front of it is unacked
    const stream0 = msg_queue.streams.getPtr(0).?;
    try testing.expectEqual(2, stream0.messages.len);
    try testing.expectEqualStrings("", stream0.messages.at(1).data());
    try testing.expect(stream0.messages.at(1).flags.abandoned_unsent);
    try testing.expect(!msg_queue.pending_forward_tsn);

    // partially sent message expires: a placeholder tsn ends it and its sent fragments are abandoned
    try msg_queue.dropExpiredMessages(testing.allocator, 301);
    const stream1 = msg_queue.streams.getPtr(1).?;
    try testing.expectEqualStrings("", stream1.messages.at(0).data());
    try testing.expectEqual(1, stream1.curr_message_index);
    try testing.expectEqual(0, stream1.message_offset);
    try testing.expectEqual(4, msg_queue.chunks.len);
    try testing.expect(msg_queue.chunks.at(1).flags.abandoned);
    try testing.expect(msg_queue.chunks.at(2).flags.abandoned);
    try testing.expect(msg_queue.chunks.at(3).flags.abandoned);
    try testing.expectEqual(0, msg_queue.chunks.at(3).length);
    try testing.expectEqual(5, msg_queue.flight_size);
    try testing.expectEqual(4, msg_queue.bufferedAmount(1).?);
    try testing.expect(msg_queue.pending_forward_tsn);

    // the next message is not skipped
    _ = try msg_queue.nextChunk(testing.allocator, &buffer, .{}, 301) orelse return error.TestFailure;
    try testing.expectEqual(5, std.mem.readInt(u32, buffer[4..8], .big));
    try testing.expectEqualStrings("next", buffer[message.idata_chunk_header_size..][0..4]);

    // the abandoned message behind the acked one is dropped with it
    try testAckUpTo(&msg_queue, testing.allocator, 1);
    try testing.expectEqual(0, msg_queue.streams.get(0).?.messages.len);

    try testAckUpTo(&msg_queue, testing.allocator, 5);
    try testing.expectEqual(0, msg_queue.chunks.len);
    try testing.expectEqual(0, msg_queue.streams.get(1).?.messages.len);
    try testing.expectEqual(0, msg_queue.flight_size);
}

test "MessageQueue.nextChunk: abandon an expired message whose sent fragments are all acked" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);
    msg_queue.rwnd = 10_000;

    var big_message: [2 * message.PMDCS]u8 = undefined;
    try msg_queue.pushData(testing.allocator, &big_message, .{
        .ppid = 0,
        .stream_id = 1,
        .policy = .{ .deadline = 300 },
    });
    try msg_queue.pushData(testing.allocator, "next", .{ .ppid = 0, .stream_id = 1 });

    var buffer: [message.PMDCS]u8 = undefined;
    _ = try msg_queue.nextChunk(testing.allocator, &buffer, .{}, 0) orelse return error.TestFailure;
    try testAckUpTo(&msg_queue, testing.allocator, 1);

    // the message stays queued while the placeholder chunk references it
    try msg_queue.dropExpiredMessages(testing.allocator, 301);
    const stream_state = msg_queue.streams.getPtr(1).?;
    try testing.expectEqual(2, stream_state.messages.len);
    try testing.expectEqual(1, stream_state.curr_message_index);
    try testing.expectEqual(0, stream_state.message_offset);
    try testing.expectEqual(4, msg_queue.bufferedAmount(1).?);
    try testing.expectEqual(1, msg_queue.chunks.len);
    try testing.expect(msg_queue.pending_forward_tsn);

    try testAckUpTo(&msg_queue, testing.allocator, 2);
    try testing.expectEqual(1, stream_state.messages.len);
    try testing.expectEqual(0, stream_state.curr_message_index);
}

test "MessageQueue.nextChunk: abandon messages on retransmission" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);
    msg_queue.rwnd = 10_000;

    var big_message: [2 * message.PMDCS]u8 = undefined;

    try msg_queue.pushData(testing.allocator, "msg1", .{ .stream_id = 1, .ppid = 0, .policy = .{ .max_retransmit = 1 } });
    try msg_queue.pushData(testing.allocator, "msg2", .{ .stream_id = 1, .ppid = 0 });
    try msg_queue.pushData(testing.allocator, &big_message, .{ .stream_id = 1, .ppid = 0, .policy = .{ .deadline = 400 } });

    var buffer: [message.PMDCS]u8 = undefined;
    for (0..3) |_| {
        _ = try msg_queue.nextChunk(testing.allocator, &buffer, .{}, 0) orelse return error.TestFailure;
    }
    try testing.expectEqual(3, msg_queue.chunks.len);

    msg_queue.handleTimeout();
    // all chunks are retransmitted
    for (0..3) |_| {
        _ = try msg_queue.nextChunk(testing.allocator, &buffer, .{}, 0) orelse return error.TestFailure;
    }
    try testing.expectEqual(null, try msg_queue.nextRetransmit(testing.allocator, &buffer, false, 200));

    msg_queue.handleTimeout();
    _ = try msg_queue.nextRetransmit(testing.allocator, &buffer, false, 100) orelse return error.TestFailure;
    try testing.expect(msg_queue.chunks.at(0).flags.abandoned);

    // Fragment of the large message is retransmitted
    _ = try msg_queue.nextRetransmit(testing.allocator, &buffer, false, 200) orelse return error.TestFailure;
    try testing.expectEqual(null, try msg_queue.nextRetransmit(testing.allocator, &buffer, false, 200));

    msg_queue.handleTimeout();
    _ = try msg_queue.nextRetransmit(testing.allocator, &buffer, false, 400) orelse return error.TestFailure;
    try testing.expectEqual(null, try msg_queue.nextRetransmit(testing.allocator, &buffer, false, 400));
    try testing.expectEqual(4, msg_queue.chunks.len);
    try testing.expect(msg_queue.chunks.at(2).flags.abandoned);
    // place holder chunk
    try testing.expect(msg_queue.chunks.at(3).flags.abandoned);

    try testAckUpTo(&msg_queue, testing.allocator, msg_queue.curr_tsn -% 2);
    try testing.expectEqual(1, msg_queue.chunks.len);
    try testing.expectEqual(1, msg_queue.streams.get(1).?.messages.len);

    // the placeholder chunk drops the abandoned message
    try testAckUpTo(&msg_queue, testing.allocator, msg_queue.curr_tsn -% 1);
    try testing.expectEqual(0, msg_queue.streams.get(1).?.messages.len);
    try testing.expectEqual(0, msg_queue.flight_size);
}

test "MessageQueue.nextFastRetransmit: abandon messages" {
    var buffer: [message.PMDCS]u8 = undefined;

    // fully sent message
    {
        var msg_queue = MessageQueue.init(1);
        defer msg_queue.deinit(testing.allocator);
        msg_queue.rwnd = 10_000;

        try msg_queue.pushData(testing.allocator, "a", .{ .stream_id = 1, .ppid = 0, .policy = .{ .max_retransmit = 0 } });
        for (0..3) |_| try msg_queue.pushData(testing.allocator, "b", .{ .stream_id = 0, .ppid = 0 });
        for (0..4) |_| _ = try msg_queue.nextChunk(testing.allocator, &buffer, .{}, 0) orelse return error.TestFailure;

        // tsn 1 gets three miss indications
        for ([_]u8{ 2, 3, 4 }) |end| {
            _ = try msg_queue.handleSack(testing.allocator, message.Sack{
                .cumulative_tsn = 0,
                .a_rwnd = 10_000,
                .blocks = &.{ 0, 2, 0, end },
                .num_gap_ack_blocks = 1,
            }, false);
        }

        try testing.expectEqual(null, try msg_queue.nextFastRetransmit(testing.allocator, &buffer, .{}, 0));
        try testing.expect(msg_queue.chunks.at(0).flags.abandoned);
        try testing.expectEqual(4, msg_queue.chunks.len);
        try testing.expectEqual(0, msg_queue.flight_size);
        try testing.expect(msg_queue.pending_forward_tsn);

        try testAckUpTo(&msg_queue, testing.allocator, 4);
        try testing.expectEqual(0, msg_queue.streams.get(1).?.messages.len);
    }

    // message abandoned while being fragmented
    {
        var msg_queue = MessageQueue.init(1);
        defer msg_queue.deinit(testing.allocator);
        msg_queue.rwnd = 10_000;

        var big_message: [5 * message.PMDCS]u8 = undefined;
        try msg_queue.pushData(testing.allocator, &big_message, .{ .stream_id = 1, .ppid = 0, .policy = .{ .deadline = 100 } });
        try msg_queue.pushData(testing.allocator, "next", .{ .stream_id = 1, .ppid = 0 });
        for (0..4) |_| _ = try msg_queue.nextChunk(testing.allocator, &buffer, .{}, 0) orelse return error.TestFailure;

        for ([_]u8{ 2, 3, 4 }) |end| {
            _ = try msg_queue.handleSack(testing.allocator, message.Sack{
                .cumulative_tsn = 0,
                .a_rwnd = 10_000,
                .blocks = &.{ 0, 2, 0, end },
                .num_gap_ack_blocks = 1,
            }, false);
        }

        try testing.expectEqual(null, try msg_queue.nextFastRetransmit(testing.allocator, &buffer, .{}, 100));
        try testing.expectEqual(5, msg_queue.chunks.len);
        for (0..5) |index| try testing.expect(msg_queue.chunks.at(index).flags.abandoned);
        try testing.expectEqual(0, msg_queue.chunks.at(4).length);
        try testing.expectEqual(6, msg_queue.curr_tsn);
        try testing.expectEqual(1, msg_queue.streams.get(1).?.curr_message_index);
        try testing.expectEqual(0, msg_queue.streams.get(1).?.message_offset);
        try testing.expectEqual(0, msg_queue.flight_size);
        try testing.expectEqual(4, msg_queue.bufferedAmount(1).?);

        // the placeholder chunk drops the abandoned message
        try testAckUpTo(&msg_queue, testing.allocator, 5);
        try testing.expectEqual(1, msg_queue.streams.get(1).?.messages.len);
        try testing.expectEqual(0, msg_queue.streams.get(1).?.curr_message_index);
    }
}

test "MessageQueue.nextRetransmit: gap acked chunks are skipped" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try testSendChunks(&msg_queue, 6);
    _ = try msg_queue.handleSack(testing.allocator, message.Sack{
        .cumulative_tsn = 1,
        .a_rwnd = 1000,
        .blocks = &.{ 0, 2, 0, 3, 0, 5, 0, 5 },
        .num_gap_ack_blocks = 2,
    }, false);
    msg_queue.handleTimeout();

    var buffer: [64]u8 = undefined;
    for ([_]u32{ 2, 5 }) |tsn| {
        _ = (try msg_queue.nextRetransmit(testing.allocator, &buffer, false, 0)).?;
        try testing.expectEqual(tsn, std.mem.readInt(u32, buffer[4..8], .big));
    }
    try testing.expectEqual(null, try msg_queue.nextRetransmit(testing.allocator, &buffer, false, 0));
    try testing.expectEqual(null, msg_queue.retransmit_index);
}

test "MessageQueue.setBufferedAmountLowThreshold: initializes the stream state of a new stream" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try msg_queue.setBufferedAmountLowThreshold(testing.allocator, 7, 100);
    try testing.expectEqual(100, msg_queue.streams.get(7).?.buffered_low_thresh);
    try testing.expectEqual(0, msg_queue.streams.get(7).?.buffered);
    try testing.expectEqual(0, msg_queue.streams.get(7).?.messages.len);

    try msg_queue.pushData(testing.allocator, "Hello", .{ .stream_id = 7, .ppid = 0 });
    try testing.expectEqual(100, msg_queue.streams.get(7).?.buffered_low_thresh);
    try testing.expectEqual(5, msg_queue.streams.get(7).?.buffered);
}

test "MessageQueue.handleSack: acknowledging every chunk during a retransmission clears the retransmit index" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try testSendChunks(&msg_queue, 3);
    msg_queue.retransmit_index = 0;
    msg_queue.flight_size = 0;

    var buffer: [64]u8 = undefined;
    _ = (try msg_queue.nextRetransmit(testing.allocator, &buffer, false, 0)).?;
    try testing.expectEqual(4, msg_queue.flight_size);

    _ = try msg_queue.handleSack(testing.allocator, message.Sack{
        .cumulative_tsn = 3,
        .a_rwnd = 0,
        .blocks = &.{},
        .num_gap_ack_blocks = 0,
    }, false);

    try testing.expectEqual(null, msg_queue.retransmit_index);
    try testing.expectEqual(null, msg_queue.getCurrentRetransmitTsn());
    try testing.expectEqual(0, msg_queue.flight_size);
}

test "MessageQueue.nextNewChunk: buffered amount low event is emitted only when crossing the threshold" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try msg_queue.setBufferedAmountLowThreshold(testing.allocator, 0, 4);
    for (0..3) |_| try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 0, .ppid = 0 });

    var buffer: [64]u8 = undefined;
    // 12 -> 8: still above the threshold
    _ = try msg_queue.testNextNewChunk(testing.allocator, &buffer);
    try testing.expectEqual(0, msg_queue.events.len);

    // 8 -> 4: crosses the threshold
    _ = try msg_queue.testNextNewChunk(testing.allocator, &buffer);
    try testing.expectEqual(1, msg_queue.events.len);
    try testing.expectEqual(Event{ .buffered_amount_low_threshold = 0 }, msg_queue.events.at(0));

    // 4 -> 0: was already at or below the threshold
    _ = try msg_queue.testNextNewChunk(testing.allocator, &buffer);
    try testing.expectEqual(1, msg_queue.events.len);
}

test "MessageQueue.nextNewChunk: one buffered amount low event per stream crossing its threshold" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try msg_queue.setBufferedAmountLowThreshold(testing.allocator, 1, 4);
    try msg_queue.pushData(testing.allocator, "aaaabbbbcccc", .{ .stream_id = 1, .ppid = 0 });
    try msg_queue.pushData(testing.allocator, "dddd", .{ .stream_id = 2, .ppid = 0 });

    // Room for 4 bytes of payload: stream 1 is sent as three fragments, interleaved with stream 2:
    // aaaa (12 -> 8), dddd (4 -> 0), bbbb (8 -> 4), cccc (4 -> 0)
    var buffer: [message.idata_chunk_header_size + 4]u8 = undefined;
    for (0..4) |_| _ = try msg_queue.testNextNewChunk(testing.allocator, &buffer);

    try testing.expectEqual(2, msg_queue.events.len);
    try testing.expectEqual(Event{ .buffered_amount_low_threshold = 2 }, msg_queue.events.at(0));
    try testing.expectEqual(Event{ .buffered_amount_low_threshold = 1 }, msg_queue.events.at(1));
}

test "MessageQueue.bufferedAmount: sent data is not buffered while waiting for its acknowledgement" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try testSendChunks(&msg_queue, 3);
    try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 0, .ppid = 0 });
    try testing.expectEqual(4, msg_queue.bufferedAmount(0).?);

    // tsn 1 is lost, the gap acked tsns don't change the buffered amount
    _ = try msg_queue.handleSack(testing.allocator, message.Sack{
        .cumulative_tsn = 0,
        .a_rwnd = 1000,
        .blocks = &.{ 0, 2, 0, 3 },
        .num_gap_ack_blocks = 1,
    }, false);
    try testing.expectEqual(4, msg_queue.bufferedAmount(0).?);

    try testAckUpTo(&msg_queue, testing.allocator, 3);
    try testing.expectEqual(4, msg_queue.bufferedAmount(0).?);
}

test "MessageQueue.nextNewChunk: running out of memory for events sends nothing" {
    var failing = testing.FailingAllocator.init(testing.allocator, .{});
    const allocator = failing.allocator();

    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(allocator);

    try msg_queue.setBufferedAmountLowThreshold(allocator, 0, 2);
    try msg_queue.pushData(allocator, "aaaa", .{ .stream_id = 0, .ppid = 0 });
    try msg_queue.chunks.ensureTotalCapacityPrecise(allocator, 1);
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;

    var buffer: [64]u8 = undefined;
    try testing.expectError(error.OutOfMemory, msg_queue.testNextNewChunk(allocator, &buffer));
    try testing.expectEqual(0, msg_queue.chunks.len);
    try testing.expectEqual(4, msg_queue.bufferedAmount(0).?);
    try testing.expectEqual(0, msg_queue.flight_size);

    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    _ = try msg_queue.testNextNewChunk(allocator, &buffer);
    try testing.expectEqual(1, msg_queue.chunks.len);
    try testing.expectEqual(0, msg_queue.bufferedAmount(0).?);
    try testing.expectEqual(1, msg_queue.events.len);
}

test "MessageQueue.nextForwardTsn: reports the highest abandoned message id of each stream and ordering" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    var buffer: [64]u8 = undefined;
    try testing.expectEqual(null, msg_queue.nextForwardTsn(&buffer));

    // tsn 1: stream 1 mid 0, tsn 2: stream 2 unordered mid 0, tsn 3: stream 3 mid 0, tsn 4-5: stream 1 mid 1,
    // tsn 6: stream 1 mid 2
    try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 1, .ppid = 0 });
    try msg_queue.pushData(testing.allocator, "aaaaaaaa", .{ .stream_id = 1, .ppid = 0 });
    try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 2, .ppid = 0, .unordered = true });
    try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 3, .ppid = 0 });
    try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 1, .ppid = 0 });
    for (0..6) |_| _ = try msg_queue.testNextNewChunk(testing.allocator, buffer[0 .. message.idata_chunk_header_size + 4]);

    // nothing abandoned
    msg_queue.pending_forward_tsn = true;
    try testing.expectEqual(null, msg_queue.nextForwardTsn(&buffer));
    try testing.expect(!msg_queue.pending_forward_tsn);

    for (0..5) |index| msg_queue.chunks.atPtr(index).flags.abandoned = true;

    // only one entry fits, the cumulative tsn stops before stream 2's message
    msg_queue.pending_forward_tsn = true;
    try testing.expectEqual(16, msg_queue.nextForwardTsn(buffer[0..16]));
    try testing.expectEqualSlices(u8, &.{
        @backingInt(message.ChunkType.iforward_tsn), 0, 0, 16,
        0,                                           0, 0, 1,
        0,                                           1, 0, 0,
        0,                                           0, 0, 0,
    }, buffer[0..16]);

    // stops at tsn 6 which is not abandoned
    msg_queue.pending_forward_tsn = true;
    try testing.expectEqual(32, msg_queue.nextForwardTsn(&buffer));
    try testing.expectEqualSlices(u8, &.{
        @backingInt(message.ChunkType.iforward_tsn), 0, 0, 32,
        0,                                           0, 0, 5,
        0,                                           1, 0, 0,
        0,                                           0, 0, 1,
        0,                                           2, 0, 1,
        0,                                           0, 0, 0,
        0,                                           3, 0, 0,
        0,                                           0, 0, 0,
    }, buffer[0..32]);
}

test "MessageQueue.moveAdvancedPeerAckPoint: advances over the abandoned chunks above the cumulative tsn" {
    // nothing sent
    {
        var msg_queue = MessageQueue.init(1);
        defer msg_queue.deinit(testing.allocator);
        msg_queue.moveAdvancedPeerAckPoint();
        try testing.expectEqual(0, msg_queue.advanced_peer_ack_point);
    }

    // stops before the first chunk that is not abandoned
    {
        var msg_queue = MessageQueue.init(1);
        defer msg_queue.deinit(testing.allocator);
        try testSendChunks(&msg_queue, 6);
        for ([_]u32{ 0, 1, 3 }) |index| msg_queue.chunks.atPtr(index).flags.abandoned = true;
        msg_queue.moveAdvancedPeerAckPoint();
        try testing.expectEqual(2, msg_queue.advanced_peer_ack_point);
    }

    // all sent chunks abandoned
    {
        var msg_queue = MessageQueue.init(1);
        defer msg_queue.deinit(testing.allocator);
        try testSendChunks(&msg_queue, 6);
        for (0..6) |index| msg_queue.chunks.atPtr(index).flags.abandoned = true;
        msg_queue.moveAdvancedPeerAckPoint();
        try testing.expectEqual(6, msg_queue.advanced_peer_ack_point);
    }

    // starts from the cumulative tsn when it is ahead
    {
        var msg_queue = MessageQueue.init(1);
        defer msg_queue.deinit(testing.allocator);
        try testSendChunks(&msg_queue, 6);
        try testAckUpTo(&msg_queue, testing.allocator, 3);
        msg_queue.chunks.atPtr(0).flags.abandoned = true;
        msg_queue.moveAdvancedPeerAckPoint();
        try testing.expectEqual(4, msg_queue.advanced_peer_ack_point);
    }
}
