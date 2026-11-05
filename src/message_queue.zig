const std = @import("std");
const message = @import("message.zig");
const Helper = @import("helper.zig");

const MessageQueue = @This();
const SackHandler = @import("sack_handler.zig");

const AllocError = std.mem.Allocator.Error;

/// The smallest message payload takes 4 bytes, padding included.
const min_fragment_payload = 4;

pub const Event = union(enum) {
    buffered_amount_low_threshold: u16,
};

pub const Message = struct {
    ptr: [*]const u8,
    len: u32,
    ppid: u32,
    stream_id: u16,
    stream_seq: u16,
    flags: packed struct(u8) {
        unordered: bool,
        policy: std.meta.Tag(message.Policy),
        _: u5 = 0,
    },
    policy_value: i64,

    pub fn init(payload: []const u8, config: message.UserMessageConfig) Message {
        return .{
            .ptr = payload.ptr,
            .policy_value = switch (config.policy) {
                .reliable => 0,
                .max_retransmit => |count| count,
                .deadline => |deadline| deadline,
            },
            .len = @intCast(payload.len),
            .ppid = config.ppid,
            .stream_id = config.stream_id,
            .stream_seq = 0,
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

    fn toDataChunk(msg: *const Message, offset: u32, length: u16, tsn: u32) message.Data {
        return message.Data{
            .flags = .{
                .start_fragment = offset == 0,
                .end_fragment = offset + length == msg.len,
                .unordered = msg.flags.unordered,
            },
            .tsn = tsn,
            .ppid = msg.ppid,
            .stream_id = msg.stream_id,
            .stream_seq = msg.stream_seq,
            .user_data = msg.ptr[offset .. offset + length],
        };
    }
};

const StreamState = struct {
    stream_seq: u16,
    buffered_low_thresh: u32,
    buffered: u32,

    const init = StreamState{
        .stream_seq = 0,
        .buffered_low_thresh = 0,
        .buffered = 0,
    };

    fn incSeq(self: *StreamState) void {
        self.stream_seq +%= 1;
    }

    fn subBuffered(self: *StreamState, amount: u32) bool {
        const old_value = self.buffered;
        self.buffered -|= amount;
        return old_value > self.buffered_low_thresh and self.buffered <= self.buffered_low_thresh;
    }
};

const MessageChunk = struct {
    message_id: u32,
    offset: u32,
    length: u16,
    retransmit: packed struct(u16) {
        count: u15 = 0,
        abandoned: bool = false,
    },

    fn getSize(self: *const MessageChunk) u32 {
        return std.mem.alignForward(u32, message.data_chunk_header_size + self.length, 4);
    }
};

sack_handler: SackHandler,
streams: std.AutoHashMapUnmanaged(u16, StreamState),
messages: std.Deque(Message),
chunks: std.Deque(MessageChunk),
events: std.Deque(Event),
earliest_message_id: u32,
curr_msg: u32,
earliest_tsn: u32,
message_offset: u32,
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
        .streams = .empty,
        .messages = .empty,
        .earliest_message_id = 0,
        .chunks = .empty,
        .earliest_tsn = initial_tsn,
        .events = .empty,
        .curr_tsn = initial_tsn,
        .curr_msg = 0,
        .message_offset = 0,
        .retransmit_index = null,
        .flight_size = 0,
        .rwnd = 0,
        .advanced_peer_ack_point = initial_tsn -% 1,
        .pending_forward_tsn = false,
    };
}

pub fn deinit(self: *MessageQueue, allocator: std.mem.Allocator) void {
    while (self.messages.popFront()) |msg| msg.deinit(allocator);
    self.streams.deinit(allocator);
    self.chunks.deinit(allocator);
    self.messages.deinit(allocator);
    self.events.deinit(allocator);
}

pub fn close(self: *MessageQueue, allocator: std.mem.Allocator, initial_tsn: u32) void {
    self.streams.clearAndFree(allocator);
    while (self.chunks.popFront()) |_| {}
    while (self.messages.popFront()) |msg| msg.deinit(allocator);
    while (self.events.popFront()) |_| {}
    self.message_offset = 0;
    self.earliest_message_id = 0;
    self.curr_msg = 0;
    self.earliest_tsn = initial_tsn;
    self.curr_tsn = initial_tsn;
    self.retransmit_index = null;
    self.advanced_peer_ack_point = initial_tsn -% 1;
    self.pending_forward_tsn = false;
    self.sack_handler = .init(initial_tsn);
}

pub fn pushData(self: *MessageQueue, allocator: std.mem.Allocator, data: []const u8, config: message.UserMessageConfig) !void {
    const data_copy = try allocator.dupe(u8, data);
    errdefer allocator.free(data_copy);

    const msg: Message = .init(data_copy, config);
    return try self.pushUserMessage(allocator, &msg);
}

pub fn pushUserMessage(self: *MessageQueue, allocator: std.mem.Allocator, msg: *const Message) !void {
    try self.messages.ensureUnusedCapacity(allocator, 1);
    try self.streams.ensureUnusedCapacity(allocator, 1);

    const result = self.streams.getOrPutAssumeCapacity(msg.stream_id);
    if (!result.found_existing) result.value_ptr.* = .init;

    self.messages.pushBackAssumeCapacity(msg.*);
    result.value_ptr.buffered +|= @intCast(msg.len);
}

pub fn dropMessages(self: *MessageQueue, allocator: std.mem.Allocator, max_streams: u16) AllocError![]u16 {
    std.debug.assert(self.chunks.len == 0 and self.curr_msg == 0 and self.message_offset == 0);
    if (self.isEmpty()) return &.{};

    var dropped_streams: std.ArrayList(u16) = .empty;
    errdefer dropped_streams.deinit(allocator);

    var dropped_messages: u32 = 0;
    var it = self.messages.iterator();
    while (it.next()) |msg| if (msg.stream_id >= max_streams) {
        dropped_messages += 1;
        if (std.mem.findScalar(u16, dropped_streams.items, msg.stream_id) != null) continue;
        try dropped_streams.append(allocator, msg.stream_id);
    };

    if (dropped_streams.items.len == 0) return &.{};

    var messages_copy: std.Deque(Message) = .empty;
    errdefer messages_copy.deinit(allocator);
    try messages_copy.ensureUnusedCapacity(allocator, self.messages.len - dropped_messages);
    const streams = try dropped_streams.toOwnedSlice(allocator);

    for (streams) |stream_id| _ = self.streams.remove(stream_id);
    while (self.messages.popFront()) |msg| {
        if (msg.stream_id >= max_streams) {
            msg.deinit(allocator);
            continue;
        }
        messages_copy.pushBackAssumeCapacity(msg);
    }

    self.messages.deinit(allocator);
    self.messages = messages_copy;
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
        duplicate_data += self.getChunkSizeAt(tsn);
    }

    acked_data += self.dropAcknowledged(allocator);
    acked_data += self.updateGapAckedFlightSize(false);
    acked_data -|= old_gap_size;
    self.rwnd = sack.a_rwnd -| self.flight_size;

    // set pending forward conditionally to avoid calling moveAdvancedPeerAckPoint() in nextForwardTsn()
    const front_abandoned = if (self.chunks.front()) |chunk| chunk.retransmit.abandoned else false;
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
        while (it.next()) |state| state.stream_seq = 0;
        return;
    }

    for (streams) |stream| if (self.streams.getPtr(stream)) |state| {
        state.stream_seq = 0;
    };
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

pub fn nextChunk(
    self: *MessageQueue,
    allocator: std.mem.Allocator,
    buffer: []u8,
    can_fragment: bool,
    now: i64,
) NextChunkError!?NextChunkResult {
    const size = try self.nextRetransmit(allocator, buffer, now) orelse {
        try self.dropExpiredMessages(allocator, now);

        var zero_wnd_probe = false;
        const next_chunk_size = try self.nextChunkSize(buffer.len, can_fragment) orelse return null;
        if (self.rwnd < next_chunk_size) {
            if (self.chunks.len != 0) return null;
            zero_wnd_probe = true;
        }

        const written = try self.nextNewChunk(allocator, buffer);
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

pub fn nextFastRetransmit(self: *MessageQueue, allocator: std.mem.Allocator, buffer: []u8, now: i64) NextChunkError!?FastRetransmit {
    while (true) {
        const tsn = self.sack_handler.firstTsnWithThreeMisses(self.lowestOutstandingTsn()) orelse return null;
        try self.events.ensureUnusedCapacity(allocator, 1);
        const chunk_id = tsn -% self.earliest_tsn;
        if (self.chunks.atPtr(chunk_id).retransmit.abandoned) {
            self.sack_handler.markFastRetransmitted(tsn);
            continue;
        }

        if (self.maybeAbandonMessage(allocator, chunk_id, now)) continue;
        if (self.getChunkSizeAt(tsn) > buffer.len) return error.WriteFailed;
        self.chunks.atPtr(chunk_id).retransmit.count +|= 1;
        self.sack_handler.markFastRetransmitted(tsn);
        return .{ .tsn = tsn, .size = self.writeChunkAt(tsn, buffer) };
    }
}

pub fn nextForwardTsn(self: *MessageQueue, buffer: []u8) error{WriteFailed}!?u32 {
    if (!self.pending_forward_tsn) return null;
    self.pending_forward_tsn = false;
    self.moveAdvancedPeerAckPoint();
    if (self.advanced_peer_ack_point == self.sack_handler.acked.cumulative_tsn) return null;

    if (buffer.len < 12) {
        self.pending_forward_tsn = true;
        return error.WriteFailed;
    }
    buffer[0] = @intFromEnum(message.ChunkType.forward_tsn);
    buffer[1] = 0;

    const end_index = self.advanced_peer_ack_point -% self.earliest_tsn;
    var last_message_id = self.chunks.atPtr(0).message_id -% 1;
    var offset: u32 = 8;
    for (0..end_index + 1) |index| {
        const chunk = self.chunks.atPtr(index);
        if (chunk.message_id == last_message_id) continue;

        const msg = self.messages.atPtr(chunk.message_id -% self.earliest_message_id);
        if (msg.flags.unordered) continue;

        var slice = buffer[8..offset];
        while (slice.len >= 4) {
            if (std.mem.readInt(u16, slice[0..2], .big) != msg.stream_id) {
                slice = slice[4..];
                continue;
            }

            std.mem.writeInt(u16, slice[2..4], msg.stream_seq, .big);
            break;
        } else {
            if (buffer.len < offset + 4) {
                self.advanced_peer_ack_point = self.earliest_tsn +% @as(u32, @intCast(index)) -% 1;
                break;
            }

            std.mem.writeInt(u16, buffer[offset..][0..2], msg.stream_id, .big);
            std.mem.writeInt(u16, buffer[offset + 2 ..][0..2], msg.stream_seq, .big);
            offset += 4;
        }

        last_message_id = chunk.message_id;
    }

    std.mem.writeInt(u32, buffer[4..8], self.advanced_peer_ack_point, .big);
    std.mem.writeInt(u16, buffer[2..4], @intCast(offset), .big);
    return offset;
}

pub fn lowestOutstandingTsn(self: *const MessageQueue) u32 {
    return self.sack_handler.acked.cumulative_tsn +% 1;
}

pub fn isEmpty(self: *const MessageQueue) bool {
    return self.messages.len == 0;
}

pub fn isAllAcked(self: *const MessageQueue) bool {
    return self.sack_handler.allAcked(self.highestTsnSent());
}

pub fn highestTsnSent(self: *const MessageQueue) u32 {
    return self.curr_tsn -% 1;
}

pub fn isAcked(self: *const MessageQueue, tsn: u32) bool {
    return self.sack_handler.isAcked(tsn);
}

pub fn isCumulativelyAcked(self: *const MessageQueue, tsn: u32) bool {
    return Helper.tsnLte(tsn, self.sack_handler.acked.cumulative_tsn);
}

fn dropAcknowledged(self: *MessageQueue, allocator: std.mem.Allocator) u32 {
    if (self.chunks.len == 0) return 0;
    var data_size: u32 = 0;

    const retransmit_index = self.retransmit_index orelse self.chunks.len;
    const dropped = self.sack_handler.acked.cumulative_tsn -% self.earliest_tsn +% 1;
    for (0..dropped) |index| {
        const chunk = self.chunks.popFront().?;
        if (chunk.retransmit.abandoned) continue;
        if (index < retransmit_index) self.flight_size -= chunk.length;
        data_size +|= chunk.getSize();
    }

    const message_bound = if (self.chunks.front()) |chunk| chunk.message_id else self.earliest_message_id +% self.curr_msg;
    while (self.earliest_message_id != message_bound) {
        const msg = self.messages.popFront().?;
        msg.deinit(allocator);
        self.earliest_message_id +%= 1;
        self.curr_msg -= 1;
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
    const index = tsn -% self.earliest_tsn;
    const chunk_size: u32 = self.chunks.atPtr(index).length;
    if (inc) self.flight_size += chunk_size else self.flight_size -= chunk_size;
}

fn writeChunkAt(self: *const MessageQueue, tsn: u32, buffer: []u8) u32 {
    const index = tsn -% self.earliest_tsn;
    return self.writeMessageChunk(index, self.chunks.atPtr(index), buffer);
}

fn nextRetransmit(self: *MessageQueue, allocator: std.mem.Allocator, buffer: []u8, now: i64) NextChunkError!?u32 {
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
        if (self.chunks.atPtr(self.retransmit_index.?).retransmit.abandoned) {
            self.nextRetransmitIndex();
            continue;
        }

        if (self.maybeAbandonMessage(allocator, self.retransmit_index.?, now)) continue;

        const chunk = self.chunks.atPtr(self.retransmit_index.?);
        if (chunk.getSize() > buffer.len) return error.WriteFailed;
        chunk.retransmit.count +|= 1;
        self.flight_size += chunk.length;
        self.rwnd -|= chunk.length;
        const written = self.writeMessageChunk(self.retransmit_index.?, chunk, buffer);
        self.nextRetransmitIndex();
        return written;
    }
}

fn writeMessageChunk(self: *const MessageQueue, chunk_index: u32, msg_chunk: *const MessageChunk, buffer: []u8) u32 {
    const msg = self.messages.atPtr(msg_chunk.message_id -% self.earliest_message_id);
    const data_chunk = msg.toDataChunk(msg_chunk.offset, msg_chunk.length, self.earliest_tsn +% chunk_index);
    return @intCast(data_chunk.write(buffer));
}

fn nextChunkSize(self: *MessageQueue, buffer_size: usize, can_fragment: bool) error{WriteFailed}!?u32 {
    if (self.curr_msg >= self.messages.len or self.chunks.len >= SackHandler.bitmap_bits - 1) return null;
    const msg = self.messages.atPtr(self.curr_msg);
    const curr_payload = msg.len - self.message_offset;
    if (std.mem.alignForward(usize, message.data_chunk_header_size + curr_payload, 4) <= buffer_size) {
        return @intCast(curr_payload);
    }

    // A message is never fragmented to fit a short buffer, and a message that fits in one packet is not fragmented
    // at all, it's sent whole on a later call to nextChunk() with a big enough buffer.
    if (!can_fragment or buffer_size < message.data_chunk_header_size + min_fragment_payload or
        (self.message_offset == 0 and msg.len <= message.PMDCS - message.data_chunk_header_size))
        return error.WriteFailed;

    return @intCast(std.mem.alignBackward(usize, buffer_size - message.data_chunk_header_size, 4));
}

fn nextNewChunk(self: *MessageQueue, allocator: std.mem.Allocator, buffer: []u8) std.mem.Allocator.Error!usize {
    std.debug.assert(self.curr_msg < self.messages.len);
    try self.chunks.ensureUnusedCapacity(allocator, 1);
    try self.events.ensureUnusedCapacity(allocator, 1);

    const msg = self.messages.atPtr(self.curr_msg);
    const max_payload = std.mem.alignBackward(usize, buffer.len - message.data_chunk_header_size, 4);
    const payload_size = @min(msg.len - self.message_offset, max_payload);

    const stream_state = self.streams.getPtr(msg.stream_id).?;
    if (self.message_offset == 0 and !msg.flags.unordered) {
        msg.stream_seq = stream_state.stream_seq;
        stream_state.incSeq();
    }

    const chunk = msg.toDataChunk(self.message_offset, @intCast(payload_size), self.curr_tsn);
    self.chunks.pushBackAssumeCapacity(.{
        .message_id = self.earliest_message_id +% self.curr_msg,
        .offset = self.message_offset,
        .length = @intCast(payload_size),
        .retransmit = .{},
    });
    self.curr_tsn +%= 1;

    if (chunk.flags.end_fragment) {
        self.curr_msg += 1;
        self.message_offset = 0;
    } else {
        self.message_offset += @intCast(payload_size);
    }

    self.flight_size += @intCast(payload_size);
    self.rwnd -|= @intCast(payload_size);

    // The buffered amount only counts data not sent yet, like the W3C `bufferedAmount`.
    if (stream_state.subBuffered(@intCast(payload_size))) {
        self.events.pushBackAssumeCapacity(.{ .buffered_amount_low_threshold = chunk.stream_id });
    }

    return chunk.write(buffer);
}

fn dropExpiredMessages(self: *MessageQueue, allocator: std.mem.Allocator, now: i64) AllocError!void {
    while (self.message_offset == 0 and self.curr_msg < self.messages.len) {
        const msg = self.messages.atPtr(self.curr_msg);
        if (msg.flags.policy != .deadline or now < msg.policy_value) return;

        try self.events.ensureUnusedCapacity(allocator, 1);
        self.releaseMessage(allocator, msg, msg.len);
        if (self.curr_msg == 0) {
            _ = self.messages.popFront();
            self.earliest_message_id +%= 1;
        } else self.curr_msg += 1;
    }
}

fn getChunkSizeAt(self: *const MessageQueue, tsn: u32) u32 {
    const index = tsn -% self.earliest_tsn;
    return self.chunks.atPtr(index).getSize();
}

fn updateGapAckedFlightSize(self: *MessageQueue, comptime inc: bool) u32 {
    var it = self.sack_handler.iterateGapAcked();
    var acked_data: u32 = 0;
    const max_tsn = if (self.getCurrentRetransmitTsn()) |tsn| tsn -% 1 else self.sack_handler.acked.highest_tsn_received;
    while (it.next()) |tsn| {
        if (self.chunks.atPtr(tsn -% self.earliest_tsn).retransmit.abandoned) continue;
        if (Helper.tsnLte(tsn, max_tsn)) self.updateFlightSize(tsn, inc);
        acked_data += self.getChunkSizeAt(tsn);
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

fn maybeAbandonMessage(self: *MessageQueue, allocator: std.mem.Allocator, chunk_index: u32, now: i64) bool {
    const chunk = self.chunks.atPtr(chunk_index);
    const msg_index = chunk.message_id -% self.earliest_message_id;
    const msg = self.messages.atPtr(msg_index);

    switch (msg.flags.policy) {
        .reliable => return false,
        .max_retransmit => if (chunk.retransmit.count < msg.policy_value) return false,
        .deadline => if (now < msg.policy_value) return false,
    }

    const retransmit_index = self.retransmit_index orelse std.math.maxInt(u32);
    self.abandonChunk(chunk_index, retransmit_index);

    var start = chunk_index;
    while (start > 0 and self.chunks.atPtr(start - 1).message_id == chunk.message_id) {
        start -= 1;
        self.abandonChunk(start, retransmit_index);
    }

    var end = chunk_index + 1;
    while (end < self.chunks.len and self.chunks.atPtr(end).message_id == chunk.message_id) : (end += 1) {
        self.abandonChunk(end, retransmit_index);
    }

    // only the message being fragmented still has unsent data
    if (msg_index == self.curr_msg) {
        self.releaseMessage(allocator, msg, msg.len - self.message_offset);
        self.curr_msg += 1;
        self.message_offset = 0;
    } else self.releaseMessage(allocator, msg, 0);

    self.pending_forward_tsn = true;
    return true;
}

fn abandonChunk(self: *MessageQueue, index: u32, retransmit_index: u32) void {
    const msg_chunk = self.chunks.atPtr(index);
    msg_chunk.retransmit.abandoned = true;
    if (index < retransmit_index and !self.sack_handler.isAcked(self.earliest_tsn +% index)) {
        self.flight_size -= msg_chunk.length;
    }
}

fn moveAdvancedPeerAckPoint(self: *MessageQueue) void {
    const cumulative_tsn = self.sack_handler.acked.cumulative_tsn;
    if (Helper.tsnGt(cumulative_tsn, self.advanced_peer_ack_point)) self.advanced_peer_ack_point = cumulative_tsn;

    var index = self.advanced_peer_ack_point +% 1 -% self.earliest_tsn;
    while (index < self.chunks.len and self.chunks.atPtr(index).retransmit.abandoned) : (index += 1) {}
    self.advanced_peer_ack_point = self.earliest_tsn +% index -% 1;
}

/// Frees the message's data, `unsent` bytes are removed from the stream's buffered amount.
fn releaseMessage(self: *MessageQueue, allocator: std.mem.Allocator, msg: *Message, unsent: u32) void {
    const stream_state = self.streams.getPtr(msg.stream_id).?;
    if (stream_state.subBuffered(unsent)) {
        self.events.pushBackAssumeCapacity(.{ .buffered_amount_low_threshold = msg.stream_id });
    }
    msg.deinit(allocator);
    msg.len = 0;
}

const testing = std.testing;

fn testSendChunks(msg_queue: *MessageQueue, count: usize) !void {
    var buffer: [64]u8 = undefined;
    for (0..count) |_| {
        try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 0, .ppid = 0 });
        _ = try msg_queue.nextNewChunk(testing.allocator, &buffer);
    }
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
    var msg_queue = MessageQueue.init(0x12345678);
    defer msg_queue.deinit(testing.allocator);

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

test "MessageQueue.nextChunk: ordered messages take the stream sequence when sent" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);
    msg_queue.rwnd = 10_000;

    try msg_queue.pushData(testing.allocator, "a", .{ .stream_id = 0, .ppid = 0 });
    try msg_queue.pushData(testing.allocator, "b", .{ .stream_id = 0, .ppid = 0, .unordered = true });
    try msg_queue.pushData(testing.allocator, "c", .{ .stream_id = 0, .ppid = 0 });
    try testing.expectEqual(0, msg_queue.streams.get(0).?.stream_seq);

    var buffer: [64]u8 = undefined;
    for ([_]u16{ 0, 0, 1 }) |ssn| {
        _ = (try msg_queue.nextChunk(testing.allocator, &buffer, true, 0)).?;
        try testing.expectEqual(ssn, std.mem.readInt(u16, buffer[10..12], .big));
    }
    try testing.expectEqual(2, msg_queue.streams.get(0).?.stream_seq);
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
    try testing.expectEqual(2, msg_queue.messages.len);
    try testing.expectEqual(0, msg_queue.messages.at(0).stream_id);
    try testing.expectEqual(1, msg_queue.messages.at(1).stream_id);
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
    _ = (try msg_queue.nextRetransmit(testing.allocator, &buffer, 0)).?;
    _ = (try msg_queue.nextRetransmit(testing.allocator, &buffer, 0)).?;

    try testing.expectEqual(null, msg_queue.retransmit_index);
    try testing.expectEqual(null, msg_queue.getCurrentRetransmitTsn());
    try testing.expectEqual(8, msg_queue.flight_size);
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
        _ = (try msg_queue.nextRetransmit(testing.allocator, &buffer, 0)).?;
        try testing.expectEqual(tsn, std.mem.readInt(u32, buffer[4..8], .big));
    }
    try testing.expectEqual(null, try msg_queue.nextRetransmit(testing.allocator, &buffer, 0));
    try testing.expectEqual(null, msg_queue.retransmit_index);
}

test "MessageQueue.nextChunk: a message that fits in one packet is not fragmented" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);
    msg_queue.rwnd = 10_000;

    const data: [500]u8 = @splat('a');
    try msg_queue.pushData(testing.allocator, &data, .{ .stream_id = 0, .ppid = 0 });

    var buffer: [message.PMDCS]u8 = undefined;
    try testing.expectError(error.WriteFailed, msg_queue.nextChunk(testing.allocator, buffer[0..200], true, 0));
    const result = (try msg_queue.nextChunk(testing.allocator, &buffer, true, 0)).?;
    try testing.expectEqual(message.data_chunk_header_size + data.len, result.written);
}

test "MessageQueue.nextChunk: data chunk padding fits in a buffer not aligned to 4 bytes" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);
    msg_queue.rwnd = 10_000;

    const data: [2000]u8 = @splat('a');
    try msg_queue.pushData(testing.allocator, &data, .{ .stream_id = 0, .ppid = 0 });

    var buffer: [130]u8 = undefined;
    const result = (try msg_queue.nextChunk(testing.allocator, &buffer, true, 0)).?;
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
    while (try msg_queue.nextChunk(testing.allocator, &buffer, true, 0)) |_| {}
    // the peer accepts tsns up to its cumulative tsn + bitmap_bits - 1
    try testing.expectEqual(SackHandler.bitmap_bits - 1, msg_queue.chunks.len);
}

test "MessageQueue.setBufferedAmountLowThreshold: initializes the stream state of a new stream" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try msg_queue.setBufferedAmountLowThreshold(testing.allocator, 7, 100);
    try testing.expectEqual(StreamState{ .stream_seq = 0, .buffered_low_thresh = 100, .buffered = 0 }, msg_queue.streams.get(7).?);

    try msg_queue.pushData(testing.allocator, "Hello", .{ .stream_id = 7, .ppid = 0 });
    try testing.expectEqual(StreamState{ .stream_seq = 0, .buffered_low_thresh = 100, .buffered = 5 }, msg_queue.streams.get(7).?);
}

test "MessageQueue.handleSack: acknowledging every chunk during a retransmission clears the retransmit index" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try testSendChunks(&msg_queue, 3);
    msg_queue.retransmit_index = 0;
    msg_queue.flight_size = 0;

    var buffer: [64]u8 = undefined;
    _ = (try msg_queue.nextRetransmit(testing.allocator, &buffer, 0)).?;
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
    _ = try msg_queue.nextNewChunk(testing.allocator, &buffer);
    try testing.expectEqual(0, msg_queue.events.len);

    // 8 -> 4: crosses the threshold
    _ = try msg_queue.nextNewChunk(testing.allocator, &buffer);
    try testing.expectEqual(1, msg_queue.events.len);
    try testing.expectEqual(Event{ .buffered_amount_low_threshold = 0 }, msg_queue.events.at(0));

    // 4 -> 0: was already at or below the threshold
    _ = try msg_queue.nextNewChunk(testing.allocator, &buffer);
    try testing.expectEqual(1, msg_queue.events.len);
}

test "MessageQueue.nextNewChunk: one buffered amount low event per stream crossing its threshold" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try msg_queue.setBufferedAmountLowThreshold(testing.allocator, 1, 4);
    try msg_queue.pushData(testing.allocator, "aaaabbbbcccc", .{ .stream_id = 1, .ppid = 0 });
    try msg_queue.pushData(testing.allocator, "dddd", .{ .stream_id = 2, .ppid = 0 });

    // Room for 4 bytes of payload: stream 1 is sent as three fragments
    var buffer: [message.data_chunk_header_size + 4]u8 = undefined;
    for (0..4) |_| _ = try msg_queue.nextNewChunk(testing.allocator, &buffer);

    try testing.expectEqual(2, msg_queue.events.len);
    try testing.expectEqual(Event{ .buffered_amount_low_threshold = 1 }, msg_queue.events.at(0));
    try testing.expectEqual(Event{ .buffered_amount_low_threshold = 2 }, msg_queue.events.at(1));
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
    try testing.expectError(error.OutOfMemory, msg_queue.nextNewChunk(allocator, &buffer));
    try testing.expectEqual(0, msg_queue.chunks.len);
    try testing.expectEqual(4, msg_queue.bufferedAmount(0).?);
    try testing.expectEqual(0, msg_queue.flight_size);

    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    _ = try msg_queue.nextNewChunk(allocator, &buffer);
    try testing.expectEqual(1, msg_queue.chunks.len);
    try testing.expectEqual(0, msg_queue.bufferedAmount(0).?);
    try testing.expectEqual(1, msg_queue.events.len);
}

test "MessageQueue.nextForwardTsn: reports the highest abandoned ssn of each ordered stream" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    var buffer: [64]u8 = undefined;
    try testing.expectEqual(null, msg_queue.nextForwardTsn(&buffer));

    // tsn 1: stream 1 ssn 0, tsn 2-3: stream 1 ssn 1, tsn 4: stream 2 unordered, tsn 5: stream 3 ssn 0, tsn 6: stream 1 ssn 2
    try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 1, .ppid = 0 });
    try msg_queue.pushData(testing.allocator, "aaaaaaaa", .{ .stream_id = 1, .ppid = 0 });
    try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 2, .ppid = 0, .unordered = true });
    try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 3, .ppid = 0 });
    try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 1, .ppid = 0 });
    for (0..6) |_| _ = try msg_queue.nextNewChunk(testing.allocator, buffer[0 .. message.data_chunk_header_size + 4]);

    // nothing abandoned
    msg_queue.pending_forward_tsn = true;
    try testing.expectEqual(null, msg_queue.nextForwardTsn(&buffer));
    try testing.expect(!msg_queue.pending_forward_tsn);

    for (0..5) |index| msg_queue.chunks.atPtr(index).retransmit.abandoned = true;

    // only one entry fits, the cumulative tsn stops before stream 3's message
    msg_queue.pending_forward_tsn = true;
    try testing.expectEqual(12, msg_queue.nextForwardTsn(buffer[0..12]));
    try testing.expectEqualSlices(u8, &.{
        @intFromEnum(message.ChunkType.forward_tsn), 0, 0, 12,
        0,                                           0, 0, 4,
        0,                                           1, 0, 1,
    }, buffer[0..12]);

    // stops at tsn 6 which is not abandoned
    msg_queue.pending_forward_tsn = true;
    try testing.expectEqual(16, msg_queue.nextForwardTsn(&buffer));
    try testing.expectEqualSlices(u8, &.{
        @intFromEnum(message.ChunkType.forward_tsn), 0, 0, 16,
        0,                                           0, 0, 5,
        0,                                           1, 0, 1,
        0,                                           3, 0, 0,
    }, buffer[0..16]);
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
        for ([_]u32{ 0, 1, 3 }) |index| msg_queue.chunks.atPtr(index).retransmit.abandoned = true;
        msg_queue.moveAdvancedPeerAckPoint();
        try testing.expectEqual(2, msg_queue.advanced_peer_ack_point);
    }

    // all sent chunks abandoned
    {
        var msg_queue = MessageQueue.init(1);
        defer msg_queue.deinit(testing.allocator);
        try testSendChunks(&msg_queue, 6);
        for (0..6) |index| msg_queue.chunks.atPtr(index).retransmit.abandoned = true;
        msg_queue.moveAdvancedPeerAckPoint();
        try testing.expectEqual(6, msg_queue.advanced_peer_ack_point);
    }

    // starts from the cumulative tsn when it is ahead
    {
        var msg_queue = MessageQueue.init(1);
        defer msg_queue.deinit(testing.allocator);
        try testSendChunks(&msg_queue, 6);
        try testAckUpTo(&msg_queue, testing.allocator, 3);
        msg_queue.chunks.atPtr(0).retransmit.abandoned = true;
        msg_queue.moveAdvancedPeerAckPoint();
        try testing.expectEqual(4, msg_queue.advanced_peer_ack_point);
    }
}
