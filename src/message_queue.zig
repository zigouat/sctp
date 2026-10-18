const std = @import("std");
const message = @import("message.zig");
const Helper = @import("helper.zig");

const MessageQueue = @This();
const SackHandler = @import("sack_handler.zig");

pub const Event = union(enum) {
    buffered_amount_low_threshold: u16,
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

sack_handler: SackHandler,
streams: std.AutoHashMapUnmanaged(u16, StreamState),
messages: std.Deque(message.UserMessage),
chunks: std.Deque(message.Data),
events: std.Deque(Event),
flight_size: u32,
rwnd: u32,
curr_tsn: u32,
curr_msg: u32,
message_offset: u32,
retransmit_index: ?u32,

pub fn init(initial_tsn: u32) MessageQueue {
    return .{
        .sack_handler = .init(initial_tsn),
        .streams = .empty,
        .messages = .empty,
        .chunks = .empty,
        .events = .empty,
        .curr_tsn = initial_tsn,
        .curr_msg = 0,
        .message_offset = 0,
        .retransmit_index = null,
        .flight_size = 0,
        .rwnd = 0,
    };
}

pub fn deinit(self: *MessageQueue, allocator: std.mem.Allocator) void {
    while (self.messages.popFront()) |msg| allocator.free(msg.data);
    self.streams.deinit(allocator);
    self.chunks.deinit(allocator);
    self.messages.deinit(allocator);
    self.events.deinit(allocator);
}

pub fn close(self: *MessageQueue, allocator: std.mem.Allocator, initial_tsn: u32) void {
    self.streams.clearAndFree(allocator);
    while (self.chunks.popFront()) |_| {}
    while (self.messages.popFront()) |msg| allocator.free(msg.data);
    while (self.events.popFront()) |_| {}
    self.curr_tsn = initial_tsn;
    self.curr_msg = 0;
    self.message_offset = 0;
    self.retransmit_index = null;
    self.sack_handler = .init(initial_tsn);
}

pub fn pushData(self: *MessageQueue, allocator: std.mem.Allocator, data: []const u8, config: message.UserMessageConfig) !void {
    try self.messages.ensureUnusedCapacity(allocator, 1);
    try self.streams.ensureUnusedCapacity(allocator, 1);
    const data_copy = try allocator.dupe(u8, data);

    const result = self.streams.getOrPutAssumeCapacity(config.stream_id);
    if (!result.found_existing) result.value_ptr.* = .init;

    self.messages.pushBackAssumeCapacity(.{
        .data = data_copy,
        .ppid = config.ppid,
        .stream_id = config.stream_id,
        .stream_seq = if (config.unordered) 0 else result.value_ptr.stream_seq,
        .unordered = config.unordered,
    });
    if (!config.unordered) result.value_ptr.incSeq();
    result.value_ptr.buffered +|= @intCast(data.len);
}

pub const SackResult = struct {
    acked_data: u32,
    duplicate_data: u32,
    has_fast_retransmit: bool,
    cumulative_ack_advanced: bool,
};

pub fn handleSack(self: *MessageQueue, allocator: std.mem.Allocator, sack: message.Sack, fast_recovery: bool) std.mem.Allocator.Error!?SackResult {
    const prev_flight_size = self.flight_size;
    const prev_cumulative_tsn = self.sack_handler.acked.cumulative_tsn;
    var acked_data: u32 = 0;
    var duplicate_data: u32 = 0;

    const old_gap_size = self.updateGapAckedFlightSize(true);
    const has_fast_retransmit = self.sack_handler.handleSack(&sack, self.highestTsnSent(), fast_recovery) catch {
        self.flight_size = prev_flight_size;
        return null;
    };

    var duplicate_it = sack.iterateDuplicateTsns();
    while (duplicate_it.next()) |tsn| {
        if (Helper.tsnLte(tsn, prev_cumulative_tsn)) continue;
        if (!self.sack_handler.isAcked(tsn)) continue;
        duplicate_data += self.getChunkSizeAt(tsn);
    }

    errdefer _ = self.updateGapAckedFlightSize(false);
    acked_data += try self.dropAcknowledged(allocator);
    acked_data += self.updateGapAckedFlightSize(false);
    acked_data -|= old_gap_size;
    self.rwnd = sack.a_rwnd -| self.flight_size;

    return .{
        .acked_data = acked_data,
        .duplicate_data = duplicate_data,
        .has_fast_retransmit = has_fast_retransmit,
        .cumulative_ack_advanced = prev_cumulative_tsn != self.sack_handler.acked.cumulative_tsn,
    };
}

pub fn bufferedAmount(self: *const MessageQueue, stream_id: u16) ?u32 {
    return if (self.streams.getPtr(stream_id)) |stream| stream.buffered else null;
}

pub fn setBufferedAmountLowThreshold(self: *MessageQueue, allocator: std.mem.Allocator, stream_id: u16, threshold: u32) !void {
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
    _ = self.dropAcknowledged(allocator) catch {};
    _ = self.updateGapAckedFlightSize(false);
    return .advanced;
}

pub fn handleTimeout(self: *MessageQueue) void {
    self.rwnd +|= self.flight_size;
    self.retransmit_index = 0;
    self.flight_size = 0;
    self.sack_handler.clearFastRetransmit();
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

pub fn nextChunk(self: *MessageQueue, allocator: std.mem.Allocator, buffer: []u8) NextChunkError!?NextChunkResult {
    const size = try self.nextRetransmit(buffer) orelse {
        var zero_wnd_probe = false;
        const next_chunk_size = self.nextChunkSize(buffer.len) orelse return null;
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

pub fn nextFastRetransmit(self: *MessageQueue, buffer: []u8) error{WriteFailed}!?FastRetransmit {
    if (self.sack_handler.firstTsnWithThreeMisses(self.lowestOutstandingTsn())) |tsn| {
        if (self.getChunkSizeAt(tsn) > buffer.len) return error.WriteFailed;
        self.sack_handler.markFastRetransmitted(tsn);
        return .{ .tsn = tsn, .size = self.writeChunkAt(tsn, buffer) };
    }

    return null;
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

fn dropAcknowledged(self: *MessageQueue, allocator: std.mem.Allocator) !u32 {
    if (self.chunks.len == 0) return 0;
    var data_size: u32 = 0;

    // A message's chunks share one stream and only shrink its buffered amount,
    // so each message crosses the threshold at most once: one event slot per message.
    try self.events.ensureUnusedCapacity(allocator, 1);

    const retransmit_index = self.retransmit_index orelse self.chunks.len;
    const dropped = self.sack_handler.acked.cumulative_tsn -% self.chunks.frontPtr().?.tsn +% 1;
    for (0..dropped) |index| {
        const chunk = self.chunks.popFront().?;

        const stream_state = self.streams.getPtr(chunk.stream_id).?;
        if (stream_state.subBuffered(@intCast(chunk.user_data.len))) {
            self.events.pushBackAssumeCapacity(.{ .buffered_amount_low_threshold = chunk.stream_id });
        }

        if (index < retransmit_index) self.flight_size -= @intCast(chunk.user_data.len);
        data_size +|= chunk.getSize();

        if (chunk.flags.end_fragment) {
            const msg = self.messages.popFront().?;
            allocator.free(msg.data);
            self.curr_msg -= 1;

            if (index + 1 < dropped) {
                self.events.ensureUnusedCapacity(allocator, 1) catch |err| {
                    self.subRetransmitIndex(@intCast(index + 1));
                    return err;
                };
            }
        }
    }

    self.subRetransmitIndex(dropped);
    return data_size;
}

fn subRetransmitIndex(self: *MessageQueue, count: u32) void {
    if (self.retransmit_index) |index| {
        self.retransmit_index = if (index -| count < self.chunks.len) index -| count else null;
    }
}

fn updateFlightSize(self: *MessageQueue, tsn: u32, comptime inc: bool) void {
    const index = tsn -% self.chunks.atPtr(0).tsn;
    const chunk_size: u32 = @intCast(self.chunks.atPtr(index).user_data.len);
    if (inc) self.flight_size += chunk_size else self.flight_size -= chunk_size;
}

fn writeChunkAt(self: *const MessageQueue, tsn: u32, buffer: []u8) u32 {
    const index = tsn -% self.chunks.atPtr(0).tsn;
    return @intCast(self.chunks.at(index).write(buffer));
}

fn nextRetransmit(self: *MessageQueue, buffer: []u8) error{WriteFailed}!?u32 {
    const start_tsn = self.getCurrentRetransmitTsn() orelse return null;
    const acked = &self.sack_handler.acked;
    const highest_tsn = acked.highest_tsn_received;
    const tsn = if (Helper.tsnGt(start_tsn, highest_tsn))
        start_tsn
    else
        acked.firstUnacked(start_tsn, highest_tsn) orelse highest_tsn +% 1;

    if (Helper.tsnGt(tsn, self.highestTsnSent())) {
        self.retransmit_index = null;
        return null;
    }

    self.retransmit_index = tsn -% self.chunks.atPtr(0).tsn;
    const chunk = self.chunks.atPtr(self.retransmit_index.?);
    if (chunk.getSize() > buffer.len) return error.WriteFailed;
    self.flight_size += @intCast(chunk.user_data.len);
    self.rwnd -|= @intCast(chunk.user_data.len);
    self.nextRetransmitIndex();
    return @intCast(chunk.write(buffer));
}

fn nextChunkSize(self: *MessageQueue, buffer_size: usize) ?u32 {
    if (self.curr_msg >= self.messages.len) return null;
    if (self.chunks.len >= 1024) return null;
    const msg = self.messages.atPtr(self.curr_msg);
    const curr_payload = msg.data.len - self.message_offset;
    const max_payload = std.mem.alignBackward(usize, buffer_size - message.data_chunk_header_size, 4);
    return @intCast(@min(curr_payload, max_payload));
}

fn nextNewChunk(self: *MessageQueue, allocator: std.mem.Allocator, buffer: []u8) std.mem.Allocator.Error!usize {
    std.debug.assert(self.curr_msg < self.messages.len);
    try self.chunks.ensureUnusedCapacity(allocator, 1);

    const msg = self.messages.atPtr(self.curr_msg);
    const max_payload = std.mem.alignBackward(usize, buffer.len - message.data_chunk_header_size, 4);
    const payload_size = @min(msg.data.len - self.message_offset, max_payload);

    const chunk = message.Data{
        .flags = .{
            .start_fragment = self.message_offset == 0,
            .end_fragment = self.message_offset + payload_size == msg.data.len,
            .unordered = msg.unordered,
        },
        .tsn = self.curr_tsn,
        .ppid = msg.ppid,
        .stream_id = msg.stream_id,
        .stream_seq = msg.stream_seq,
        .user_data = msg.data[self.message_offset .. self.message_offset + payload_size],
    };
    self.chunks.pushBackAssumeCapacity(chunk);
    self.curr_tsn +%= 1;

    if (chunk.flags.end_fragment) {
        self.curr_msg += 1;
        self.message_offset = 0;
    } else {
        self.message_offset += @intCast(payload_size);
    }

    self.flight_size += @intCast(payload_size);
    self.rwnd -|= @intCast(payload_size);

    return chunk.write(buffer);
}

fn getChunkSizeAt(self: *const MessageQueue, tsn: u32) u32 {
    const index = tsn -% self.chunks.atPtr(0).tsn;
    return self.chunks.atPtr(index).getSize();
}

fn updateGapAckedFlightSize(self: *MessageQueue, comptime inc: bool) u32 {
    var it = self.sack_handler.iterateGapAcked();
    var acked_data: u32 = 0;
    const max_tsn = if (self.getCurrentRetransmitTsn()) |tsn| tsn -% 1 else self.sack_handler.acked.highest_tsn_received;
    while (it.next()) |tsn| {
        if (Helper.tsnLte(tsn, max_tsn)) self.updateFlightSize(tsn, inc);
        acked_data += self.getChunkSizeAt(tsn);
    }

    return acked_data;
}

fn getCurrentRetransmitTsn(self: *const MessageQueue) ?u32 {
    if (self.retransmit_index) |index| return self.chunks.atPtr(index).tsn;
    return null;
}

fn nextRetransmitIndex(self: *MessageQueue) void {
    if (self.retransmit_index.? + 1 >= self.chunks.len) {
        self.retransmit_index = null;
    } else {
        self.retransmit_index = self.retransmit_index.? + 1;
    }
}

const testing = std.testing;

fn testSendChunks(msg_queue: *MessageQueue, count: usize) !void {
    var buffer: [64]u8 = undefined;
    for (0..count) |_| {
        try msg_queue.pushData(testing.allocator, "aaaa", .{ .stream_id = 0, .ppid = 0 });
        _ = try msg_queue.nextNewChunk(testing.allocator, &buffer);
    }
}

test "MessageQueue.pushData: increment sequence and buffered size" {
    var msg_queue = MessageQueue.init(0x12345678);
    defer msg_queue.deinit(testing.allocator);

    try msg_queue.pushData(testing.allocator, "Hello", .{
        .ppid = 42,
        .stream_id = 100,
        .unordered = false,
    });
    try testing.expectEqual(1, msg_queue.streams.get(100).?.stream_seq);
    try testing.expectEqual(5, msg_queue.streams.get(100).?.buffered);

    try msg_queue.pushData(testing.allocator, "World", .{
        .ppid = 43,
        .stream_id = 100,
        .unordered = true,
    });
    try testing.expectEqual(1, msg_queue.streams.get(100).?.stream_seq);
    try testing.expectEqual(10, msg_queue.streams.get(100).?.buffered);
}

test "MessageQueue.nextRetransmit: retransmitting the last chunk clears the retransmit index" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try testSendChunks(&msg_queue, 2);
    msg_queue.retransmit_index = 0;
    msg_queue.flight_size = 0;

    var buffer: [64]u8 = undefined;
    _ = (try msg_queue.nextRetransmit(&buffer)).?;
    _ = (try msg_queue.nextRetransmit(&buffer)).?;

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
        .gap_ack_blocks = &.{ 0, 2, 0, 3, 0, 5, 0, 5 },
        .duplicate_tsns = &.{},
    }, false);
    msg_queue.handleTimeout();

    var buffer: [64]u8 = undefined;
    for ([_]u32{ 2, 5 }) |tsn| {
        _ = (try msg_queue.nextRetransmit(&buffer)).?;
        try testing.expectEqual(tsn, std.mem.readInt(u32, buffer[4..8], .big));
    }
    try testing.expectEqual(null, try msg_queue.nextRetransmit(&buffer));
    try testing.expectEqual(null, msg_queue.retransmit_index);
}

test "MessageQueue.handleSack: acknowledging every chunk during a retransmission clears the retransmit index" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try testSendChunks(&msg_queue, 3);
    msg_queue.retransmit_index = 0;
    msg_queue.flight_size = 0;

    var buffer: [64]u8 = undefined;
    _ = (try msg_queue.nextRetransmit(&buffer)).?;
    try testing.expectEqual(4, msg_queue.flight_size);

    _ = try msg_queue.handleSack(testing.allocator, message.Sack{
        .cumulative_tsn = 3,
        .a_rwnd = 0,
        .gap_ack_blocks = &.{},
        .duplicate_tsns = &.{},
    }, false);

    try testing.expectEqual(null, msg_queue.retransmit_index);
    try testing.expectEqual(null, msg_queue.getCurrentRetransmitTsn());
    try testing.expectEqual(0, msg_queue.flight_size);
}

fn testAckUpTo(msg_queue: *MessageQueue, allocator: std.mem.Allocator, cumulative_tsn: u32) !void {
    _ = try msg_queue.handleSack(allocator, message.Sack{
        .cumulative_tsn = cumulative_tsn,
        .a_rwnd = 1000,
        .gap_ack_blocks = &.{},
        .duplicate_tsns = &.{},
    }, false);
}

test "MessageQueue.setBufferedAmountLowThreshold: initializes the stream state of a new stream" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try msg_queue.setBufferedAmountLowThreshold(testing.allocator, 7, 100);
    try testing.expectEqual(StreamState{ .stream_seq = 0, .buffered_low_thresh = 100, .buffered = 0 }, msg_queue.streams.get(7).?);

    try msg_queue.pushData(testing.allocator, "Hello", .{ .stream_id = 7, .ppid = 0 });
    try testing.expectEqual(0, msg_queue.messages.at(0).stream_seq);
    try testing.expectEqual(StreamState{ .stream_seq = 1, .buffered_low_thresh = 100, .buffered = 5 }, msg_queue.streams.get(7).?);
}

test "MessageQueue.handleSack: buffered amount low event is emitted only when crossing the threshold" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try msg_queue.setBufferedAmountLowThreshold(testing.allocator, 0, 4);
    try testSendChunks(&msg_queue, 3);

    // 12 -> 8: still above the threshold
    try testAckUpTo(&msg_queue, testing.allocator, 1);
    try testing.expectEqual(0, msg_queue.events.len);

    // 8 -> 4: crosses the threshold
    try testAckUpTo(&msg_queue, testing.allocator, 2);
    try testing.expectEqual(1, msg_queue.events.len);
    try testing.expectEqual(Event{ .buffered_amount_low_threshold = 0 }, msg_queue.events.at(0));

    // 4 -> 0: was already at or below the threshold
    try testAckUpTo(&msg_queue, testing.allocator, 3);
    try testing.expectEqual(1, msg_queue.events.len);
}

test "MessageQueue.handleSack: one buffered amount low event per stream crossing its threshold" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try msg_queue.setBufferedAmountLowThreshold(testing.allocator, 1, 4);
    try msg_queue.pushData(testing.allocator, "aaaabbbbcccc", .{ .stream_id = 1, .ppid = 0 });
    try msg_queue.pushData(testing.allocator, "dddd", .{ .stream_id = 2, .ppid = 0 });

    // Room for 4 bytes of payload: stream 1 is sent as three fragments
    var buffer: [message.data_chunk_header_size + 4]u8 = undefined;
    for (0..4) |_| _ = try msg_queue.nextNewChunk(testing.allocator, &buffer);

    try testAckUpTo(&msg_queue, testing.allocator, 4);
    try testing.expectEqual(2, msg_queue.events.len);
    try testing.expectEqual(Event{ .buffered_amount_low_threshold = 1 }, msg_queue.events.at(0));
    try testing.expectEqual(Event{ .buffered_amount_low_threshold = 2 }, msg_queue.events.at(1));
}

test "MessageQueue.handleSack: running out of memory for events keeps the queue consistent" {
    var failing = testing.FailingAllocator.init(testing.allocator, .{});
    const allocator = failing.allocator();

    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(allocator);

    var buffer: [64]u8 = undefined;
    for ([_]u16{ 1, 0, 0, 0 }) |stream_id| {
        try msg_queue.pushData(allocator, "aaaa", .{ .stream_id = stream_id, .ppid = 0 });
        _ = try msg_queue.nextNewChunk(allocator, &buffer);
    }

    // Dropping tsn 1 fills the only event slot, reserving the next one fails
    try msg_queue.events.ensureTotalCapacityPrecise(allocator, 1);
    failing.fail_index = failing.alloc_index;
    failing.resize_fail_index = failing.resize_index;

    const sack = message.Sack{
        .cumulative_tsn = 2,
        .a_rwnd = 1000,
        .gap_ack_blocks = &.{ 0, 2, 0, 2 },
        .duplicate_tsns = &.{},
    };
    try testing.expectError(error.OutOfMemory, msg_queue.handleSack(allocator, sack, false));
    try testing.expectEqual(3, msg_queue.chunks.len);
    try testing.expectEqual(1, msg_queue.events.len);
    // tsn 1 is dropped and the gap acked tsn 4 is still excluded
    try testing.expectEqual(8, msg_queue.flight_size);

    failing.fail_index = std.math.maxInt(usize);
    failing.resize_fail_index = std.math.maxInt(usize);
    _ = try msg_queue.handleSack(allocator, sack, false);
    try testing.expectEqual(2, msg_queue.chunks.len);
    try testing.expectEqual(4, msg_queue.flight_size);
}
