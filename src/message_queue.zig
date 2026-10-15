const std = @import("std");
const message = @import("message.zig");
const Helper = @import("helper.zig");

const MessageQueue = @This();
const SackHandler = @import("sack_handler.zig");

sack_handler: SackHandler,
streams_seq: std.AutoHashMapUnmanaged(u16, u16),
messages: std.Deque(message.UserMessage),
chunks: std.Deque(message.Data),
flight_size: u32,
pending_size: u32,
rwnd: u32,
curr_tsn: u32,
curr_msg: u32,
message_offset: u32,
retransmit_index: ?u32,

pub fn init(initial_tsn: u32) MessageQueue {
    return .{
        .sack_handler = .init(initial_tsn),
        .streams_seq = .empty,
        .messages = .empty,
        .chunks = .empty,
        .curr_tsn = initial_tsn,
        .curr_msg = 0,
        .message_offset = 0,
        .retransmit_index = null,
        .flight_size = 0,
        .pending_size = 0,
        .rwnd = 0,
    };
}

pub fn deinit(self: *MessageQueue, allocator: std.mem.Allocator) void {
    while (self.messages.popFront()) |msg| allocator.free(msg.data);
    self.streams_seq.deinit(allocator);
    self.chunks.deinit(allocator);
    self.messages.deinit(allocator);
}

pub fn close(self: *MessageQueue, allocator: std.mem.Allocator, initial_tsn: u32) void {
    self.streams_seq.clearAndFree(allocator);
    while (self.chunks.popFront()) |_| {}
    while (self.messages.popFront()) |msg| allocator.free(msg.data);
    self.curr_tsn = initial_tsn;
    self.curr_msg = 0;
    self.message_offset = 0;
    self.retransmit_index = null;
    self.sack_handler = .init(initial_tsn);
}

pub fn pushData(self: *MessageQueue, allocator: std.mem.Allocator, data: []const u8, config: message.UserMessageConfig) !void {
    try self.messages.ensureUnusedCapacity(allocator, 1);
    try self.streams_seq.ensureUnusedCapacity(allocator, 1);
    const data_copy = try allocator.dupe(u8, data);

    const result = self.streams_seq.getOrPutAssumeCapacity(config.stream_id);
    if (!result.found_existing) result.value_ptr.* = 0;

    self.messages.pushBackAssumeCapacity(.{
        .data = data_copy,
        .ppid = config.ppid,
        .stream_id = config.stream_id,
        .stream_seq = if (config.unordered) 0 else result.value_ptr.*,
        .unordered = config.unordered,
    });
    self.pending_size +|= @intCast(data.len);
    if (!config.unordered) result.value_ptr.* +%= 1;
}

pub const SackResult = struct {
    acked_data: u32,
    duplicate_data: u32,
    has_fast_retransmit: bool,
    cumulative_ack_advanced: bool,
};

pub fn handleSack(self: *MessageQueue, allocator: std.mem.Allocator, sack: message.Sack) ?SackResult {
    const prev_flight_size = self.flight_size;
    const prev_cumulative_tsn = self.sack_handler.cumulative_tsn;
    var acked_data: u32 = 0;
    var duplicate_data: u32 = 0;

    const old_gap_size = self.updateGapAckedFlightSize(true);
    const has_fast_retransmit = self.sack_handler.handleSack(&sack, self.highestTsnSent()) catch {
        self.flight_size = prev_flight_size;
        return null;
    };

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

    return .{
        .acked_data = acked_data,
        .duplicate_data = duplicate_data,
        .has_fast_retransmit = has_fast_retransmit,
        .cumulative_ack_advanced = prev_cumulative_tsn != self.sack_handler.cumulative_tsn,
    };
}

pub const CumulativeAck = enum { invalid, unchanged, advanced };

pub fn handleShutdownChunk(self: *MessageQueue, allocator: std.mem.Allocator, cumulative_tsn: u32) CumulativeAck {
    if (Helper.tsnGt(cumulative_tsn, self.highestTsnSent())) return .invalid;
    if (!Helper.tsnGt(cumulative_tsn, self.sack_handler.cumulative_tsn)) return .unchanged;
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
}

pub fn nextChunkSize(self: *MessageQueue, buffer_size: usize) ?u32 {
    if (self.curr_msg >= self.messages.len) return null;
    if (self.chunks.len >= 1024) return null;
    const msg = self.messages.atPtr(self.curr_msg);
    const curr_payload = msg.data.len - self.message_offset;
    const max_payload = std.mem.alignBackward(usize, buffer_size - message.data_chunk_header_size, 4);
    return @intCast(@min(curr_payload, max_payload));
}

pub fn nextChunk(self: *MessageQueue, allocator: std.mem.Allocator, buffer: []u8) std.mem.Allocator.Error!usize {
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

    self.pending_size -|= @intCast(payload_size);
    self.flight_size += @intCast(payload_size);
    self.rwnd -|= @intCast(payload_size);

    return chunk.write(buffer);
}

pub fn nextRetransmit(self: *MessageQueue, buffer: []u8) error{WriteFailed}!?u32 {
    while (true) {
        if (self.retransmit_index == null) return null;
        const chunk = self.chunks.atPtr(self.retransmit_index.?);
        if (self.sack_handler.isAcked(chunk.tsn)) {
            self.nextRetransmitIndex();
            continue;
        }

        if (chunk.getSize() > buffer.len) return error.WriteFailed;
        self.flight_size += @intCast(chunk.user_data.len);
        self.rwnd -|= @intCast(chunk.user_data.len);
        self.nextRetransmitIndex();
        return @intCast(chunk.write(buffer));
    }
}

pub fn nextFastRetransmit(self: *MessageQueue, buffer: []u8) error{WriteFailed}!?u32 {
    if (self.sack_handler.firstTsnWithThreeMisses(self.sack_handler.cumulative_tsn +% 1)) |tsn| {
        if (self.getChunkSizeAt(tsn) > buffer.len) return error.WriteFailed;
        self.sack_handler.markFastRetransmitted(tsn);
        return self.writeChunkAt(tsn, buffer);
    }

    return null;
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
    return Helper.tsnLte(tsn, self.sack_handler.cumulative_tsn);
}

fn dropAcknowledged(self: *MessageQueue, allocator: std.mem.Allocator) u32 {
    if (self.chunks.len == 0) return 0;
    var data_size: u32 = 0;

    const retransmit_index = self.retransmit_index orelse self.chunks.len;
    const dropped = self.sack_handler.cumulative_tsn -% self.chunks.frontPtr().?.tsn +% 1;
    for (0..dropped) |index| {
        const chunk = self.chunks.popFront().?;
        if (chunk.flags.end_fragment) {
            const msg = self.messages.popFront().?;
            allocator.free(msg.data);
            self.curr_msg -= 1;
        }
        if (index < retransmit_index) self.flight_size -= @intCast(chunk.user_data.len);
        data_size +|= chunk.getSize();
    }

    if (self.retransmit_index) |index| {
        self.retransmit_index = if (index -| dropped < self.chunks.len) index -| dropped else null;
    }

    return data_size;
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

fn getChunkSizeAt(self: *const MessageQueue, tsn: u32) u32 {
    const index = tsn -% self.chunks.atPtr(0).tsn;
    return self.chunks.atPtr(index).getSize();
}

fn updateGapAckedFlightSize(self: *MessageQueue, comptime inc: bool) u32 {
    var it = self.sack_handler.iterateGapAcked();
    var acked_data: u32 = 0;
    const max_tsn = if (self.getCurrentRetransmitTsn()) |tsn| tsn -% 1 else self.sack_handler.highest_tsn_received;
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
        _ = try msg_queue.nextChunk(testing.allocator, &buffer);
    }
}

test "MessageQueue.pushData: unordered data does not increment stream sequence number" {
    var msg_queue = MessageQueue.init(0x12345678);
    defer msg_queue.deinit(testing.allocator);

    try msg_queue.pushData(testing.allocator, "Hello", .{
        .ppid = 42,
        .stream_id = 100,
        .unordered = false,
    });
    try testing.expectEqual(1, msg_queue.streams_seq.get(100).?);

    try msg_queue.pushData(testing.allocator, "World", .{
        .ppid = 43,
        .stream_id = 1,
        .unordered = true,
    });
    try testing.expectEqual(1, msg_queue.streams_seq.get(100).?);
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

test "MessageQueue.handleSack: acknowledging every chunk during a retransmission clears the retransmit index" {
    var msg_queue = MessageQueue.init(1);
    defer msg_queue.deinit(testing.allocator);

    try testSendChunks(&msg_queue, 3);
    msg_queue.retransmit_index = 0;
    msg_queue.flight_size = 0;

    var buffer: [64]u8 = undefined;
    _ = (try msg_queue.nextRetransmit(&buffer)).?;
    try testing.expectEqual(4, msg_queue.flight_size);

    _ = msg_queue.handleSack(testing.allocator, message.Sack{
        .cumulative_tsn = 3,
        .a_rwnd = 0,
        .gap_ack_blocks = &.{},
        .duplicate_tsns = &.{},
    });

    try testing.expectEqual(null, msg_queue.retransmit_index);
    try testing.expectEqual(null, msg_queue.getCurrentRetransmitTsn());
    try testing.expectEqual(0, msg_queue.flight_size);
}
