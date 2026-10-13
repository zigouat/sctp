const std = @import("std");
const message = @import("message.zig");
const Helper = @import("helper.zig");

const MessageQueue = @This();
const SackHandler = @import("sack_handler.zig");

streams_seq: std.AutoHashMapUnmanaged(u16, u16),
messages: std.Deque(message.UserMessage),
chunks: std.Deque(message.Data),
flight_size: u32,
pending_size: u32,
curr_tsn: u32,
curr_msg: u32,
message_offset: u32,
retransmit_index: ?u32,

pub fn init(initial_tsn: u32) MessageQueue {
    return .{
        .streams_seq = .empty,
        .messages = .empty,
        .chunks = .empty,
        .curr_tsn = initial_tsn,
        .curr_msg = 0,
        .message_offset = 0,
        .retransmit_index = null,
        .flight_size = 0,
        .pending_size = 0,
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

pub fn nextChunkSize(self: *MessageQueue, buffer_size: usize) ?u32 {
    if (self.curr_msg >= self.messages.len) return null;
    if (self.chunks.len >= 1024) return null;
    const msg = self.messages.atPtr(self.curr_msg);
    const curr_payload = msg.data.len - self.message_offset;
    const max_payload = std.mem.alignBackward(usize, buffer_size - message.data_chunk_header_size, 4);
    return @intCast(@min(curr_payload, max_payload));
}

pub fn nextChunk(self: *MessageQueue, allocator: std.mem.Allocator, buffer: []u8) std.mem.Allocator.Error!struct { usize, usize } {
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

    return .{ chunk.user_data.len, chunk.write(buffer) };
}

pub fn getOutstandingDataSize(self: *const MessageQueue, sack_handler: *const SackHandler) u32 {
    var start: usize = 0;
    const end: usize = if (self.retransmit_index) |index| index else self.chunks.len; // a retransmission in progress.

    var size: u32 = 0;
    while (start < end) : (start += 1) {
        const chunk = self.chunks.atPtr(start);
        if (sack_handler.isAcked(chunk.tsn)) continue;
        size += @intCast(chunk.user_data.len);
    }

    return size;
}

pub fn nextRetransmit(self: *MessageQueue, sack_handler: *const SackHandler, buffer: []u8) error{WriteFailed}!?struct { u32, u32 } {
    if (self.retransmit_index == null or self.retransmit_index.? >= self.chunks.len) {
        self.retransmit_index = null;
        return null;
    }

    while (true) {
        const chunk = self.chunks.atPtr(self.retransmit_index.?);
        if (sack_handler.isAcked(chunk.tsn)) {
            self.retransmit_index = self.retransmit_index.? + 1;
            if (self.retransmit_index.? >= self.chunks.len) {
                self.retransmit_index = null;
                return null;
            }
            continue;
        }

        if (chunk.getSize() > buffer.len) return error.WriteFailed;
        self.retransmit_index = self.retransmit_index.? + 1;
        return .{ @intCast(chunk.user_data.len), @intCast(chunk.write(buffer)) };
    }
}

pub fn dropAcknowledged(self: *MessageQueue, allocator: std.mem.Allocator, ack_tsn: u32) void {
    if (self.isEmpty()) return;

    var dropped: u32 = 0;
    while (self.chunks.frontPtr()) |chunk| {
        if (!Helper.tsnLte(chunk.tsn, ack_tsn)) break;

        if (chunk.flags.end_fragment) {
            const msg = self.messages.popFront().?;
            allocator.free(msg.data);
            self.curr_msg -= 1;
        }

        self.flight_size -|= @intCast(chunk.user_data.len);
        _ = self.chunks.popFront();
        dropped += 1;
    }

    if (self.retransmit_index) |*index| index.* -|= dropped;
}

pub fn isEmpty(self: *const MessageQueue) bool {
    return self.messages.len == 0;
}

pub fn updateFlightSize(self: *MessageQueue, tsn: u32, comptime inc: bool) void {
    const index = tsn -% self.chunks.atPtr(0).tsn;
    const chunk_size: u32 = @intCast(self.chunks.atPtr(index).user_data.len);
    if (inc) self.flight_size += chunk_size else self.flight_size -= chunk_size;
}

const testing = std.testing;

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
