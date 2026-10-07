const std = @import("std");
const message = @import("message.zig");
const Helper = @import("helper.zig");

const MessageQueue = @This();
const SackHandler = @import("sack_handler.zig");

pub const mtu = 1200;

const UserMessage = struct {
    data: []const u8,
    highest_tsn: u32,
    stream_id: u16,
    stream_seq: u16,
    ppid: u32,
    unordered: bool,
};

streams_seq: std.AutoHashMapUnmanaged(u16, u16),
messages: std.Deque(UserMessage),
chunks: std.Deque(message.Data),
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
    };
}

pub fn deinit(self: *MessageQueue, allocator: std.mem.Allocator) void {
    self.streams_seq.deinit(allocator);
    self.messages.deinit(allocator);
    self.chunks.deinit(allocator);
}

pub fn close(self: *MessageQueue, allocator: std.mem.Allocator, initial_tsn: u32) void {
    self.streams_seq.clearAndFree(allocator);
    while (self.chunks.popFront()) |_| {}
    self.curr_tsn = initial_tsn;
    self.curr_msg = 0;
    self.message_offset = 0;
    self.retransmit_index = null;
}

pub fn pushData(self: *MessageQueue, allocator: std.mem.Allocator, data: []const u8, config: message.UserMessageConfig) !void {
    try self.messages.ensureUnusedCapacity(allocator, 1);
    try self.streams_seq.ensureUnusedCapacity(allocator, 1);

    const result = self.streams_seq.getOrPutAssumeCapacity(config.stream_id);
    if (!result.found_existing) result.value_ptr.* = 0;

    self.messages.pushBackAssumeCapacity(.{
        .data = data,
        .highest_tsn = 0,
        .ppid = config.ppid,
        .stream_id = config.stream_id,
        .stream_seq = if (config.unordered) 0 else result.value_ptr.*,
        .unordered = config.unordered,
    });
    if (!config.unordered) result.value_ptr.* +%= 1;
}

pub fn nextChunk(self: *MessageQueue, allocator: std.mem.Allocator, buffer: []u8) ?usize {
    if (self.curr_msg >= self.messages.len) return null;
    self.chunks.ensureUnusedCapacity(allocator, 1) catch return null;

    const msg = self.messages.atPtr(self.curr_msg);
    const payload_size = @min(msg.data.len - self.message_offset, buffer.len - message.data_chunk_header_size);

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

    msg.highest_tsn = chunk.tsn;
    if (chunk.flags.end_fragment) {
        self.curr_msg += 1;
        self.message_offset = 0;
    } else {
        self.message_offset += @intCast(payload_size);
    }

    return chunk.write(buffer);
}

pub fn nextRetransmit(self: *MessageQueue, sack_handler: *const SackHandler, buffer: []u8) ?usize {
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

        self.retransmit_index = self.retransmit_index.? + 1;
        return chunk.write(buffer);
    }
}

pub fn dropAcknowledged(self: *MessageQueue, ack_tsn: u32) ?[]const u8 {
    if (self.isEmpty()) return null;

    var dropped: u32 = 0;
    while (self.chunks.frontPtr()) |chunk| {
        if (Helper.tsnLte(chunk.tsn, ack_tsn)) {
            _ = self.chunks.popFront();
            dropped += 1;
        } else break;
    }

    if (self.retransmit_index) |*index| index.* -|= dropped;

    // only release messages whose fragments have all been sent
    if (self.curr_msg == 0) return null;

    if (self.messages.frontPtr()) |msg| if (Helper.tsnLte(msg.highest_tsn, ack_tsn)) {
        const data = msg.data;
        _ = self.messages.popFront();
        self.curr_msg -= 1;
        return data;
    };

    return null;
}

pub fn isEmpty(self: *const MessageQueue) bool {
    return self.messages.len == 0;
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
