const std = @import("std");
const message = @import("message.zig");
const Helper = @import("helper.zig");
const SackHandler = @import("sack_handler.zig");

const MessageQueue = @This();

pub const mtu = 1200;
const max_chunk_size = mtu - message.packet_header_size - message.data_chunk_header_size;
const base_rto = 1; // seconds
const max_rto = 60; // seconds

const UserMessage = struct {
    data: []const u8,
    start_tsn: u32,
    end_tsn: u32,
    stream_id: u16,
    stream_seq: u16,
    ppid: u32,
    unordered: bool,
    rto: u8,
    deadline: i64,

    fn getChunkData(self: *const UserMessage, tsn: u32, buffer: []u8) []const u8 {
        const flags = message.Data.Flags{
            .immediate = false,
            .unordered = self.unordered,
            .start_fragment = tsn == self.start_tsn,
            .end_fragment = tsn == self.end_tsn,
        };

        const start = (tsn -% self.start_tsn) * max_chunk_size;
        const end = @min(start + max_chunk_size, self.data.len);
        const slice = self.data[start..end];

        const written = (message.Data{
            .flags = flags,
            .tsn = tsn,
            .stream_id = self.stream_id,
            .stream_seq = self.stream_seq,
            .ppid = self.ppid,
            .user_data = slice,
        }).write(buffer);

        return buffer[0..written];
    }
};

messages: std.ArrayListUnmanaged(UserMessage),
streams_seq: std.AutoHashMapUnmanaged(u16, u16),
curr_message: u16,
/// TSN to assign the next message
tsn: u32,
/// TSN to send next
next_tsn: u32,

retransmit_tsn: ?u32 = null,
retrasmit_message: u16 = 0,

pub fn init(initial_tsn: u32) MessageQueue {
    return .{
        .messages = .empty,
        .streams_seq = .empty,
        .tsn = initial_tsn,
        .curr_message = 0,
        .next_tsn = initial_tsn,
    };
}

pub fn deinit(self: *MessageQueue, allocator: std.mem.Allocator) void {
    self.messages.deinit(allocator);
    self.streams_seq.deinit(allocator);
}

pub fn close(self: *MessageQueue, allocator: std.mem.Allocator, initial_tsn: u32) void {
    self.messages.clearAndFree(allocator);
    self.curr_message = 0;
    self.tsn = initial_tsn;
    self.next_tsn = initial_tsn;
    self.retransmit_tsn = null;
    self.retrasmit_message = 0;
}

pub fn pushData(self: *MessageQueue, allocator: std.mem.Allocator, data: []const u8, config: message.UserMessageConfig) !void {
    try self.messages.ensureUnusedCapacity(allocator, 1);
    try self.streams_seq.ensureUnusedCapacity(allocator, 1);

    const result = self.streams_seq.getOrPutAssumeCapacity(config.stream_id);
    if (!result.found_existing) result.value_ptr.* = 0;
    const num_chunks: u32 = @intCast(@divFloor(data.len - 1, max_chunk_size) + 1);

    self.messages.appendAssumeCapacity(.{
        .data = data,
        .start_tsn = self.tsn,
        .end_tsn = self.tsn +% (num_chunks - 1),
        .stream_id = config.stream_id,
        .stream_seq = result.value_ptr.*,
        .ppid = config.ppid,
        .unordered = config.unordered,
        .deadline = std.math.maxInt(i64),
        .rto = base_rto,
    });
    self.tsn +%= num_chunks;
    result.value_ptr.* +%= 1;
}

pub const TimeoutIterator = struct {
    messages: []UserMessage,
    now: i64,
    index: u16,

    pub fn init(messages: []UserMessage, now: i64) TimeoutIterator {
        return TimeoutIterator{
            .messages = messages,
            .now = now,
            .index = 0,
        };
    }

    pub fn next(self: *TimeoutIterator) ?u16 {
        while (self.index < self.messages.len) {
            const msg = &self.messages[self.index];
            self.index += 1;
            if (msg.deadline <= self.now) {
                msg.deadline = self.now + @as(i64, msg.rto) * std.time.ms_per_s;
                msg.rto = @min(msg.rto * 2, max_rto);
                return self.index - 1;
            }
        }

        return null;
    }
};

pub fn handleTimeout(self: *MessageQueue, now: i64) TimeoutIterator {
    return TimeoutIterator.init(self.messages.items, now);
}

pub fn next(self: *MessageQueue, buffer: []u8, now: i64) ?[]const u8 {
    const len = self.messages.items.len;
    if (len == 0) return null;

    while (self.curr_message < len) {
        const msg = &self.messages.items[self.curr_message];
        if (msg.deadline == std.math.maxInt(i64)) {
            msg.deadline = now + @as(i64, msg.rto) * std.time.ms_per_s;
            msg.rto = @min(msg.rto * 2, max_rto);
        }
        if (Helper.tsnGt(self.next_tsn, msg.end_tsn)) {
            self.curr_message += 1;
            continue;
        }

        const result = msg.getChunkData(self.next_tsn, buffer);
        self.next_tsn +%= 1;
        return result;
    }

    return null;
}

pub fn nextRetransmit(self: *MessageQueue, sack_handler: *const SackHandler, buffer: []u8) ?[]const u8 {
    if (self.retransmit_tsn == null) return null;

    const msg = &self.messages.items[self.retrasmit_message];
    while (true) {
        if (Helper.tsnGt(self.retransmit_tsn.?, msg.end_tsn)) {
            self.retransmit_tsn = null;
            return null;
        }

        if (sack_handler.isAcked(self.retransmit_tsn.?)) {
            self.retransmit_tsn = self.retransmit_tsn.? +% 1;
            continue;
        }

        const result = msg.getChunkData(self.retransmit_tsn.?, buffer);
        self.retransmit_tsn = self.retransmit_tsn.? +% 1;
        return result;
    }
}

pub fn dropAcknowledged(self: *MessageQueue, ack_tsn: u32) ?[]const u8 {
    if (self.messages.items.len == 0) return null;

    const msg = &self.messages.items[0];
    if (!Helper.tsnLte(msg.end_tsn, ack_tsn)) return null;

    const result = msg.data;
    _ = self.messages.orderedRemove(0);
    self.curr_message -|= 1;
    return result;
}

pub fn isEmpty(self: *const MessageQueue) bool {
    return self.messages.items.len == 0;
}

pub fn pollTimeout(self: *MessageQueue) i64 {
    var deadline: i64 = std.math.maxInt(i64);
    for (self.messages.items) |*msg| deadline = @min(deadline, msg.deadline);
    return deadline;
}
