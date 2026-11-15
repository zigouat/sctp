const std = @import("std");
const message = @import("message.zig");
const Helper = @import("helper.zig");

pub const Sequential = @import("message_queue/sequential.zig");
pub const Interleaver = @import("message_queue/interleaver.zig");

/// RFC8260 - 2.2.1. user messages are sent in I-DATA chunks once message interleaving is negotiated, in DATA chunks otherwise.
const MessageQueue = @This();

pub const Event = union(enum) {
    buffered_amount_low_threshold: u16,
};

pub const SackResult = struct {
    acked_data: u32,
    duplicate_data: u32,
    has_fast_retransmit: bool,
    cumulative_ack_advanced: bool,
};

pub const CumulativeAck = enum { invalid, unchanged, advanced };

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

pub const FastRetransmit = struct {
    tsn: u32,
    size: u32,
};

/// The stream reset holds back messages as `Sequential` messages.
pub const Message = Sequential.Message;

const AllocError = std.mem.Allocator.Error;

inner: union(enum) {
    data: Sequential,
    idata: Interleaver,
},

pub fn init(initial_tsn: u32) MessageQueue {
    return .{ .inner = .{ .data = .init(initial_tsn) } };
}

pub fn deinit(self: *MessageQueue, allocator: std.mem.Allocator) void {
    switch (self.inner) {
        inline else => |*q| q.deinit(allocator),
    }
}

pub fn close(self: *MessageQueue, allocator: std.mem.Allocator, initial_tsn: u32) void {
    switch (self.inner) {
        inline else => |*q| q.close(allocator, initial_tsn),
    }
}

/// Send the messages in I-DATA chunks, the messages written before the handshake completed are moved over.
///
/// No chunk must have been sent yet.
pub fn useInterleaving(self: *MessageQueue, allocator: std.mem.Allocator, initial_tsn: u32) AllocError!void {
    const old = switch (self.inner) {
        .data => |*q| q,
        .idata => return,
    };
    std.debug.assert(old.chunks.len == 0);

    var queue: Interleaver = .init(initial_tsn);
    errdefer queue.deinit(allocator);
    queue.rwnd = old.rwnd;

    var it = old.streams.iterator();
    while (it.next()) |entry| if (entry.value_ptr.buffered_low_thresh != 0) {
        try queue.setBufferedAmountLowThreshold(allocator, entry.key_ptr.*, entry.value_ptr.buffered_low_thresh);
    };
    // A message leaves the old queue only once `queue` owns it, so a failure frees it once.
    while (old.messages.frontPtr()) |msg| {
        try queue.pushUserMessage(allocator, &toInterleaverMessage(msg));
        _ = old.messages.popFront();
    }

    old.deinit(allocator);
    self.inner = .{ .idata = queue };
}

/// Send the messages in DATA chunks, the queue must be empty (e.g. after `close`).
pub fn useSequential(self: *MessageQueue, allocator: std.mem.Allocator, initial_tsn: u32) void {
    if (self.inner == .data) return;
    std.debug.assert(self.isEmpty());
    self.deinit(allocator);
    self.inner = .{ .data = .init(initial_tsn) };
}

pub fn pushData(self: *MessageQueue, allocator: std.mem.Allocator, data: []const u8, config: message.UserMessageConfig) AllocError!void {
    return switch (self.inner) {
        inline else => |*q| q.pushData(allocator, data, config),
    };
}

/// The stream reset holds back messages as `Sequential` messages.
pub fn pushUserMessage(self: *MessageQueue, allocator: std.mem.Allocator, msg: *const Message) AllocError!void {
    return switch (self.inner) {
        .data => |*q| q.pushUserMessage(allocator, msg),
        .idata => |*q| q.pushUserMessage(allocator, &toInterleaverMessage(msg)),
    };
}

pub fn dropMessages(self: *MessageQueue, allocator: std.mem.Allocator, max_streams: u16) AllocError![]u16 {
    return switch (self.inner) {
        inline else => |*q| q.dropMessages(allocator, max_streams),
    };
}

pub fn handleSack(self: *MessageQueue, allocator: std.mem.Allocator, sack: message.Sack, fast_recovery: bool) AllocError!?SackResult {
    return switch (self.inner) {
        inline else => |*q| q.handleSack(allocator, sack, fast_recovery),
    };
}

pub fn handleShutdownChunk(self: *MessageQueue, allocator: std.mem.Allocator, cumulative_tsn: u32) CumulativeAck {
    return switch (self.inner) {
        inline else => |*q| q.handleShutdownChunk(allocator, cumulative_tsn),
    };
}

pub fn handleTimeout(self: *MessageQueue) void {
    switch (self.inner) {
        inline else => |*q| q.handleTimeout(),
    }
}

pub fn resetStreams(self: *MessageQueue, streams: []const u16) void {
    switch (self.inner) {
        inline else => |*q| q.resetStreams(streams),
    }
}

pub fn bufferedAmount(self: *const MessageQueue, stream_id: u16) ?u32 {
    return switch (self.inner) {
        inline else => |*q| q.bufferedAmount(stream_id),
    };
}

pub fn setBufferedAmountLowThreshold(self: *MessageQueue, allocator: std.mem.Allocator, stream_id: u16, threshold: u32) AllocError!void {
    return switch (self.inner) {
        inline else => |*q| q.setBufferedAmountLowThreshold(allocator, stream_id, threshold),
    };
}

pub fn popEvent(self: *MessageQueue) ?Event {
    return switch (self.inner) {
        inline else => |*q| q.popEvent(),
    };
}

pub fn nextChunk(
    self: *MessageQueue,
    allocator: std.mem.Allocator,
    buffer: []u8,
    options: ChunkOptions,
    now: i64,
) NextChunkError!?NextChunkResult {
    return switch (self.inner) {
        inline else => |*q| q.nextChunk(allocator, buffer, options, now),
    };
}

pub fn nextFastRetransmit(
    self: *MessageQueue,
    allocator: std.mem.Allocator,
    buffer: []u8,
    options: ChunkOptions,
    now: i64,
) NextChunkError!?FastRetransmit {
    return switch (self.inner) {
        inline else => |*q| q.nextFastRetransmit(allocator, buffer, options, now),
    };
}

pub fn nextForwardTsn(self: *MessageQueue, buffer: []u8) error{WriteFailed}!?u32 {
    return switch (self.inner) {
        inline else => |*q| q.nextForwardTsn(buffer),
    };
}

/// Whether a message of one of `streams` is not fully assigned TSNs yet, `null` checks all the streams.
pub fn hasUnsentMessages(self: *const MessageQueue, streams: ?*const std.AutoHashMapUnmanaged(u16, void)) bool {
    switch (self.inner) {
        .data => |*q| {
            // Messages before `curr_msg` are fully assigned TSNs, the one at `curr_msg` may be partially sent.
            for (q.curr_msg..q.messages.len) |index| {
                const s = streams orelse return true;
                if (s.contains(q.messages.atPtr(index).stream_id)) return true;
            }
            return false;
        },
        .idata => |*q| {
            var it = q.streams.iterator();
            while (it.next()) |entry| {
                // Messages before `curr_message_index` are fully assigned TSNs, the current one may be partially sent.
                if (entry.value_ptr.curr_message_index >= entry.value_ptr.messages.len) continue;
                const s = streams orelse return true;
                if (s.contains(entry.key_ptr.*)) return true;
            }
            return false;
        },
    }
}

/// Changes when the queue may have finished sending a message, a pending stream reset is checked again then.
///
/// `null` for the interleaver, which also finishes messages by abandoning them without sending anything.
pub fn sendProgress(self: *const MessageQueue) ?u32 {
    return switch (self.inner) {
        .data => |*q| q.curr_msg,
        .idata => null,
    };
}

pub fn flightSize(self: *const MessageQueue) u32 {
    return switch (self.inner) {
        inline else => |*q| q.flight_size,
    };
}

pub fn setRwnd(self: *MessageQueue, rwnd: u32) void {
    switch (self.inner) {
        inline else => |*q| q.rwnd = rwnd,
    }
}

pub fn lowestOutstandingTsn(self: *const MessageQueue) u32 {
    return switch (self.inner) {
        inline else => |*q| q.lowestOutstandingTsn(),
    };
}

pub fn highestTsnSent(self: *const MessageQueue) u32 {
    return switch (self.inner) {
        inline else => |*q| q.highestTsnSent(),
    };
}

/// No message is waiting to be sent or acknowledged.
pub fn isEmpty(self: *const MessageQueue) bool {
    return switch (self.inner) {
        .data => |*q| q.isEmpty(),
        .idata => |*q| q.chunks.len == 0 and q.stream_scheduler.peek() == null,
    };
}

pub fn isAllAcked(self: *const MessageQueue) bool {
    return switch (self.inner) {
        inline else => |*q| q.sack_handler.allAcked(q.highestTsnSent()),
    };
}

pub fn isAcked(self: *const MessageQueue, tsn: u32) bool {
    return switch (self.inner) {
        inline else => |*q| q.sack_handler.isAcked(tsn),
    };
}

pub fn isCumulativelyAcked(self: *const MessageQueue, tsn: u32) bool {
    return switch (self.inner) {
        inline else => |*q| Helper.tsnLte(tsn, q.sack_handler.acked.cumulative_tsn),
    };
}

fn toInterleaverMessage(msg: *const Message) Interleaver.Message {
    return .init(msg.data(), .{
        .stream_id = msg.stream_id,
        .ppid = msg.ppid,
        .unordered = msg.flags.unordered,
        .policy = msg.policy(),
    });
}

test "MessageQueue.useInterleaving: moves the queued messages and thresholds" {
    const allocator = std.testing.allocator;
    var queue: MessageQueue = .init(10);
    defer queue.deinit(allocator);

    queue.setRwnd(1000);
    try queue.setBufferedAmountLowThreshold(allocator, 1, 3);
    try queue.pushData(allocator, "a", .{ .stream_id = 1, .ppid = 0 });
    try queue.pushData(allocator, "bb", .{ .stream_id = 2, .ppid = 0, .unordered = true });
    try queue.pushData(allocator, "ccc", .{ .stream_id = 1, .ppid = 0 });

    try queue.useInterleaving(allocator, 10);
    try std.testing.expect(queue.inner == .idata);
    try std.testing.expectEqual(1000, queue.inner.idata.rwnd);
    try std.testing.expectEqual(4, queue.bufferedAmount(1));
    try std.testing.expectEqual(2, queue.bufferedAmount(2));

    const stream = queue.inner.idata.streams.getPtr(1).?;
    try std.testing.expectEqual(3, stream.buffered_low_thresh);
    try std.testing.expectEqualStrings("a", stream.messages.at(0).data());
    try std.testing.expectEqualStrings("ccc", stream.messages.at(1).data());
    try std.testing.expect(queue.inner.idata.streams.getPtr(2).?.messages.at(0).flags.unordered);
}
