//! FIFO (First In First Out): messages served in the order they came in. The only scheduler when interleaving is not supported
//!
//! One entry per message not fully sent, a message stays at the front until its last fragment is sent so the fragments
//! of different messages are never interleaved.
const std = @import("std");

const Fifo = @This();
const AllocError = std.mem.Allocator.Error;

streams: std.Deque(u16),

pub const empty: Fifo = .{ .streams = .empty };

pub fn deinit(self: *Fifo, allocator: std.mem.Allocator) void {
    self.streams.deinit(allocator);
}

pub fn clear(self: *Fifo) void {
    while (self.streams.popFront()) |_| {}
}

pub fn ensureUnusedCapacity(self: *Fifo, allocator: std.mem.Allocator, count: usize) AllocError!void {
    try self.streams.ensureUnusedCapacity(allocator, count);
}

/// The stream whose turn it is.
pub fn peek(self: *const Fifo) ?u16 {
    return self.streams.front();
}

/// A message of `stream_id` is queued, the capacity must be reserved.
pub fn pushMessage(self: *Fifo, stream_id: u16, had_unsent: bool) void {
    _ = had_unsent;
    self.streams.pushBackAssumeCapacity(stream_id);
}

/// A chunk of the current message of the stream whose turn it is was sent.
pub fn chunkSent(self: *Fifo, message_done: bool, stream_has_more: bool) void {
    _ = stream_has_more;
    if (message_done) _ = self.streams.popFront();
}

/// The current message of the stream whose turn it is is dropped before it was fully sent.
pub fn messageDropped(self: *Fifo) void {
    _ = self.streams.popFront();
}

/// The stream whose turn it is has nothing left to send.
pub fn skipStream(self: *Fifo) void {
    _ = self.streams.popFront();
}

/// Removes the messages of the streams at or above `max_streams`.
pub fn dropStreams(self: *Fifo, max_streams: u16) void {
    for (0..self.streams.len) |_| {
        const stream_id = self.streams.popFront().?;
        if (stream_id < max_streams) self.streams.pushBackAssumeCapacity(stream_id);
    }
}
