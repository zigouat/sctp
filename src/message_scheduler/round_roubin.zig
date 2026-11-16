//! Round robin stream scheduler: the streams with data to send take turns, one chunk each.
const std = @import("std");

const RoundRobin = @This();
const AllocError = std.mem.Allocator.Error;

streams: std.Deque(u16),

pub const empty: RoundRobin = .{ .streams = .empty };

pub fn deinit(self: *RoundRobin, allocator: std.mem.Allocator) void {
    self.streams.deinit(allocator);
}

pub fn clear(self: *RoundRobin) void {
    while (self.streams.popFront()) |_| {}
}

pub fn ensureUnusedCapacity(self: *RoundRobin, allocator: std.mem.Allocator, count: usize) AllocError!void {
    try self.streams.ensureUnusedCapacity(allocator, count);
}

/// The stream whose turn it is.
pub fn peek(self: *const RoundRobin) ?u16 {
    return self.streams.front();
}

/// A message of `stream_id` is queued, `had_unsent` if the stream already had messages to send. The capacity must be
/// reserved.
pub fn pushMessage(self: *RoundRobin, stream_id: u16, had_unsent: bool) void {
    if (!had_unsent) self.streams.pushBackAssumeCapacity(stream_id);
}

/// A chunk of the current message of the stream whose turn it is was sent, the next stream takes its turn.
pub fn chunkSent(self: *RoundRobin, message_done: bool, stream_has_more: bool) void {
    _ = message_done;
    // popping makes room for the push
    const stream_id = self.streams.popFront().?;
    if (stream_has_more) self.streams.pushBackAssumeCapacity(stream_id);
}

/// The current message of the stream whose turn it is is dropped before it was fully sent.
pub fn messageDropped(self: *RoundRobin) void {
    _ = self;
}

/// The stream whose turn it is has nothing left to send.
pub fn skipStream(self: *RoundRobin) void {
    _ = self.streams.popFront();
}

/// Removes the streams at or above `max_streams`.
pub fn dropStreams(self: *RoundRobin, max_streams: u16) void {
    for (0..self.streams.len) |_| {
        const stream_id = self.streams.popFront().?;
        if (stream_id < max_streams) self.streams.pushBackAssumeCapacity(stream_id);
    }
}
