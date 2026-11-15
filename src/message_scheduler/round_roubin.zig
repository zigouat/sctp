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

/// The stream whose turn it is, it stays scheduled.
pub fn peek(self: *const RoundRobin) ?u16 {
    return self.streams.front();
}

/// Removes the stream whose turn it is, push it back to give it another turn.
pub fn nextStream(self: *RoundRobin) ?u16 {
    return self.streams.popFront();
}

pub fn pushBack(self: *RoundRobin, allocator: std.mem.Allocator, stream_id: u16) AllocError!void {
    try self.streams.pushBack(allocator, stream_id);
}

/// Removes the streams at or above `max_streams`.
pub fn dropStreams(self: *RoundRobin, max_streams: u16) void {
    for (0..self.streams.len) |_| {
        const stream_id = self.streams.popFront().?;
        if (stream_id < max_streams) self.streams.pushBackAssumeCapacity(stream_id);
    }
}
