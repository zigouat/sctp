const std = @import("std");
const message = @import("message.zig");

pub const Sequential = @import("message_assembler/sequential.zig");
pub const Interleaved = @import("message_assembler/interleaved.zig");

/// RFC8260 - 2.2.1. the peer sends I-DATA chunks once message interleaving is negotiated, DATA chunks otherwise.
const MessageAssembler = @This();

// The `Interleaved` error sets include the `Sequential` ones.
pub const ReceiveError = Interleaved.ReceiveError;
pub const ForwardTsnError = Interleaved.ForwardTsnError;
pub const sack_delay = Sequential.sack_delay;

inner: union(enum) {
    data: Sequential,
    idata: Interleaved,
},

pub fn init(capacity: u32) MessageAssembler {
    return .{ .inner = .{ .data = .init(capacity) } };
}

pub fn deinit(self: *MessageAssembler, allocator: std.mem.Allocator) void {
    switch (self.inner) {
        inline else => |*a| a.deinit(allocator),
    }
}

pub fn reset(self: *MessageAssembler, allocator: std.mem.Allocator) void {
    switch (self.inner) {
        inline else => |*a| a.reset(allocator),
    }
}

/// Expect I-DATA chunks if `interleaving`, DATA chunks otherwise. Nothing must have been received yet.
pub fn setInterleaving(self: *MessageAssembler, allocator: std.mem.Allocator, interleaving: bool) void {
    if ((self.inner == .idata) == interleaving) return;
    const capacity = self.getCapacity();
    self.deinit(allocator);
    self.inner = if (interleaving) .{ .idata = .init(capacity) } else .{ .data = .init(capacity) };
}

pub fn setInitTsn(self: *MessageAssembler, init_tsn: u32) void {
    switch (self.inner) {
        inline else => |*a| a.setInitTsn(init_tsn),
    }
}

/// `chunk` is a `message.Data` for DATA chunks and a `message.IData` for I-DATA chunks, it must match the active variant.
pub fn receiveData(self: *MessageAssembler, allocator: std.mem.Allocator, chunk: anytype) ReceiveError!bool {
    return switch (@TypeOf(chunk)) {
        message.Data => self.inner.data.receiveData(allocator, chunk),
        message.IData => self.inner.idata.receiveData(allocator, &chunk),
        else => @compileError("expected a DATA or I-DATA chunk"),
    };
}

pub fn acknowledge(self: *MessageAssembler, chunk: anytype) bool {
    return switch (@TypeOf(chunk)) {
        message.Data => self.inner.data.acknowledge(chunk),
        message.IData => self.inner.idata.acknowledge(chunk),
        else => @compileError("expected a DATA or I-DATA chunk"),
    };
}

/// `fwd_tsn` is a `message.ForwardTsn` or a `message.IForwardTsn`, it must match the active variant.
pub fn handleForwardTsn(self: *MessageAssembler, allocator: std.mem.Allocator, fwd_tsn: anytype) ForwardTsnError!void {
    return switch (@TypeOf(fwd_tsn)) {
        message.ForwardTsn => self.inner.data.handleForwardTsn(allocator, &fwd_tsn),
        message.IForwardTsn => self.inner.idata.handleForwardTsn(allocator, &fwd_tsn),
        else => @compileError("expected a FORWARD TSN or I-FORWARD-TSN chunk"),
    };
}

pub fn scheduleSack(self: *MessageAssembler, immediate: bool, now: i64) bool {
    return switch (self.inner) {
        inline else => |*a| a.scheduleSack(immediate, now),
    };
}

pub fn clearDelayedSack(self: *MessageAssembler) void {
    switch (self.inner) {
        inline else => |*a| a.clearDelayedSack(),
    }
}

pub fn writeSack(self: *MessageAssembler, buffer: []u8) u32 {
    return switch (self.inner) {
        inline else => |*a| a.writeSack(buffer),
    };
}

pub fn drainReady(self: *MessageAssembler, allocator: std.mem.Allocator) ?message.UserMessage {
    return switch (self.inner) {
        inline else => |*a| a.drainReady(allocator),
    };
}

pub fn hasReady(self: *const MessageAssembler) bool {
    return switch (self.inner) {
        inline else => |*a| a.ready.len != 0,
    };
}

pub fn getCumulativeTsn(self: *const MessageAssembler) u32 {
    return switch (self.inner) {
        inline else => |*a| a.getCumulativeTsn(),
    };
}

pub fn needsImmediateSack(self: *const MessageAssembler) bool {
    return switch (self.inner) {
        inline else => |*a| a.needsImmediateSack(),
    };
}

pub fn resetStreams(self: *MessageAssembler, allocator: std.mem.Allocator, streams: []const u16) void {
    switch (self.inner) {
        inline else => |*a| a.resetStreams(allocator, streams),
    }
}

pub fn sackDeadline(self: *const MessageAssembler) i64 {
    return switch (self.inner) {
        inline else => |*a| a.sack_deadline,
    };
}

pub fn getCapacity(self: *const MessageAssembler) u32 {
    return switch (self.inner) {
        inline else => |*a| a.capacity,
    };
}
