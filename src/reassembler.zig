const std = @import("std");
const testing = std.testing;
const message = @import("message.zig");

const Reassembler = @This();
const UserMessage = message.UserMessage;

const StreamState = struct {
    next_ssn: u16 = 0,
    reorder: std.AutoHashMapUnmanaged(u16, UserMessage) = .empty,

    fn deinit(self: *StreamState, allocator: std.mem.Allocator) void {
        var it = self.reorder.valueIterator();
        while (it.next()) |msg| allocator.free(msg.data);
        self.reorder.deinit(allocator);
    }
};

pending: std.AutoHashMapUnmanaged(u32, message.Data),
streams: std.AutoHashMapUnmanaged(u16, StreamState),
buffered_data: u32,

pub fn init() Reassembler {
    return .{
        .pending = .empty,
        .streams = .empty,
        .buffered_data = 0,
    };
}

pub fn deinit(self: *Reassembler, allocator: std.mem.Allocator) void {
    var pending_it = self.pending.valueIterator();
    while (pending_it.next()) |d| allocator.free(d.user_data);
    self.pending.deinit(allocator);

    var streams_it = self.streams.valueIterator();
    while (streams_it.next()) |state| state.deinit(allocator);
    self.streams.deinit(allocator);
}

pub fn reset(self: *Reassembler, allocator: std.mem.Allocator) void {
    self.deinit(allocator);
    self.* = .init();
}

pub fn receiveData(self: *Reassembler, allocator: std.mem.Allocator, d: message.Data) !?UserMessage {
    if (d.flags.start_fragment and d.flags.end_fragment) {
        const data = try allocator.dupe(u8, d.user_data);
        errdefer allocator.free(data);

        self.buffered_data += @intCast(data.len);
        return self.classify(allocator, .{
            .stream_id = d.stream_id,
            .stream_seq = d.stream_seq,
            .ppid = d.ppid,
            .unordered = d.flags.unordered,
            .data = data,
        });
    }

    var copy = d;
    try self.pending.ensureUnusedCapacity(allocator, 1);
    copy.user_data = try allocator.dupe(u8, d.user_data);
    errdefer allocator.free(copy.user_data);

    self.pending.putAssumeCapacity(d.tsn, copy);
    self.buffered_data += @intCast(copy.user_data.len);

    return self.tryReassemble(allocator, d.tsn);
}

pub fn drainReady(self: *Reassembler, stream_id: u16) ?UserMessage {
    const state = self.streams.getPtr(stream_id) orelse return null;
    const kv = state.reorder.fetchRemove(state.next_ssn) orelse return null;
    state.next_ssn +%= 1;
    self.buffered_data -= @intCast(kv.value.data.len);
    return kv.value;
}

fn tryReassemble(self: *Reassembler, allocator: std.mem.Allocator, tsn: u32) !?UserMessage {
    var start_tsn = tsn;
    while (true) {
        const frag = self.pending.get(start_tsn) orelse return null;
        if (frag.flags.start_fragment) break;
        start_tsn -%= 1;
    }

    var end_tsn = start_tsn;
    var total_len: usize = 0;
    while (true) {
        const frag = self.pending.get(end_tsn) orelse return null;
        total_len += frag.user_data.len;
        if (frag.flags.end_fragment) break;
        end_tsn +%= 1;
    }

    const data = try allocator.alloc(u8, total_len);
    errdefer allocator.free(data);

    var offset: usize = 0;
    var t = start_tsn;
    const first = self.pending.get(start_tsn).?;
    while (true) : (t +%= 1) {
        const entry = self.pending.fetchRemove(t).?.value;
        @memcpy(data[offset .. offset + entry.user_data.len], entry.user_data);
        offset += entry.user_data.len;
        allocator.free(entry.user_data);
        if (entry.flags.end_fragment) break;
    }

    return self.classify(allocator, .{
        .stream_id = first.stream_id,
        .stream_seq = first.stream_seq,
        .ppid = first.ppid,
        .unordered = first.flags.unordered,
        .data = data,
    });
}

fn classify(self: *Reassembler, allocator: std.mem.Allocator, msg: UserMessage) !?UserMessage {
    if (msg.unordered) {
        self.buffered_data -= @intCast(msg.data.len);
        return msg;
    }

    const state = try self.streams.getOrPut(allocator, msg.stream_id);
    if (!state.found_existing) state.value_ptr.* = .{};

    try state.value_ptr.reorder.put(allocator, msg.stream_seq, msg);
    return null;
}

fn testData(tsn: u32, stream_id: u16, stream_seq: u16, flags: message.Data.Flags, payload: []const u8) message.Data {
    return .{
        .flags = flags,
        .tsn = tsn,
        .stream_id = stream_id,
        .stream_seq = stream_seq,
        .ppid = 0,
        .user_data = payload,
    };
}

test "single-chunk unordered message is delivered immediately" {
    var r: Reassembler = .init();
    defer r.deinit(testing.allocator);

    const msg = (try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .start_fragment = true, .end_fragment = true, .unordered = true }, "hello"))).?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", msg.data);
    try testing.expect(r.drainReady(0) == null);
}

test "single-chunk ordered message with ssn 0 is delivered immediately" {
    var r: Reassembler = .init();
    defer r.deinit(testing.allocator);

    try testing.expect(try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .start_fragment = true, .end_fragment = true }, "hello")) == null);

    const msg = r.drainReady(0).?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", msg.data);
    try testing.expect(r.drainReady(0) == null);
}

test "two-fragment message reassembled in arrival order" {
    var r: Reassembler = .init();
    defer r.deinit(testing.allocator);

    try testing.expect(try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .start_fragment = true }, "hel")) == null);
    try testing.expect(r.drainReady(0) == null);

    try testing.expect(try r.receiveData(testing.allocator, testData(2, 0, 0, .{ .end_fragment = true }, "lo")) == null);
    const msg = r.drainReady(0).?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", msg.data);
}

test "two-fragment message reassembled when end arrives before start" {
    var r: Reassembler = .init();
    defer r.deinit(testing.allocator);

    try testing.expect(try r.receiveData(testing.allocator, testData(2, 0, 0, .{ .end_fragment = true }, "lo")) == null);
    try testing.expect(r.drainReady(0) == null);

    try testing.expect(try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .start_fragment = true }, "hel")) == null);
    const msg = r.drainReady(0).?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", msg.data);
}

test "three-fragment message with middle fragment arriving last" {
    var r: Reassembler = .init();
    defer r.deinit(testing.allocator);

    try testing.expect(try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .start_fragment = true }, "he")) == null);
    try testing.expect(try r.receiveData(testing.allocator, testData(3, 0, 0, .{ .end_fragment = true }, "lo")) == null);
    try testing.expect(r.drainReady(0) == null);

    try testing.expect(try r.receiveData(testing.allocator, testData(2, 0, 0, .{}, "l")) == null);
    const msg = r.drainReady(0).?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", msg.data);
}

test "ordered message with ssn 1 is buffered until ssn 0 arrives, then both drain" {
    var r: Reassembler = .init();
    defer r.deinit(testing.allocator);

    try testing.expect(try r.receiveData(testing.allocator, testData(1, 0, 1, .{ .start_fragment = true, .end_fragment = true }, "second")) == null);
    try testing.expect(r.drainReady(0) == null);

    try testing.expect(try r.receiveData(testing.allocator, testData(2, 0, 0, .{ .start_fragment = true, .end_fragment = true }, "first")) == null);

    const first = r.drainReady(0).?;
    defer first.deinit(testing.allocator);
    try testing.expectEqualStrings("first", first.data);

    const second = r.drainReady(0).?;
    defer second.deinit(testing.allocator);
    try testing.expectEqualStrings("second", second.data);

    try testing.expect(r.drainReady(0) == null);
}

test "unordered message bypasses a pending ordered reorder buffer" {
    var r: Reassembler = .init();
    defer r.deinit(testing.allocator);

    try testing.expect(try r.receiveData(testing.allocator, testData(1, 0, 1, .{ .start_fragment = true, .end_fragment = true }, "ordered-second")) == null);
    try testing.expect(r.drainReady(0) == null);

    const msg = (try r.receiveData(testing.allocator, testData(2, 0, 0, .{ .start_fragment = true, .end_fragment = true, .unordered = true }, "unordered"))).?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("unordered", msg.data);

    // the ordered ssn-1 message is still pending, unaffected by the unordered delivery
    try testing.expect(r.drainReady(0) == null);
}

test "independent streams track ssn separately" {
    var r: Reassembler = .init();
    defer r.deinit(testing.allocator);

    try testing.expect(try r.receiveData(testing.allocator, testData(1, 1, 0, .{ .start_fragment = true, .end_fragment = true }, "stream1")) == null);
    try testing.expect(try r.receiveData(testing.allocator, testData(2, 0, 0, .{ .start_fragment = true, .end_fragment = true }, "stream0")) == null);

    const a = r.drainReady(1).?;
    defer a.deinit(testing.allocator);
    try testing.expectEqualStrings("stream1", a.data);

    const b = r.drainReady(0).?;
    defer b.deinit(testing.allocator);
    try testing.expectEqualStrings("stream0", b.data);
}

test "ssn wraparound at 0xFFFF advances to 0" {
    var r: Reassembler = .init();
    defer r.deinit(testing.allocator);

    try r.streams.put(testing.allocator, 0, .{ .next_ssn = 0xFFFF });

    try testing.expect(try r.receiveData(testing.allocator, testData(1, 0, 0xFFFF, .{ .start_fragment = true, .end_fragment = true }, "last")) == null);
    const first = r.drainReady(0).?;
    defer first.deinit(testing.allocator);
    try testing.expectEqualStrings("last", first.data);
    try testing.expectEqual(@as(u16, 0), r.streams.get(0).?.next_ssn);

    try testing.expect(try r.receiveData(testing.allocator, testData(2, 0, 0, .{ .start_fragment = true, .end_fragment = true }, "wrapped")) == null);
    const second = r.drainReady(0).?;
    defer second.deinit(testing.allocator);
    try testing.expectEqualStrings("wrapped", second.data);
}

test "deinit frees pending fragments and buffered reorder entries" {
    var r: Reassembler = .init();

    try testing.expect(try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .start_fragment = true }, "partial")) == null);
    try testing.expect(try r.receiveData(testing.allocator, testData(2, 0, 1, .{ .start_fragment = true, .end_fragment = true }, "buffered")) == null);

    r.deinit(testing.allocator);
}
