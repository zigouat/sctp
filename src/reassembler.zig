const std = @import("std");
const testing = std.testing;
const message = @import("message.zig");

const Reassembler = @This();
const UserMessage = message.UserMessage;

const StreamState = struct {
    next_ssn: u16,
    seq_to_tsn: std.AutoArrayHashMapUnmanaged(u16, u32),

    const init = StreamState{ .next_ssn = 0, .seq_to_tsn = .empty };

    fn recordTsn(self: *StreamState, allocator: std.mem.Allocator, stream_seq: u16, tsn: u32) !void {
        if (!self.seq_to_tsn.contains(stream_seq)) try self.seq_to_tsn.put(allocator, stream_seq, tsn);
    }

    fn deinit(self: *StreamState, allocator: std.mem.Allocator) void {
        self.seq_to_tsn.deinit(allocator);
    }
};

pub const AllocError = std.mem.Allocator.Error;

pending: std.AutoHashMapUnmanaged(u32, message.Data),
streams: std.AutoHashMapUnmanaged(u16, StreamState),
ready: std.Deque(UserMessage),
capacity: u32,
buffered_data: u32,
advertised_wnd: u32,

pub fn init(capacity: u32) Reassembler {
    return .{
        .pending = .empty,
        .streams = .empty,
        .ready = .empty,
        .capacity = capacity,
        .buffered_data = 0,
        .advertised_wnd = capacity,
    };
}

pub fn deinit(self: *Reassembler, allocator: std.mem.Allocator) void {
    var pending_it = self.pending.valueIterator();
    while (pending_it.next()) |d| allocator.free(d.user_data);
    self.pending.deinit(allocator);

    var streams_it = self.streams.valueIterator();
    while (streams_it.next()) |state| state.deinit(allocator);
    self.streams.deinit(allocator);

    while (self.ready.popFront()) |msg| msg.deinit(allocator);
    self.ready.deinit(allocator);
}

pub fn reset(self: *Reassembler, allocator: std.mem.Allocator) void {
    self.deinit(allocator);
    self.* = .init(self.capacity);
}

pub fn receiveData(self: *Reassembler, allocator: std.mem.Allocator, d: message.Data) AllocError!void {
    const may_complete = try self.storeChunk(allocator, d);
    self.buffered_data += @intCast(d.user_data.len);
    self.advertised_wnd -|= @intCast(d.user_data.len);

    if (may_complete) try self.assembleReadyMessages(allocator, self.streams.getPtr(d.stream_id).?);
}

pub fn drainReady(self: *Reassembler) ?UserMessage {
    if (self.ready.popFront()) |msg| {
        self.buffered_data -= @intCast(msg.data.len);
        self.updateAdvertisedWindow();
        return msg;
    }

    return null;
}

fn storeChunk(self: *Reassembler, allocator: std.mem.Allocator, d: message.Data) !bool {
    try self.ready.ensureUnusedCapacity(allocator, 1);
    try self.pending.ensureUnusedCapacity(allocator, 1);

    var copy = d;
    copy.user_data = try allocator.dupe(u8, d.user_data);
    errdefer allocator.free(copy.user_data);

    if (copy.flags.unordered) {
        try self.handleUnorderedChunk(allocator, copy);
        return false;
    }

    const stream_state = try self.getStreamState(allocator, d.stream_id);
    const is_next = stream_state.next_ssn == d.stream_seq;
    if (is_next and d.isComplete()) {
        stream_state.next_ssn +%= 1;
        self.ready.pushBackAssumeCapacity(copy.toUserMessage());
        return true;
    }

    try stream_state.recordTsn(allocator, d.stream_seq, d.tsn);
    self.pending.putAssumeCapacity(d.tsn, copy);
    return is_next;
}

fn handleUnorderedChunk(self: *Reassembler, allocator: std.mem.Allocator, d: message.Data) !void {
    if (d.isComplete()) {
        self.ready.pushBackAssumeCapacity(d.toUserMessage());
        return;
    }

    self.pending.putAssumeCapacity(d.tsn, d);
    errdefer _ = self.pending.remove(d.tsn);
    if (try self.tryReassemble(allocator, d.tsn)) |msg| self.ready.pushBackAssumeCapacity(msg);
}

fn assembleReadyMessages(self: *Reassembler, allocator: std.mem.Allocator, stream_state: *StreamState) !void {
    while (stream_state.seq_to_tsn.get(stream_state.next_ssn)) |tsn| {
        try self.ready.ensureUnusedCapacity(allocator, 1);
        const msg = try self.tryReassemble(allocator, tsn) orelse return;
        self.ready.pushBackAssumeCapacity(msg);
        _ = stream_state.seq_to_tsn.swapRemove(stream_state.next_ssn);
        stream_state.next_ssn +%= 1;
    }
}

fn updateAdvertisedWindow(self: *Reassembler) void {
    const avail = self.capacity -| self.buffered_data;
    const thres = @min(message.mtu - message.data_chunk_header_size - message.packet_header_size, self.capacity / 2);

    self.advertised_wnd = @min(avail, self.advertised_wnd);
    if (avail -| self.advertised_wnd >= thres) {
        self.advertised_wnd = avail;
    }
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

    return UserMessage{
        .stream_id = first.stream_id,
        .stream_seq = first.stream_seq,
        .unordered = first.flags.unordered,
        .ppid = first.ppid,
        .data = data,
    };
}

fn getStreamState(self: *Reassembler, allocator: std.mem.Allocator, stream_id: u16) !*StreamState {
    const state = try self.streams.getOrPut(allocator, stream_id);
    if (!state.found_existing) state.value_ptr.* = .init;
    return state.value_ptr;
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

test "Reassembler.deinit: frees pending fragments, buffered reorder entries and ready messages" {
    var r: Reassembler = .init(4000);

    try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .start_fragment = true }, "partial"));
    try r.receiveData(testing.allocator, testData(2, 0, 1, .{ .start_fragment = true, .end_fragment = true }, "buffered"));
    try r.receiveData(testing.allocator, testData(3, 1, 0, .{ .start_fragment = true, .end_fragment = true }, "ready"));

    r.deinit(testing.allocator);
}

test "Reassembler.receiveData: single-chunk unordered message is delivered immediately" {
    var r: Reassembler = .init(4000);
    defer r.deinit(testing.allocator);

    try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .start_fragment = true, .end_fragment = true, .unordered = true }, "hello"));

    const msg = r.drainReady().?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", msg.data);
    try testing.expect(r.drainReady() == null);
}

test "Reassembler.receiveData: single-chunk ordered message with ssn 0 is delivered immediately" {
    var r: Reassembler = .init(4000);
    defer r.deinit(testing.allocator);

    try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .start_fragment = true, .end_fragment = true }, "hello"));

    const msg = r.drainReady().?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", msg.data);
    try testing.expect(r.drainReady() == null);
}

test "Reassembler.receiveData: unordered message bypasses a pending ordered reorder buffer" {
    var r: Reassembler = .init(4000);
    defer r.deinit(testing.allocator);

    try r.receiveData(testing.allocator, testData(1, 0, 1, .{ .start_fragment = true, .end_fragment = true }, "ordered-second"));
    try testing.expect(r.drainReady() == null);

    try r.receiveData(testing.allocator, testData(2, 0, 0, .{ .start_fragment = true, .end_fragment = true, .unordered = true }, "unordered"));
    const msg = r.drainReady().?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("unordered", msg.data);

    // the ordered ssn-1 message is still pending, unaffected by the unordered delivery
    try testing.expect(r.drainReady() == null);
}

test "Reassembler.drainReady: re-assemble messages" {
    var r: Reassembler = .init(4000);
    defer r.deinit(testing.allocator);

    {
        try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .start_fragment = true }, "hel"));
        try testing.expect(r.drainReady() == null);

        try r.receiveData(testing.allocator, testData(2, 0, 0, .{ .end_fragment = true }, "lo"));
        const msg = r.drainReady().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings("hello", msg.data);
    }

    {
        try r.receiveData(testing.allocator, testData(4, 0, 1, .{ .end_fragment = true }, "lo"));
        try testing.expect(r.drainReady() == null);

        try r.receiveData(testing.allocator, testData(3, 0, 1, .{ .start_fragment = true }, "hel"));
        const msg = r.drainReady().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings("hello", msg.data);
    }

    {
        try r.receiveData(testing.allocator, testData(5, 1, 0, .{ .start_fragment = true }, "he"));
        try r.receiveData(testing.allocator, testData(7, 1, 0, .{ .end_fragment = true }, "lo"));
        try testing.expect(r.drainReady() == null);

        try r.receiveData(testing.allocator, testData(6, 1, 0, .{}, "l"));
        const msg = r.drainReady().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings("hello", msg.data);
    }

    {
        try r.receiveData(testing.allocator, testData(8, 2, 0, .{ .start_fragment = true, .unordered = true }, "hel"));
        try testing.expect(r.drainReady() == null);

        try r.receiveData(testing.allocator, testData(9, 2, 0, .{ .end_fragment = true, .unordered = true }, "lo"));
        const msg = r.drainReady().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings("hello", msg.data);
    }
}

test "Reassembler.drainReady: re-ordered messages are drained in order" {
    var r: Reassembler = .init(4000);
    defer r.deinit(testing.allocator);

    try r.receiveData(testing.allocator, testData(3, 0, 2, .{ .start_fragment = true, .end_fragment = true }, "third"));
    try r.receiveData(testing.allocator, testData(2, 0, 1, .{ .start_fragment = true, .end_fragment = true }, "second"));
    try testing.expect(r.drainReady() == null);

    try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .start_fragment = true, .end_fragment = true }, "first"));

    for ([_][]const u8{ "first", "second", "third" }) |expected| {
        const msg = r.drainReady().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings(expected, msg.data);
    }

    try testing.expect(r.drainReady() == null);
}

test "Reassembler: independent streams track ssn separately" {
    var r: Reassembler = .init(4000);
    defer r.deinit(testing.allocator);

    try r.receiveData(testing.allocator, testData(1, 1, 0, .{ .start_fragment = true, .end_fragment = true }, "stream1"));
    try r.receiveData(testing.allocator, testData(2, 0, 0, .{ .start_fragment = true, .end_fragment = true }, "stream0"));

    const a = r.drainReady().?;
    defer a.deinit(testing.allocator);
    try testing.expectEqualStrings("stream1", a.data);

    const b = r.drainReady().?;
    defer b.deinit(testing.allocator);
    try testing.expectEqualStrings("stream0", b.data);
}

test "Reassembler: ssn wraparound at 0xFFFF advances to 0" {
    var r: Reassembler = .init(4000);
    defer r.deinit(testing.allocator);

    try r.streams.put(testing.allocator, 0, .{ .next_ssn = 0xFFFF, .seq_to_tsn = .empty });

    try r.receiveData(testing.allocator, testData(1, 0, 0xFFFF, .{ .start_fragment = true, .end_fragment = true }, "last"));
    const first = r.drainReady().?;
    defer first.deinit(testing.allocator);
    try testing.expectEqualStrings("last", first.data);
    try testing.expectEqual(@as(u16, 0), r.streams.get(0).?.next_ssn);

    try r.receiveData(testing.allocator, testData(2, 0, 0, .{ .start_fragment = true, .end_fragment = true }, "wrapped"));
    const second = r.drainReady().?;
    defer second.deinit(testing.allocator);
    try testing.expectEqualStrings("wrapped", second.data);
}

test "Reassembler: advertised window" {
    var r = Reassembler.init(4000);
    defer r.deinit(testing.allocator);

    try testing.expectEqual(4000, r.advertised_wnd);

    const data_chunk: [1000]u8 = @splat(0xBB);
    try r.receiveData(testing.allocator, testData(0, 0, 0, .{ .start_fragment = true, .unordered = true }, &data_chunk));
    try testing.expectEqual(3000, r.advertised_wnd);

    try r.receiveData(testing.allocator, testData(3, 0, 1, .{ .start_fragment = true }, &data_chunk));
    try testing.expectEqual(2000, r.advertised_wnd);

    try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .end_fragment = true, .unordered = true }, &data_chunk));
    try testing.expectEqual(1000, r.advertised_wnd);

    const msg = r.drainReady().?;
    msg.deinit(testing.allocator);
    try testing.expectEqual(3000, r.advertised_wnd);

    try r.receiveData(testing.allocator, testData(2, 0, 0, .{ .start_fragment = true, .end_fragment = true }, data_chunk[0..100]));
    try testing.expectEqual(2900, r.advertised_wnd);

    const msg2 = r.drainReady().?;
    msg2.deinit(testing.allocator);
    try testing.expectEqual(2900, r.advertised_wnd);
}

test "Reassembler.receiveData: allocation failure" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        pub fn run(alloc: std.mem.Allocator) !void {
            var r: Reassembler = .init(4000);
            defer r.deinit(alloc);

            try r.receiveData(alloc, testData(1, 0, 0, .{ .start_fragment = true }, "hel"));
            try r.receiveData(alloc, testData(2, 0, 0, .{ .end_fragment = true }, "lo"));
        }
    }.run, .{});

    try testing.checkAllAllocationFailures(testing.allocator, struct {
        pub fn run(alloc: std.mem.Allocator) !void {
            var r: Reassembler = .init(4000);
            defer r.deinit(alloc);

            try r.receiveData(alloc, testData(2, 0, 0, .{
                .start_fragment = true,
                .end_fragment = true,
            }, "Hello Zig!"));
        }
    }.run, .{});

    try testing.checkAllAllocationFailures(testing.allocator, struct {
        pub fn run(alloc: std.mem.Allocator) !void {
            var r: Reassembler = .init(4000);
            defer r.deinit(alloc);

            try r.receiveData(alloc, testData(3, 0, 1, .{ .start_fragment = true, .end_fragment = true }, "second"));
            try r.receiveData(alloc, testData(2, 0, 0, .{ .end_fragment = true }, "lo"));
            try r.receiveData(alloc, testData(1, 0, 0, .{ .start_fragment = true }, "hel"));
            try r.receiveData(alloc, testData(4, 1, 0, .{ .start_fragment = true, .unordered = true }, "un"));
            try r.receiveData(alloc, testData(5, 1, 0, .{ .end_fragment = true, .unordered = true }, "ordered"));
        }
    }.run, .{});
}

test "Reassembler.receiveData: lazy reassembly of ordered messages" {
    var r = Reassembler.init(4000);
    defer r.deinit(testing.allocator);

    try r.receiveData(testing.allocator, testData(1, 0, 0, .{ .start_fragment = true }, "hel"));
    try testing.expect(r.pending.contains(1));

    try r.receiveData(testing.allocator, testData(3, 0, 1, .{ .start_fragment = true, .end_fragment = true }, "hola"));
    try testing.expect(r.pending.contains(3));

    try r.receiveData(testing.allocator, testData(
        4,
        0,
        0,
        .{ .start_fragment = true, .end_fragment = true, .unordered = true },
        "Marhaba",
    ));
    try testing.expect(!r.pending.contains(4));
    try testing.expect(r.ready.len == 1);

    try r.receiveData(testing.allocator, testData(6, 0, 0, .{ .end_fragment = true, .unordered = true }, "jour"));
    try testing.expect(r.pending.contains(6));
    try testing.expect(r.ready.len == 1);

    try r.receiveData(testing.allocator, testData(5, 0, 0, .{ .start_fragment = true, .unordered = true }, "bon"));
    try testing.expect(!r.pending.contains(5));
    try testing.expect(!r.pending.contains(6));
    try testing.expect(r.ready.len == 2);

    try r.receiveData(testing.allocator, testData(2, 0, 0, .{ .end_fragment = true }, "lo"));
    try testing.expect(r.pending.count() == 0);
    try testing.expect(r.ready.len == 4);

    try r.receiveData(testing.allocator, testData(7, 0, 0, .{ .start_fragment = true, .unordered = true }, "sa"));
    try testing.expect(r.pending.contains(7));
    try testing.expect(r.ready.len == 4);

    try r.receiveData(testing.allocator, testData(8, 0, 0, .{ .end_fragment = true, .unordered = true }, "lut"));
    try testing.expect(r.pending.count() == 0);
    try testing.expect(r.ready.len == 5);

    try r.receiveData(testing.allocator, testData(10, 0, 3, .{ .start_fragment = true, .end_fragment = true }, "ciao"));
    try testing.expect(r.pending.contains(10));
    try testing.expect(r.ready.len == 5);

    try r.receiveData(testing.allocator, testData(9, 0, 2, .{ .start_fragment = true, .end_fragment = true }, "hej"));
    try testing.expect(r.pending.count() == 0);
    try testing.expect(r.ready.len == 7);

    for ([_][]const u8{ "Marhaba", "bonjour", "hello", "hola", "salut", "hej", "ciao" }) |expected| {
        const msg = r.drainReady().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings(expected, msg.data);
    }
}
