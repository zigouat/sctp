const std = @import("std");
const testing = std.testing;
const message = @import("message.zig");

const MessageAssembler = @This();
const UserMessage = message.UserMessage;
const SackGenerator = @import("sack_generator.zig");
const Helper = @import("helper.zig");

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
pub const ReceiveError = AllocError || error{NoUserData};

pub const sack_delay = 200;

sack_generator: SackGenerator,
send_sack: bool,
sack_deadline: i64,
pending: std.AutoArrayHashMapUnmanaged(u32, message.Data),
streams: std.AutoHashMapUnmanaged(u16, StreamState),
ready: std.Deque(UserMessage),
capacity: u32,
buffered_data: u32,
advertised_wnd: u32,

pub fn init(capacity: u32) MessageAssembler {
    return .{
        .sack_generator = .init(0),
        .send_sack = false,
        .sack_deadline = std.math.maxInt(i64),
        .pending = .empty,
        .streams = .empty,
        .ready = .empty,
        .capacity = capacity,
        .buffered_data = 0,
        .advertised_wnd = capacity,
    };
}

pub fn deinit(self: *MessageAssembler, allocator: std.mem.Allocator) void {
    for (self.pending.values()) |d| allocator.free(d.user_data);
    self.pending.deinit(allocator);

    var streams_it = self.streams.valueIterator();
    while (streams_it.next()) |state| state.deinit(allocator);
    self.streams.deinit(allocator);

    while (self.ready.popFront()) |msg| msg.deinit(allocator);
    self.ready.deinit(allocator);
}

pub fn reset(self: *MessageAssembler, allocator: std.mem.Allocator) void {
    self.deinit(allocator);
    self.* = .init(self.capacity);
}

pub fn setInitTsn(self: *MessageAssembler, init_tsn: u32) void {
    self.sack_generator = .init(init_tsn);
}

pub fn receiveData(self: *MessageAssembler, allocator: std.mem.Allocator, d: message.Data) ReceiveError!bool {
    if (d.user_data.len == 0) return error.NoUserData;

    self.sack_generator.checkTsn(d.tsn) catch |err| switch (err) {
        error.Duplicate => return true,
        else => return false,
    };

    const window_full = self.advertised_wnd == 0;
    if (window_full) {
        @branchHint(.unlikely);
        if (!self.dropChunkWithHighestTsn(allocator, d)) return true;
    }

    self.sack_generator.receiveTsn(d.tsn);

    const may_complete = try self.storeChunk(allocator, d);
    self.buffered_data += @intCast(d.user_data.len);
    self.advertised_wnd -|= @intCast(d.user_data.len);

    if (may_complete) try self.assembleReadyMessages(allocator, self.streams.getPtr(d.stream_id).?);
    return window_full or d.flags.immediate;
}

pub fn scheduleSack(self: *MessageAssembler, immediate: bool, now: i64) bool {
    if (self.send_sack or immediate or self.hasGaps()) return true;
    self.send_sack = true;
    self.sack_deadline = now + sack_delay;
    return false;
}

pub fn clearDelayedSack(self: *MessageAssembler) void {
    self.send_sack = false;
    self.sack_deadline = std.math.maxInt(i64);
}

pub fn writeSack(self: *MessageAssembler, buffer: []u8) u32 {
    const written = self.sack_generator.writeSack(buffer, self.advertised_wnd);
    self.sack_generator.clearDuplicates();
    return written;
}

pub fn drainReady(self: *MessageAssembler) ?UserMessage {
    if (self.ready.popFront()) |msg| {
        self.buffered_data -= @intCast(msg.data.len);
        self.updateAdvertisedWindow();
        return msg;
    }

    return null;
}

pub fn getCumulativeTsn(self: *const MessageAssembler) u32 {
    return self.sack_generator.cumulative_tsn;
}

pub fn hasGaps(self: *const MessageAssembler) bool {
    return self.sack_generator.cumulative_tsn != self.sack_generator.highest_tsn_received;
}

fn dropChunkWithHighestTsn(self: *MessageAssembler, allocator: std.mem.Allocator, d: message.Data) bool {
    if (Helper.tsnLte(d.tsn, self.sack_generator.cumulative_tsn) or
        Helper.tsnLte(self.sack_generator.highest_tsn_received, d.tsn)) return false;

    var high = self.sack_generator.highest_tsn_received;
    while (Helper.tsnGt(high, d.tsn)) : (high -%= 1) {
        if (!self.dropChunk(allocator, high)) continue;
        self.sack_generator.reneg(high);
        return true;
    }

    return false;
}

fn dropChunk(self: *MessageAssembler, allocator: std.mem.Allocator, tsn: u32) bool {
    if (self.pending.fetchSwapRemove(tsn)) |d| {
        allocator.free(d.value.user_data);
        self.buffered_data -= @intCast(d.value.user_data.len);
        self.updateAdvertisedWindow();
        return true;
    }

    return false;
}

fn storeChunk(self: *MessageAssembler, allocator: std.mem.Allocator, d: message.Data) !bool {
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

fn handleUnorderedChunk(self: *MessageAssembler, allocator: std.mem.Allocator, d: message.Data) !void {
    if (d.isComplete()) {
        self.ready.pushBackAssumeCapacity(d.toUserMessage());
        return;
    }

    self.pending.putAssumeCapacity(d.tsn, d);
    errdefer _ = self.pending.swapRemove(d.tsn);
    if (try self.tryReassemble(allocator, d.tsn)) |msg| self.ready.pushBackAssumeCapacity(msg);
}

fn assembleReadyMessages(self: *MessageAssembler, allocator: std.mem.Allocator, stream_state: *StreamState) !void {
    while (stream_state.seq_to_tsn.get(stream_state.next_ssn)) |tsn| {
        try self.ready.ensureUnusedCapacity(allocator, 1);
        const msg = try self.tryReassemble(allocator, tsn) orelse return;
        self.ready.pushBackAssumeCapacity(msg);
        _ = stream_state.seq_to_tsn.swapRemove(stream_state.next_ssn);
        stream_state.next_ssn +%= 1;
    }
}

fn updateAdvertisedWindow(self: *MessageAssembler) void {
    const avail = self.capacity -| self.buffered_data;
    const thresh = @min(message.PMDCS - message.data_chunk_header_size, self.capacity / 2);

    self.advertised_wnd = @min(avail, self.advertised_wnd);
    if (avail -| self.advertised_wnd >= thresh) {
        self.advertised_wnd = avail;
    }
}

fn tryReassemble(self: *MessageAssembler, allocator: std.mem.Allocator, tsn: u32) !?UserMessage {
    var start_tsn = tsn;
    while (true) {
        const frag = self.pending.getPtr(start_tsn) orelse return null;
        if (frag.flags.start_fragment) break;
        start_tsn -%= 1;
    }

    var end_tsn = start_tsn;
    var total_len: usize = 0;
    while (true) {
        const frag = self.pending.getPtr(end_tsn) orelse return null;
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
        const entry = self.pending.fetchSwapRemove(t).?.value;
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

fn getStreamState(self: *MessageAssembler, allocator: std.mem.Allocator, stream_id: u16) !*StreamState {
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

test "MessageAssembler.deinit: frees pending fragments, buffered reorder entries and ready messages" {
    var r: MessageAssembler = .init(4000);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .start_frag, "partial"));
    _ = try r.receiveData(testing.allocator, testData(2, 0, 1, .complete, "buffered"));
    _ = try r.receiveData(testing.allocator, testData(3, 1, 0, .complete, "ready"));

    r.deinit(testing.allocator);
}

test "MessageAssembler.receiveData: single-chunk unordered message is delivered immediately" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .unordered_complete, "hello"));

    const msg = r.drainReady().?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", msg.data);
    try testing.expect(r.drainReady() == null);
}

test "MessageAssembler.receiveData: single-chunk ordered message with ssn 0 is delivered immediately" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .complete, "hello"));

    const msg = r.drainReady().?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("hello", msg.data);
    try testing.expect(r.drainReady() == null);
}

test "MessageAssembler.receiveData: unordered message bypasses a pending ordered reorder buffer" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 1, .complete, "ordered-second"));
    try testing.expect(r.drainReady() == null);

    _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .unordered_complete, "unordered"));
    const msg = r.drainReady().?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("unordered", msg.data);

    // the ordered ssn-1 message is still pending, unaffected by the unordered delivery
    try testing.expect(r.drainReady() == null);
}

test "MessageAssembler.receiveData: zero receive window drops or reneg chunks" {
    var r = MessageAssembler.init(100);
    defer r.deinit(testing.allocator);
    const tsn = 0x12345678;
    r.setInitTsn(tsn);

    const payload: [100]u8 = @splat(0xAA);

    // ssn 1 waits for ssn 0
    try testing.expect(!try r.receiveData(testing.allocator, testData(tsn +% 1, 1, 1, .complete, payload[0..60])));
    try testing.expect(r.drainReady() == null);
    try testing.expect(r.pending.contains(tsn +% 1));
    try testing.expectEqual(40, r.advertised_wnd);

    // unordered message is delivered right away and closes the window
    try testing.expect(!try r.receiveData(testing.allocator, testData(tsn +% 3, 1, 0, .unordered_complete, payload[0..40])));
    try testing.expectEqual(0, r.advertised_wnd);
    try testing.expectEqual(tsn +% 3, r.sack_generator.highest_tsn_received);

    // a tsn above the highest one is dropped
    try testing.expect(try r.receiveData(testing.allocator, testData(tsn +% 4, 1, 0, .complete, payload[0..40])));
    try testing.expectEqual(tsn +% 3, r.sack_generator.highest_tsn_received);

    // the higher tsns are already delivered or never received, nothing to reneg
    try testing.expect(try r.receiveData(testing.allocator, testData(tsn +% 2, 1, 0, .complete, payload[0..40])));
    try testing.expectEqual(tsn +% 3, r.sack_generator.highest_tsn_received);
    try testing.expectEqual(tsn -% 1, r.sack_generator.cumulative_tsn);

    // ssn 1 held for reordering is reneged to accept ssn 0
    try testing.expect(try r.receiveData(testing.allocator, testData(tsn, 1, 0, .complete, payload[0..10])));
    try testing.expect(!r.pending.contains(tsn +% 1));
    try testing.expectEqual(tsn, r.sack_generator.cumulative_tsn);
    try testing.expectEqual(tsn +% 3, r.sack_generator.highest_tsn_received);
}

test "MessageAssembler.drainReady: re-assemble messages" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    {
        _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .start_frag, "hel"));
        try testing.expect(r.drainReady() == null);

        _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .end_frag, "lo"));
        const msg = r.drainReady().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings("hello", msg.data);
    }

    {
        _ = try r.receiveData(testing.allocator, testData(4, 0, 1, .end_frag, "lo"));
        try testing.expect(r.drainReady() == null);

        _ = try r.receiveData(testing.allocator, testData(3, 0, 1, .start_frag, "hel"));
        const msg = r.drainReady().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings("hello", msg.data);
    }

    {
        _ = try r.receiveData(testing.allocator, testData(5, 1, 0, .start_frag, "he"));
        _ = try r.receiveData(testing.allocator, testData(7, 1, 0, .end_frag, "lo"));
        try testing.expect(r.drainReady() == null);

        _ = try r.receiveData(testing.allocator, testData(6, 1, 0, .middle_frag, "l"));
        const msg = r.drainReady().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings("hello", msg.data);
    }

    {
        _ = try r.receiveData(testing.allocator, testData(8, 2, 0, .unordered_start_frag, "hel"));
        try testing.expect(r.drainReady() == null);

        _ = try r.receiveData(testing.allocator, testData(9, 2, 0, .unordered_end_frag, "lo"));
        const msg = r.drainReady().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings("hello", msg.data);
    }
}

test "MessageAssembler.drainReady: re-ordered messages are drained in order" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, testData(3, 0, 2, .complete, "third"));
    _ = try r.receiveData(testing.allocator, testData(2, 0, 1, .complete, "second"));
    try testing.expect(r.drainReady() == null);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .complete, "first"));

    for ([_][]const u8{ "first", "second", "third" }) |expected| {
        const msg = r.drainReady().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings(expected, msg.data);
    }

    try testing.expect(r.drainReady() == null);
}

test "MessageAssembler: independent streams track ssn separately" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, testData(1, 1, 0, .complete, "stream1"));
    _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .complete, "stream0"));

    const a = r.drainReady().?;
    defer a.deinit(testing.allocator);
    try testing.expectEqualStrings("stream1", a.data);

    const b = r.drainReady().?;
    defer b.deinit(testing.allocator);
    try testing.expectEqualStrings("stream0", b.data);
}

test "MessageAssembler: ssn wraparound at 0xFFFF advances to 0" {
    var r: MessageAssembler = .init(4000);
    defer r.deinit(testing.allocator);

    try r.streams.put(testing.allocator, 0, .{ .next_ssn = 0xFFFF, .seq_to_tsn = .empty });

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0xFFFF, .complete, "last"));
    const first = r.drainReady().?;
    defer first.deinit(testing.allocator);
    try testing.expectEqualStrings("last", first.data);
    try testing.expectEqual(@as(u16, 0), r.streams.get(0).?.next_ssn);

    _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .complete, "wrapped"));
    const second = r.drainReady().?;
    defer second.deinit(testing.allocator);
    try testing.expectEqualStrings("wrapped", second.data);
}

test "MessageAssembler: advertised window" {
    var r = MessageAssembler.init(4000);
    defer r.deinit(testing.allocator);

    try testing.expectEqual(4000, r.advertised_wnd);

    const data_chunk: [1000]u8 = @splat(0xBB);
    _ = try r.receiveData(testing.allocator, testData(0, 0, 0, .unordered_start_frag, &data_chunk));
    try testing.expectEqual(3000, r.advertised_wnd);

    _ = try r.receiveData(testing.allocator, testData(3, 0, 1, .start_frag, &data_chunk));
    try testing.expectEqual(2000, r.advertised_wnd);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .unordered_end_frag, &data_chunk));
    try testing.expectEqual(1000, r.advertised_wnd);

    const msg = r.drainReady().?;
    msg.deinit(testing.allocator);
    try testing.expectEqual(3000, r.advertised_wnd);

    _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .complete, data_chunk[0..100]));
    try testing.expectEqual(2900, r.advertised_wnd);

    const msg2 = r.drainReady().?;
    msg2.deinit(testing.allocator);
    try testing.expectEqual(2900, r.advertised_wnd);
}

test "MessageAssembler.receiveData: allocation failure" {
    try testing.checkAllAllocationFailures(testing.allocator, struct {
        pub fn run(alloc: std.mem.Allocator) !void {
            var r: MessageAssembler = .init(4000);
            defer r.deinit(alloc);

            _ = try r.receiveData(alloc, testData(1, 0, 0, .start_frag, "hel"));
            _ = try r.receiveData(alloc, testData(2, 0, 0, .end_frag, "lo"));
        }
    }.run, .{});

    try testing.checkAllAllocationFailures(testing.allocator, struct {
        pub fn run(alloc: std.mem.Allocator) !void {
            var r: MessageAssembler = .init(4000);
            defer r.deinit(alloc);

            _ = try r.receiveData(alloc, testData(2, 0, 0, .complete, "Hello Zig!"));
        }
    }.run, .{});

    try testing.checkAllAllocationFailures(testing.allocator, struct {
        pub fn run(alloc: std.mem.Allocator) !void {
            var r: MessageAssembler = .init(4000);
            defer r.deinit(alloc);

            _ = try r.receiveData(alloc, testData(3, 0, 1, .complete, "second"));
            _ = try r.receiveData(alloc, testData(2, 0, 0, .end_frag, "lo"));
            _ = try r.receiveData(alloc, testData(1, 0, 0, .start_frag, "hel"));
            _ = try r.receiveData(alloc, testData(4, 1, 0, .unordered_start_frag, "un"));
            _ = try r.receiveData(alloc, testData(5, 1, 0, .unordered_end_frag, "ordered"));
        }
    }.run, .{});
}

test "MessageAssembler.receiveData: lazy reassembly of ordered messages" {
    var r = MessageAssembler.init(4000);
    defer r.deinit(testing.allocator);

    _ = try r.receiveData(testing.allocator, testData(1, 0, 0, .start_frag, "hel"));
    try testing.expect(r.pending.contains(1));

    _ = try r.receiveData(testing.allocator, testData(3, 0, 1, .complete, "hola"));
    try testing.expect(r.pending.contains(3));

    _ = try r.receiveData(testing.allocator, testData(4, 0, 0, .unordered_complete, "Marhaba"));
    try testing.expect(!r.pending.contains(4));
    try testing.expect(r.ready.len == 1);

    _ = try r.receiveData(testing.allocator, testData(6, 0, 0, .unordered_end_frag, "jour"));
    try testing.expect(r.pending.contains(6));
    try testing.expect(r.ready.len == 1);

    _ = try r.receiveData(testing.allocator, testData(5, 0, 0, .unordered_start_frag, "bon"));
    try testing.expect(!r.pending.contains(5));
    try testing.expect(!r.pending.contains(6));
    try testing.expect(r.ready.len == 2);

    _ = try r.receiveData(testing.allocator, testData(2, 0, 0, .end_frag, "lo"));
    try testing.expect(r.pending.count() == 0);
    try testing.expect(r.ready.len == 4);

    _ = try r.receiveData(testing.allocator, testData(7, 0, 0, .unordered_start_frag, "sa"));
    try testing.expect(r.pending.contains(7));
    try testing.expect(r.ready.len == 4);

    _ = try r.receiveData(testing.allocator, testData(8, 0, 0, .unordered_end_frag, "lut"));
    try testing.expect(r.pending.count() == 0);
    try testing.expect(r.ready.len == 5);

    _ = try r.receiveData(testing.allocator, testData(10, 0, 3, .complete, "ciao"));
    try testing.expect(r.pending.contains(10));
    try testing.expect(r.ready.len == 5);

    _ = try r.receiveData(testing.allocator, testData(9, 0, 2, .complete, "hej"));
    try testing.expect(r.pending.count() == 0);
    try testing.expect(r.ready.len == 7);

    for ([_][]const u8{ "Marhaba", "bonjour", "hello", "hola", "salut", "hej", "ciao" }) |expected| {
        const msg = r.drainReady().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings(expected, msg.data);
    }
}

test "MessageAssembler.dropChunk: drops only chunks held for reordering" {
    var r = MessageAssembler.init(100);
    defer r.deinit(testing.allocator);

    const payload: [60]u8 = @splat(0xAA);
    _ = try r.receiveData(testing.allocator, testData(2, 0, 1, .complete, &payload));
    _ = try r.receiveData(testing.allocator, testData(3, 1, 0, .complete, payload[0..40]));
    try testing.expectEqual(0, r.advertised_wnd);

    // ready messages are not held for reordering
    try testing.expect(!r.dropChunk(testing.allocator, 3));
    try testing.expect(!r.dropChunk(testing.allocator, 4));

    try testing.expect(r.dropChunk(testing.allocator, 2));
    try testing.expect(!r.pending.contains(2));
    try testing.expectEqual(40, r.buffered_data);
    try testing.expectEqual(60, r.advertised_wnd);

    // the dropped chunk can be received again
    r.sack_generator.reneg(2);
    try testing.expect(!try r.receiveData(testing.allocator, testData(2, 0, 1, .complete, &payload)));
    try testing.expect(r.pending.contains(2));
    try testing.expectEqual(0, r.advertised_wnd);
}
