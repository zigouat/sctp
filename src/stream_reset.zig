const std = @import("std");
const message = @import("message.zig");
const Helper = @import("helper.zig");
const MessageQueue = @import("message_queue.zig");

const StreamReset = @This();
const Allocator = std.mem.Allocator;

/// A RE-CONFIG chunk bundling a Re-configuration Response and an Outgoing SSN Reset Request must fit in a packet.
const max_streams_per_request = (message.PMDCS - 4 - 12 - 16) / 2;

const Request = struct {
    streams: []u16,
    seq_num: u32,
    /// Set on the first send, a retransmission must carry the same value.
    last_assigned_tsn: ?u32,
};

const Response = struct {
    seq_num: u32,
    result: message.ReConfigResponse.Result,
};

const Deferred = struct {
    streams: []u16,
    last_assigned_tsn: u32,
};

/// Streams with a queued or in-flight outgoing reset.
streams: std.AutoHashMapUnmanaged(u16, void),
/// Messages written to resetting streams, held until the reset completes.
pending_messages: std.ArrayList(message.UserMessage),
/// Only the front request is in flight (RFC6525 - 5.1.1).
requests: std.Deque(Request),
req_seq_num: u32,
/// Sequence number of the last processed peer request, starts at the peer's initial TSN - 1 (RFC6525 - 4.1).
resp_seq_num: u32,
response: Response,
send_response: bool,
/// Peer reset performed once the cumulative TSN reaches the peer's last assigned TSN (RFC6525 - 5.2.2).
deferred: ?Deferred,
deadline: i64,
retransmit: bool,

pub fn init(initial_tsn: u32) StreamReset {
    return .{
        .streams = .empty,
        .pending_messages = .empty,
        .requests = .empty,
        .req_seq_num = initial_tsn,
        .resp_seq_num = 0,
        .response = .{ .seq_num = 0, .result = .success_nothing_to_do },
        .send_response = false,
        .deferred = null,
        .deadline = std.math.maxInt(i64),
        .retransmit = false,
    };
}

pub fn deinit(self: *StreamReset, allocator: Allocator) void {
    self.streams.deinit(allocator);
    for (self.pending_messages.items) |msg| msg.deinit(allocator);
    self.pending_messages.deinit(allocator);
    while (self.requests.popFront()) |request| allocator.free(request.streams);
    self.requests.deinit(allocator);
    if (self.deferred) |deferred| allocator.free(deferred.streams);
}

pub fn setPeerInitTsn(self: *StreamReset, init_tsn: u32) void {
    self.resp_seq_num = init_tsn -% 1;
}

/// Queue an outgoing reset of `streams`, streams that are already resetting are skipped.
pub fn resetStreams(self: *StreamReset, allocator: Allocator, streams: []const u16) Allocator.Error!void {
    try self.streams.ensureUnusedCapacity(allocator, @intCast(streams.len));
    const new_streams = try allocator.alloc(u16, streams.len);
    defer allocator.free(new_streams);

    var count: u32 = 0;
    for (streams) |stream| {
        if (self.streams.contains(stream)) continue;
        self.streams.putAssumeCapacity(stream, {});
        new_streams[count] = stream;
        count += 1;
    }
    errdefer for (new_streams[0..count]) |stream| {
        _ = self.streams.remove(stream);
    };
    if (count == 0) return;

    const request_count = std.math.divCeil(u32, count, max_streams_per_request) catch unreachable;
    try self.requests.ensureUnusedCapacity(allocator, request_count);

    var pushed: u32 = 0;
    errdefer for (0..pushed) |_| allocator.free(self.requests.popBack().?.streams);

    var start: u32 = 0;
    while (start < count) : (start += max_streams_per_request) {
        const end = @min(start + max_streams_per_request, count);
        self.requests.pushBackAssumeCapacity(.{
            .streams = try allocator.dupe(u16, new_streams[start..end]),
            .seq_num = self.req_seq_num +% pushed,
            .last_assigned_tsn = null,
        });
        pushed += 1;
    }
    self.req_seq_num +%= pushed;
}

/// Buffer a message written to a resetting stream, the `data` is copied internally.
///
/// Returns `false` if the stream is not resetting, the message is not buffered and can be sent right away.
pub fn bufferMessage(self: *StreamReset, allocator: Allocator, data: []const u8, config: message.UserMessageConfig) Allocator.Error!bool {
    if (self.streams.size == 0 or !self.streams.contains(config.stream_id)) {
        @branchHint(.likely);
        return false;
    }

    try self.pending_messages.append(allocator, .{
        .data = try allocator.dupe(u8, data),
        .ppid = config.ppid,
        .stream_id = config.stream_id,
        .stream_seq = 0,
        .unordered = config.unordered,
    });

    return true;
}

/// Pop the oldest buffered message whose stream finished resetting, the caller owns the message.
pub fn popMessage(self: *StreamReset) ?message.UserMessage {
    for (self.pending_messages.items, 0..) |msg, index| {
        if (!self.streams.contains(msg.stream_id)) return self.pending_messages.orderedRemove(index);
    }
    return null;
}

pub fn maybeStartTransmitting(self: *StreamReset, msg_queue: *const MessageQueue) void {
    const request = self.requests.frontPtr() orelse return;
    if (request.last_assigned_tsn != null) return;

    // Messages before `curr_msg` are fully assigned TSNs, the one at `curr_msg` may be partially sent.
    for (msg_queue.curr_msg..msg_queue.messages.len) |index| {
        if (self.streams.contains(msg_queue.messages.atPtr(index).stream_id)) return;
    }
    request.last_assigned_tsn = msg_queue.curr_tsn -% 1;
    self.retransmit = true;
}

pub const ResponseResult = struct {
    streams: []u16,
    succeeded: bool,
};

pub fn handleResponse(self: *StreamReset, response: *const message.ReConfigResponse) ?ResponseResult {
    const request = self.requests.frontPtr() orelse return null;
    if (request.last_assigned_tsn == null or response.resp_seq_num != request.seq_num) return null;

    const success = switch (response.result) {
        .success_nothing_to_do, .success_performed => true,
        .in_progress => return null,
        else => false,
    };

    const streams = self.requests.popFront().?.streams;
    for (streams) |stream| _ = self.streams.remove(stream);
    self.deadline = std.math.maxInt(i64);
    self.retransmit = false;
    return .{
        .streams = streams,
        .succeeded = success,
    };
}

/// Handle the peer's Outgoing SSN Reset Request.
///
/// Returns the streams whose incoming stream sequence number must restart at 0, an empty slice means all the streams.
/// The caller owns the returned slice.
pub fn handleOutgoingResetRequest(
    self: *StreamReset,
    allocator: Allocator,
    request: *const message.OutgoingSsnResetRequest,
    cumulative_tsn: u32,
) Allocator.Error!?[]u16 {
    // RFC6525 - 5.2.1. a retransmitted request gets the same answer, processed requests are either performed or deferred
    if (request.req_seq_num == self.resp_seq_num) {
        self.setResponse(self.resp_seq_num, if (self.deferred != null) .in_progress else .success_performed);
        return null;
    }

    if (request.req_seq_num != self.resp_seq_num +% 1) {
        self.setResponse(request.req_seq_num, .bad_sequence_number);
        return null;
    }

    if (self.deferred != null) {
        self.setResponse(request.req_seq_num, .request_in_progress);
        return null;
    }

    const streams = try allocator.alloc(u16, request.countStreams());
    for (streams, 0..) |*stream, index| stream.* = request.getStreamAt(@intCast(index));
    self.resp_seq_num = request.req_seq_num;

    if (Helper.tsnLte(request.last_assigned_tsn, cumulative_tsn)) {
        self.setResponse(self.resp_seq_num, .success_performed);
        return streams;
    }

    self.deferred = .{ .streams = streams, .last_assigned_tsn = request.last_assigned_tsn };
    self.setResponse(self.resp_seq_num, .in_progress);
    return null;
}

/// Complete the deferred peer reset once all the data sent before the request is received. The caller owns the returned
/// streams.
///
/// Data the peer sends on the streams after the request is not held back, the peer must wait for the response before
/// sending on them again.
pub fn handleCumulativeTsn(self: *StreamReset, cumulative_tsn: u32) ?[]u16 {
    const deferred = self.deferred orelse return null;
    if (!Helper.tsnLte(deferred.last_assigned_tsn, cumulative_tsn)) return null;

    self.deferred = null;
    self.setResponse(self.resp_seq_num, .success_performed);
    return deferred.streams;
}

pub fn handleTimeout(self: *StreamReset, now: i64) void {
    if (now < self.deadline) return;
    self.deadline = std.math.maxInt(i64);
    self.retransmit = true;
}

pub fn pollTimeout(self: *const StreamReset) i64 {
    return self.deadline;
}

/// Write a RE-CONFIG chunk bundling the pending response and our outgoing request (RFC6525 - 3.1).
pub fn pollTransmit(self: *StreamReset, buffer: []u8, now: i64, rto: u32) error{WriteFailed}!?u32 {
    const request = self.requestToSend();
    if (request == null and !self.send_response) return null;

    const response_length: u32 = if (self.send_response) 12 else 0;
    const request_length: u32 = if (request) |req| 16 + 2 * @as(u32, @intCast(req.streams.len)) else 0;
    const length = 4 + response_length + request_length;
    const size = std.mem.alignForward(u32, length, 4);
    if (buffer.len < size) return error.WriteFailed;

    buffer[0] = @intFromEnum(message.ChunkType.reconfig);
    buffer[1] = 0; // flags
    std.mem.writeInt(u16, buffer[2..4], @intCast(length), .big);

    if (self.send_response) {
        writeResponse(self.response, buffer[4..]);
        self.send_response = false;
    }

    if (request) |req| {
        writeOutgoingRequest(req, self.resp_seq_num, buffer[4 + response_length .. size]);
        self.deadline = now + rto;
        self.retransmit = false;
    }

    return size;
}

fn requestToSend(self: *const StreamReset) ?*const Request {
    const request = self.requests.frontPtr() orelse return null;
    return if (request.last_assigned_tsn != null and self.retransmit) request else null;
}

/// `buffer` holds exactly the parameter and its padding.
fn writeOutgoingRequest(req: *const Request, resp_seq_num: u32, buffer: []u8) void {
    const param_length: u32 = 16 + 2 * @as(u32, @intCast(req.streams.len));

    std.mem.writeInt(u16, buffer[0..2], @intFromEnum(message.ParameterType.outgoing_ssn_reset_req), .big);
    std.mem.writeInt(u16, buffer[2..4], @intCast(param_length), .big);
    std.mem.writeInt(u32, buffer[4..8], req.seq_num, .big);
    std.mem.writeInt(u32, buffer[8..12], resp_seq_num, .big);
    std.mem.writeInt(u32, buffer[12..16], req.last_assigned_tsn.?, .big);
    for (req.streams, 0..) |stream, index| std.mem.writeInt(u16, buffer[16 + 2 * index ..][0..2], stream, .big);
    @memset(buffer[param_length..], 0);
}

fn setResponse(self: *StreamReset, seq_num: u32, result: message.ReConfigResponse.Result) void {
    self.response = .{ .seq_num = seq_num, .result = result };
    self.send_response = true;
}

fn writeResponse(response: Response, buffer: []u8) void {
    std.mem.writeInt(u16, buffer[0..2], @intFromEnum(message.ParameterType.reconfig_response), .big);
    std.mem.writeInt(u16, buffer[2..4], 12, .big);
    std.mem.writeInt(u32, buffer[4..8], response.seq_num, .big);
    std.mem.writeInt(u32, buffer[8..12], @intFromEnum(response.result), .big);
}

const testing = std.testing;

fn testParseRequest(chunk: []const u8) !message.OutgoingSsnResetRequest {
    var it = message.ChunkIterator.init(chunk);
    var params = (try it.next()).?.reconfig;
    try testing.expectEqual(null, try it.next());

    const request = (try params.next()).?.outgoing_ssn_reset_req;
    try testing.expectEqual(null, try params.next());
    return request;
}

fn testExpectStreams(request: message.OutgoingSsnResetRequest, streams: []const u16) !void {
    try testing.expectEqual(streams.len, request.countStreams());
    for (streams, 0..) |stream, index| try testing.expectEqual(stream, request.getStreamAt(@intCast(index)));
}

fn testResponse(seq_num: u32, result: message.ReConfigResponse.Result) message.ReConfigResponse {
    return .{ .resp_seq_num = seq_num, .result = result, .sender_next_tsn = null, .receiver_next_tsn = null };
}

fn testPushMessage(r: *StreamReset, stream_id: u16, data: []const u8) !bool {
    return r.bufferMessage(testing.allocator, data, .{ .stream_id = stream_id, .ppid = 51 });
}

/// Start transmitting with an empty message queue whose last assigned tsn is `last_assigned_tsn`.
fn testStart(r: *StreamReset, last_assigned_tsn: u32) void {
    const msg_queue = MessageQueue.init(last_assigned_tsn +% 1);
    r.maybeStartTransmitting(&msg_queue);
}

fn testQueue(last_assigned_tsn: u32, streams: []const u16) !MessageQueue {
    var msg_queue = MessageQueue.init(last_assigned_tsn +% 1);
    errdefer msg_queue.deinit(testing.allocator);
    for (streams) |stream| try msg_queue.pushData(testing.allocator, "data", .{ .stream_id = stream, .ppid = 51 });
    return msg_queue;
}

test "StreamReset.pollTransmit: writes an outgoing ssn reset request" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    r.setPeerInitTsn(500);

    var buffer: [64]u8 = undefined;
    try testing.expectEqual(null, try r.pollTransmit(&buffer, 0, 1000));

    // Not started yet
    try r.resetStreams(testing.allocator, &.{ 1, 2, 3 });
    try testing.expectEqual(null, try r.pollTransmit(&buffer, 0, 1000));

    testStart(&r, 10);
    const written = (try r.pollTransmit(&buffer, 0, 1000)).?;
    // 4 bytes chunk header + 16 bytes parameter header + 3 streams, padded
    try testing.expectEqual(28, written);
    try testing.expectEqual(26, std.mem.readInt(u16, buffer[2..4], .big));

    const request = try testParseRequest(buffer[0..written]);
    try testing.expectEqual(100, request.req_seq_num);
    try testing.expectEqual(499, request.resp_seq_num);
    try testing.expectEqual(10, request.last_assigned_tsn);
    try testExpectStreams(request, &.{ 1, 2, 3 });

    // Already sent, nothing to do until a response or a timeout
    try testing.expectEqual(null, try r.pollTransmit(&buffer, 0, 1000));
}

test "StreamReset.maybeStartTransmitting: waits for the queued messages of the resetting streams" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    var buffer: [64]u8 = undefined;

    try r.resetStreams(testing.allocator, &.{ 1, 2 });

    {
        var msg_queue = try testQueue(10, &.{ 3, 2 });
        defer msg_queue.deinit(testing.allocator);
        r.maybeStartTransmitting(&msg_queue);
        try testing.expectEqual(null, try r.pollTransmit(&buffer, 0, 1000));
    }

    // Messages of other streams don't block the request
    {
        var msg_queue = try testQueue(20, &.{3});
        defer msg_queue.deinit(testing.allocator);
        r.maybeStartTransmitting(&msg_queue);
    }
    var written = (try r.pollTransmit(&buffer, 0, 1000)).?;
    try testing.expectEqual(20, (try testParseRequest(buffer[0..written])).last_assigned_tsn);

    // Once started, the last assigned tsn doesn't change
    testStart(&r, 30);
    r.handleTimeout(1000);
    written = (try r.pollTransmit(&buffer, 1000, 1000)).?;
    try testing.expectEqual(20, (try testParseRequest(buffer[0..written])).last_assigned_tsn);
}

test "StreamReset.maybeStartTransmitting: only messages without tsn block the request" {
    var buffer: [message.PMDCS]u8 = undefined;

    // Messages fully assigned tsns don't block the request
    {
        var r = StreamReset.init(100);
        defer r.deinit(testing.allocator);
        try r.resetStreams(testing.allocator, &.{2});

        var msg_queue = try testQueue(10, &.{2});
        defer msg_queue.deinit(testing.allocator);
        r.maybeStartTransmitting(&msg_queue);
        try testing.expectEqual(null, try r.pollTransmit(&buffer, 0, 1000));

        _ = (try msg_queue.nextChunk(testing.allocator, &buffer)).?;
        r.maybeStartTransmitting(&msg_queue);
        const written = (try r.pollTransmit(&buffer, 0, 1000)).?;
        try testing.expectEqual(11, (try testParseRequest(buffer[0..written])).last_assigned_tsn);
    }

    // A partially sent message still blocks the request
    {
        var r = StreamReset.init(100);
        defer r.deinit(testing.allocator);
        try r.resetStreams(testing.allocator, &.{2});

        var msg_queue = MessageQueue.init(11);
        defer msg_queue.deinit(testing.allocator);
        const data: [2 * message.PMDCS]u8 = @splat(0);
        try msg_queue.pushData(testing.allocator, &data, .{ .stream_id = 2, .ppid = 51 });

        _ = (try msg_queue.nextChunk(testing.allocator, &buffer)).?;
        r.maybeStartTransmitting(&msg_queue);
        try testing.expectEqual(null, try r.pollTransmit(&buffer, 0, 1000));
    }
}

test "StreamReset.resetStreams: only the front request is in flight" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    var buffer: [64]u8 = undefined;

    // Already resetting and duplicated streams are skipped
    try r.resetStreams(testing.allocator, &.{ 1, 2 });
    try r.resetStreams(testing.allocator, &.{ 2, 3, 3 });
    try r.resetStreams(testing.allocator, &.{ 1, 2, 3 });
    try testing.expectEqual(2, r.requests.len);

    testStart(&r, 10);
    var written = (try r.pollTransmit(&buffer, 0, 1000)).?;
    try testExpectStreams(try testParseRequest(buffer[0..written]), &.{ 1, 2 });
    try testing.expectEqual(null, try r.pollTransmit(&buffer, 0, 1000));

    {
        const result = r.handleResponse(&testResponse(100, .success_performed)).?;
        defer testing.allocator.free(result.streams);
    }
    try testing.expectEqual(null, try r.pollTransmit(&buffer, 0, 1000));

    testStart(&r, 20);
    written = (try r.pollTransmit(&buffer, 0, 1000)).?;
    const request = try testParseRequest(buffer[0..written]);
    try testing.expectEqual(101, request.req_seq_num);
    try testing.expectEqual(20, request.last_assigned_tsn);
    try testExpectStreams(request, &.{3});
}

test "StreamReset.resetStreams: large stream lists are split into multiple requests" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);

    var streams: [max_streams_per_request + 1]u16 = undefined;
    for (&streams, 0..) |*stream, index| stream.* = @intCast(index);
    try r.resetStreams(testing.allocator, &streams);

    try testing.expectEqual(2, r.requests.len);
    try testing.expectEqual(max_streams_per_request, r.requests.at(0).streams.len);
    try testing.expectEqual(1, r.requests.at(1).streams.len);
    try testing.expectEqual(102, r.req_seq_num);

    // The largest request bundled with a response fills the packet
    var buffer: [message.PMDCS]u8 = undefined;
    r.setPeerInitTsn(500);
    testStart(&r, 10);
    testing.allocator.free((try testPeerRequest(&r, 500, 10, &.{1}, 10)).?);
    try testing.expectEqual(message.PMDCS, (try r.pollTransmit(&buffer, 0, 1000)).?);
}

test "StreamReset.resetStreams: an allocation failure leaves no partial request" {
    var streams: [max_streams_per_request + 1]u16 = undefined;
    for (&streams, 0..) |*stream, index| stream.* = @intCast(index);

    var fail_index: usize = 0;
    while (true) : (fail_index += 1) {
        var r = StreamReset.init(100);
        defer r.deinit(testing.allocator);
        var failing = std.testing.FailingAllocator.init(testing.allocator, .{ .fail_index = fail_index });

        r.resetStreams(failing.allocator(), &streams) catch {
            try testing.expectEqual(0, r.requests.len);
            try testing.expectEqual(0, r.streams.count());
            try testing.expectEqual(100, r.req_seq_num);
            continue;
        };
        break;
    }
}

test "StreamReset.bufferMessage: messages are held until the reset succeeds" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    var buffer: [64]u8 = undefined;

    try r.resetStreams(testing.allocator, &.{ 1, 2 });
    try testing.expect(!try testPushMessage(&r, 3, "other"));
    try testing.expect(try testPushMessage(&r, 1, "first"));
    try testing.expect(try testPushMessage(&r, 2, "second"));
    try testing.expect(try testPushMessage(&r, 1, "third"));
    try testing.expectEqual(null, r.popMessage());

    testStart(&r, 10);
    _ = try r.pollTransmit(&buffer, 0, 1000);
    const result = r.handleResponse(&testResponse(100, .success_performed)).?;
    defer testing.allocator.free(result.streams);
    try testing.expect(result.succeeded);
    try testing.expectEqualSlices(u16, &.{ 1, 2 }, result.streams);

    for ([_][]const u8{ "first", "second", "third" }) |expected| {
        const msg = r.popMessage().?;
        defer msg.deinit(testing.allocator);
        try testing.expectEqualStrings(expected, msg.data);
    }
    try testing.expectEqual(null, r.popMessage());
    try testing.expect(!try testPushMessage(&r, 1, "after"));
}

test "StreamReset.handleResponse: a refused reset releases the buffered messages" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    var buffer: [64]u8 = undefined;

    try r.resetStreams(testing.allocator, &.{1});
    try testing.expect(try testPushMessage(&r, 1, "data"));
    testStart(&r, 10);
    _ = try r.pollTransmit(&buffer, 0, 1000);
    const result = r.handleResponse(&testResponse(100, .denied)).?;
    defer testing.allocator.free(result.streams);
    try testing.expect(!result.succeeded);
    try testing.expectEqualSlices(u16, &.{1}, result.streams);

    try testing.expectEqual(std.math.maxInt(i64), r.pollTimeout());
    const msg = r.popMessage().?;
    defer msg.deinit(testing.allocator);
    try testing.expectEqualStrings("data", msg.data);
}

test "StreamReset.handleResponse: in progress and unrelated responses keep the request" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    var buffer: [64]u8 = undefined;

    try r.resetStreams(testing.allocator, &.{1});

    // Not started yet
    try testing.expectEqual(null, r.handleResponse(&testResponse(100, .success_performed)));
    try testing.expectEqual(1, r.requests.len);

    testStart(&r, 10);
    _ = try r.pollTransmit(&buffer, 0, 1000);
    try testing.expectEqual(null, r.handleResponse(&testResponse(99, .success_performed)));
    try testing.expectEqual(null, r.handleResponse(&testResponse(100, .in_progress)));
    try testing.expectEqual(1, r.requests.len);
    try testing.expectEqual(1000, r.pollTimeout());
}

test "StreamReset.handleTimeout: retransmits the same request" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    var buffer: [64]u8 = undefined;

    try testing.expectEqual(std.math.maxInt(i64), r.pollTimeout());
    try r.resetStreams(testing.allocator, &.{ 1, 2 });
    testStart(&r, 10);
    const written = (try r.pollTransmit(&buffer, 0, 1000)).?;
    const first = buffer;
    try testing.expectEqual(1000, r.pollTimeout());

    r.handleTimeout(999);
    try testing.expectEqual(null, try r.pollTransmit(&buffer, 999, 1000));

    r.handleTimeout(1000);
    try testing.expectEqual(std.math.maxInt(i64), r.pollTimeout());
    try testing.expectEqual(written, (try r.pollTransmit(&buffer, 1000, 2000)).?);
    try testing.expectEqualSlices(u8, first[0..written], buffer[0..written]);
    try testing.expectEqual(3000, r.pollTimeout());
}

test "StreamReset.pollTransmit: fails when the buffer is too small" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);

    try r.resetStreams(testing.allocator, &.{ 1, 2, 3 });
    testStart(&r, 10);
    var buffer: [24]u8 = undefined;
    try testing.expectError(error.WriteFailed, r.pollTransmit(&buffer, 0, 1000));
    try testing.expectEqual(std.math.maxInt(i64), r.pollTimeout());

    var big_buffer: [28]u8 = undefined;
    try testing.expectEqual(28, (try r.pollTransmit(&big_buffer, 0, 1000)).?);
}

fn testPeerRequest(r: *StreamReset, req_seq_num: u32, last_assigned_tsn: u32, streams: []const u16, cumulative_tsn: u32) !?[]u16 {
    var bytes: [16]u8 = undefined;
    for (streams, 0..) |stream, index| std.mem.writeInt(u16, bytes[2 * index ..][0..2], stream, .big);
    const request = message.OutgoingSsnResetRequest{
        .req_seq_num = req_seq_num,
        .resp_seq_num = 0,
        .last_assigned_tsn = last_assigned_tsn,
        .streams = bytes[0 .. 2 * streams.len],
    };
    return r.handleOutgoingResetRequest(testing.allocator, &request, cumulative_tsn);
}

fn testExpectResponse(r: *StreamReset, seq_num: u32, result: message.ReConfigResponse.Result) !void {
    var buffer: [64]u8 = undefined;
    const written = (try r.pollTransmit(&buffer, 0, 1000)).?;
    try testing.expectEqual(16, written);

    var it = message.ChunkIterator.init(buffer[0..written]);
    var params = (try it.next()).?.reconfig;
    const response = (try params.next()).?.reconfig_response;
    try testing.expectEqual(seq_num, response.resp_seq_num);
    try testing.expectEqual(result, response.result);
}

test "StreamReset.handleOutgoingResetRequest: resets the streams when the data before the request is received" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    r.setPeerInitTsn(500);
    var buffer: [64]u8 = undefined;

    const streams = (try testPeerRequest(&r, 500, 10, &.{ 1, 2 }, 10)).?;
    defer testing.allocator.free(streams);
    try testing.expectEqualSlices(u16, &.{ 1, 2 }, streams);
    try testExpectResponse(&r, 500, .success_performed);
    try testing.expectEqual(null, try r.pollTransmit(&buffer, 0, 1000));

    // Our requests carry the last processed peer request
    try r.resetStreams(testing.allocator, &.{3});
    testStart(&r, 20);
    const written = (try r.pollTransmit(&buffer, 0, 1000)).?;
    try testing.expectEqual(500, (try testParseRequest(buffer[0..written])).resp_seq_num);
}

test "StreamReset.pollTransmit: bundles the response with our request" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    r.setPeerInitTsn(500);
    var buffer: [64]u8 = undefined;

    try r.resetStreams(testing.allocator, &.{3});
    testStart(&r, 20);
    testing.allocator.free((try testPeerRequest(&r, 500, 10, &.{1}, 10)).?);

    // 4 bytes chunk header + 12 bytes response + 16 bytes request header + 1 stream, padded
    const written = (try r.pollTransmit(&buffer, 0, 1000)).?;
    try testing.expectEqual(36, written);
    try testing.expectEqual(34, std.mem.readInt(u16, buffer[2..4], .big));
    try testing.expectEqual(@intFromEnum(message.ParameterType.reconfig_response), std.mem.readInt(u16, buffer[4..6], .big));

    var it = message.ChunkIterator.init(buffer[0..written]);
    var params = (try it.next()).?.reconfig;
    try testing.expectEqual(null, try it.next());

    const response = (try params.next()).?.reconfig_response;
    try testing.expectEqual(500, response.resp_seq_num);
    try testing.expectEqual(.success_performed, response.result);

    const request = (try params.next()).?.outgoing_ssn_reset_req;
    try testing.expectEqual(100, request.req_seq_num);
    try testing.expectEqual(500, request.resp_seq_num);
    try testing.expectEqual(20, request.last_assigned_tsn);
    try testExpectStreams(request, &.{3});
    try testing.expectEqual(null, try params.next());

    try testing.expectEqual(null, try r.pollTransmit(&buffer, 0, 1000));
    try testing.expectEqual(1000, r.pollTimeout());
}

test "StreamReset.handleOutgoingResetRequest: an empty stream list resets all the streams" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    r.setPeerInitTsn(500);

    const streams = (try testPeerRequest(&r, 500, 10, &.{}, 10)).?;
    defer testing.allocator.free(streams);
    try testing.expectEqual(0, streams.len);
    try testExpectResponse(&r, 500, .success_performed);
}

test "StreamReset.handleCumulativeTsn: completes a deferred reset" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    r.setPeerInitTsn(500);
    var buffer: [64]u8 = undefined;

    try testing.expectEqual(null, try testPeerRequest(&r, 500, 10, &.{1}, 9));
    try testExpectResponse(&r, 500, .in_progress);

    try testing.expectEqual(null, r.handleCumulativeTsn(9));
    try testing.expectEqual(null, try r.pollTransmit(&buffer, 0, 1000));

    const streams = r.handleCumulativeTsn(10).?;
    defer testing.allocator.free(streams);
    try testing.expectEqualSlices(u16, &.{1}, streams);
    try testExpectResponse(&r, 500, .success_performed);
    try testing.expectEqual(null, r.handleCumulativeTsn(11));
}

test "StreamReset.handleOutgoingResetRequest: retransmitted and unexpected requests" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    r.setPeerInitTsn(500);

    try testing.expectEqual(null, try testPeerRequest(&r, 500, 10, &.{1}, 5));
    try testExpectResponse(&r, 500, .in_progress);

    // Retransmission while deferred
    try testing.expectEqual(null, try testPeerRequest(&r, 500, 10, &.{1}, 5));
    try testExpectResponse(&r, 500, .in_progress);

    // A new request while one is deferred
    try testing.expectEqual(null, try testPeerRequest(&r, 501, 10, &.{2}, 5));
    try testExpectResponse(&r, 501, .request_in_progress);

    try testing.expectEqual(null, try testPeerRequest(&r, 600, 10, &.{2}, 5));
    try testExpectResponse(&r, 600, .bad_sequence_number);

    testing.allocator.free(r.handleCumulativeTsn(10).?);
    try testExpectResponse(&r, 500, .success_performed);

    // Retransmission once performed, the streams are not reset again
    try testing.expectEqual(null, try testPeerRequest(&r, 500, 10, &.{1}, 10));
    try testExpectResponse(&r, 500, .success_performed);
}

test "StreamReset.deinit: frees a deferred reset" {
    var r = StreamReset.init(100);
    defer r.deinit(testing.allocator);
    r.setPeerInitTsn(500);

    try testing.expectEqual(null, try testPeerRequest(&r, 500, 10, &.{ 1, 2 }, 5));
}
