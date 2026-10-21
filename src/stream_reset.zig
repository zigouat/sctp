const std = @import("std");
const message = @import("message.zig");
const Helper = @import("helper.zig");
const MessageQueue = @import("message_queue.zig");

const StreamReset = @This();
const Allocator = std.mem.Allocator;

pub const max_streams = 128;

pub const Direction = packed struct(u2) {
    outgoing: bool,
    incoming: bool,

    pub const unset = Direction{ .outgoing = false, .incoming = false };
};

/// Resetting both directions sends an Outgoing and an Incoming SSN Reset Request for the same streams together
/// (RFC6525 - 3.1), the outgoing part uses `seq_num` and the incoming part the next one.
const Request = struct {
    seq_num: u32,
    resp_seq_num: u32,
    last_assigned_tsn: u32,
    stream_count: u8,
    flags: packed struct(u8) {
        direction: Direction,
        done: Direction = .unset,
        /// The last assigned TSN is set, or the request has no outgoing part.
        started: bool = false,
        /// Answers the peer's Incoming SSN Reset Request (RFC6525 - 5.2.3).
        implicit_response: bool,
        _: u2 = 0,
    },

    fn incomingSeqNum(request: *const Request) u32 {
        return request.seq_num +% @intFromBool(request.flags.direction.outgoing);
    }

    fn isOutgoingPending(request: *const Request) bool {
        return request.flags.direction.outgoing and !request.flags.done.outgoing;
    }

    fn isIncomingPending(request: *const Request) bool {
        return request.flags.direction.incoming and !request.flags.done.incoming;
    }

    /// `streams_it` starts at the request's streams, `buffer` holds the parameter and its padding.
    fn writeOutgoingRequest(request: *const Request, streams_it: std.Deque(u16).Iterator, buffer: []u8) void {
        const param_length: u32 = 16 + 2 * @as(u32, request.stream_count);

        std.mem.writeInt(u16, buffer[0..2], @intFromEnum(message.ParameterType.outgoing_ssn_reset_req), .big);
        std.mem.writeInt(u16, buffer[2..4], @intCast(param_length), .big);
        std.mem.writeInt(u32, buffer[4..8], request.seq_num, .big);
        std.mem.writeInt(u32, buffer[8..12], request.resp_seq_num, .big);
        std.mem.writeInt(u32, buffer[12..16], request.last_assigned_tsn, .big);

        var it = streams_it;
        for (0..request.stream_count) |index| std.mem.writeInt(u16, buffer[16 + 2 * index ..][0..2], it.next().?, .big);
        @memset(buffer[param_length..], 0);
    }

    /// `streams_it` starts at the request's streams, `buffer` holds the parameter and its padding.
    fn writeIncomingRequest(request: *const Request, streams_it: std.Deque(u16).Iterator, buffer: []u8) void {
        const param_length: u32 = 8 + 2 * @as(u32, request.stream_count);

        std.mem.writeInt(u16, buffer[0..2], @intFromEnum(message.ParameterType.incoming_ssn_reset_req), .big);
        std.mem.writeInt(u16, buffer[2..4], @intCast(param_length), .big);
        std.mem.writeInt(u32, buffer[4..8], request.incomingSeqNum(), .big);

        var it = streams_it;
        for (0..request.stream_count) |index| std.mem.writeInt(u16, buffer[8 + 2 * index ..][0..2], it.next().?, .big);
        @memset(buffer[param_length..], 0);
    }
};

const Response = struct {
    seq_num: u32,
    result: message.ReConfigResponse.Result,

    fn write(response: *const Response, buffer: []u8) void {
        const reconfig_response = message.Parameter{
            .reconfig_response = .{
                .result = response.result,
                .resp_seq_num = response.seq_num,
                .receiver_next_tsn = null,
                .sender_next_tsn = null,
            },
        };

        _ = message.Parameter.writeBuffer(reconfig_response, buffer) catch unreachable;
    }
};

const Deferred = struct {
    streams: []u16,
    last_assigned_tsn: u32,
    seq_num: u32,
};

const SeqNumCheck = enum { new, retransmission, bad };

/// Streams with a pending outgoing reset, rebuilt from `requests` when an outgoing part completes since a stream may
/// be in several requests.
streams: std.AutoHashMapUnmanaged(u16, void),
/// Streams of the queued requests in order.
request_streams: std.Deque(u16),
/// Set when a request is queued to reset all the streams.
reset_all_streams: bool,
/// Only the front request is in flight (RFC6525 - 5.1.1).
requests: std.Deque(Request),
/// Messages written to resetting streams, held until the reset completes.
pending_messages: std.ArrayList(message.UserMessage),
req_seq_num: u32,
/// Sequence number of the last processed peer request, starts at the peer's initial TSN - 1 (RFC6525 - 4.1).
resp_seq_num: u32,
/// Responses to send, a RE-CONFIG chunk carries two of them at most (RFC6525 - 3.1).
responses: [2]Response,
response_count: u8,
/// Peer reset performed once the cumulative TSN reaches the peer's last assigned TSN (RFC6525 - 5.2.2).
deferred: ?Deferred,
deadline: i64,
retransmit: bool,

pub fn init(initial_tsn: u32) StreamReset {
    return .{
        .streams = .empty,
        .request_streams = .empty,
        .reset_all_streams = false,
        .requests = .empty,
        .pending_messages = .empty,
        .req_seq_num = initial_tsn,
        .resp_seq_num = 0,
        .responses = undefined,
        .response_count = 0,
        .deferred = null,
        .deadline = std.math.maxInt(i64),
        .retransmit = false,
    };
}

pub fn deinit(self: *StreamReset, allocator: Allocator) void {
    self.streams.deinit(allocator);
    self.request_streams.deinit(allocator);
    self.requests.deinit(allocator);
    for (self.pending_messages.items) |msg| msg.deinit(allocator);
    self.pending_messages.deinit(allocator);
    if (self.deferred) |deferred| allocator.free(deferred.streams);
}

pub fn setPeerInitTsn(self: *StreamReset, init_tsn: u32) void {
    self.resp_seq_num = init_tsn -% 1;
}

pub const ResetError = Allocator.Error || error{TooManyStreams};

pub fn resetStreams(self: *StreamReset, allocator: Allocator, streams: []const u16, direction: Direction) ResetError!void {
    if (streams.len > max_streams) return error.TooManyStreams;
    if (direction == Direction.unset) return;
    try self.queueRequest(allocator, streams, direction, self.resp_seq_num, false);
}

/// Buffer a message written to a resetting stream, the `data` is copied internally.
///
/// Returns `false` if the stream is not resetting, the message is not buffered and can be sent right away.
pub fn bufferMessage(self: *StreamReset, allocator: Allocator, data: []const u8, config: message.UserMessageConfig) Allocator.Error!bool {
    if (!self.reset_all_streams and (self.streams.size == 0 or !self.streams.contains(config.stream_id))) {
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
    if (self.reset_all_streams) return null;
    for (self.pending_messages.items, 0..) |msg, index| {
        if (!self.streams.contains(msg.stream_id)) return self.pending_messages.orderedRemove(index);
    }
    return null;
}

pub fn maybeStartTransmitting(self: *StreamReset, msg_queue: *const MessageQueue) void {
    const request = self.requests.frontPtr() orelse return;
    if (request.flags.started) return;

    if (request.flags.direction.outgoing) {
        // Messages before `curr_msg` are fully assigned TSNs, the one at `curr_msg` may be partially sent.
        for (msg_queue.curr_msg..msg_queue.messages.len) |index| {
            if (request.stream_count == 0 or self.streams.contains(msg_queue.messages.atPtr(index).stream_id)) return;
        }
        request.last_assigned_tsn = msg_queue.curr_tsn -% 1;
    }
    request.flags.started = true;
    self.retransmit = true;
}

pub const ResponseResult = struct {
    streams: []u16,
    part: Part,
    succeeded: bool,

    pub const Part = enum { outgoing, incoming };
};

/// Handle the peer's response to a part of our in flight request, the request's streams are copied into `out`.
pub fn handleResponse(self: *StreamReset, response: *const message.ReConfigResponse, out: *[max_streams]u16) ?ResponseResult {
    const request = self.requests.frontPtr() orelse return null;
    if (!request.flags.started) return null;

    const part: ResponseResult.Part = if (request.isOutgoingPending() and response.resp_seq_num == request.seq_num)
        .outgoing
    else if (request.isIncomingPending() and response.resp_seq_num == request.incomingSeqNum())
        .incoming
    else
        return null;

    const succeeded = switch (response.result) {
        .success_nothing_to_do, .success_performed => true,
        .in_progress => return null,
        else => false,
    };

    const streams = out[0..request.stream_count];
    for (streams, 0..) |*stream, index| stream.* = self.request_streams.at(index);

    switch (part) {
        .outgoing => request.flags.done.outgoing = true,
        .incoming => request.flags.done.incoming = true,
    }
    self.completePart();
    if (part == .outgoing) self.rebuildStreams();

    return .{ .streams = streams, .part = part, .succeeded = succeeded };
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
    // The peer's request answers our Incoming SSN Reset Request (RFC6525 - 5.2.3)
    if (self.requests.frontPtr()) |front| {
        if (front.flags.started and front.isIncomingPending() and request.resp_seq_num == front.incomingSeqNum()) {
            front.flags.done.incoming = true;
            self.completePart();
        }
    }

    switch (self.checkSeqNum(request.req_seq_num)) {
        .new => {},
        // Processed requests are either performed or deferred
        .retransmission => {
            const deferred = if (self.deferred) |deferred| deferred.seq_num == request.req_seq_num else false;
            self.pushResponse(request.req_seq_num, if (deferred) .in_progress else .success_performed);
            return null;
        },
        .bad => {
            self.pushResponse(request.req_seq_num, .bad_sequence_number);
            return null;
        },
    }

    if (self.deferred != null) {
        self.pushResponse(request.req_seq_num, .request_in_progress);
        return null;
    }

    const streams = try allocator.alloc(u16, request.countStreams());
    for (streams, 0..) |*stream, index| stream.* = request.getStreamAt(@intCast(index));
    self.resp_seq_num = request.req_seq_num;

    if (Helper.tsnLte(request.last_assigned_tsn, cumulative_tsn)) {
        self.pushResponse(request.req_seq_num, .success_performed);
        return streams;
    }

    self.deferred = .{ .streams = streams, .last_assigned_tsn = request.last_assigned_tsn, .seq_num = request.req_seq_num };
    self.pushResponse(request.req_seq_num, .in_progress);
    return null;
}

/// Handle the peer's Incoming SSN Reset Request by resetting our outgoing streams, the Outgoing SSN Reset Request we send
/// for them is the response (RFC6525 - 5.2.3). An empty stream list resets the streams used in `msg_queue`.
pub fn handleIncomingResetRequest(
    self: *StreamReset,
    allocator: Allocator,
    request: *const message.IncomingSsnResetRequest,
) Allocator.Error!void {
    switch (self.checkSeqNum(request.req_seq_num)) {
        .new => {},
        .retransmission => {
            const queued = self.implicitResponseSeqNum() == request.req_seq_num;
            self.pushResponse(request.req_seq_num, if (queued) .in_progress else .success_performed);
            return;
        },
        .bad => {
            self.pushResponse(request.req_seq_num, .bad_sequence_number);
            return;
        },
    }

    // The peer must wait for our implicit response before sending a new request (RFC6525 - 5.1.1)
    if (self.implicitResponseSeqNum() != null) {
        self.pushResponse(request.req_seq_num, .request_in_progress);
        return;
    }

    const count: u32 = request.countStreams();
    if (count > max_streams) {
        self.resp_seq_num = request.req_seq_num;
        self.pushResponse(request.req_seq_num, .denied);
        return;
    }

    var streams: [max_streams]u16 = undefined;
    for (streams[0..count], 0..) |*stream, index| stream.* = request.getStreamAt(@intCast(index));

    try self.queueRequest(allocator, streams[0..count], .{ .outgoing = true, .incoming = false }, request.req_seq_num, true);
    self.resp_seq_num = request.req_seq_num;
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
    self.pushResponse(deferred.seq_num, .success_performed);
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

/// Write a RE-CONFIG chunk with the pending responses and the pending parts of our in flight request (RFC6525 - 3.1).
pub fn pollTransmit(self: *StreamReset, buffer: []u8, now: i64, rto: u32) error{WriteFailed}!?u32 {
    const request = self.nextRequest();
    if (request == null and self.response_count == 0) return null;

    const send_outgoing, const send_incoming, const streams_size = if (request) |req|
        .{ req.isOutgoingPending(), req.isIncomingPending(), 2 * @as(u32, req.stream_count) }
    else
        .{ false, false, 0 };

    // Parameter sizes including their padding
    const responses_size: u32 = message.reconfig_response_size * @as(u32, self.response_count);
    const outgoing_size: u32 = if (send_outgoing) std.mem.alignForward(u32, 16 + streams_size, 4) else 0;
    const incoming_size: u32 = if (send_incoming) std.mem.alignForward(u32, 8 + streams_size, 4) else 0;
    const size = 4 + responses_size + outgoing_size + incoming_size;
    if (buffer.len < size) return error.WriteFailed;

    // The chunk length excludes the padding of the last parameter (RFC9260 - 3.2), only an odd stream count adds one
    const padding = streams_size % 4;

    buffer[0] = @intFromEnum(message.ChunkType.reconfig);
    buffer[1] = 0; // flags
    std.mem.writeInt(u16, buffer[2..4], @intCast(size - padding), .big);

    var offset: u32 = 4;
    for (self.responses[0..self.response_count]) |*response| {
        response.write(buffer[offset..][0..message.reconfig_response_size]);
        offset += message.reconfig_response_size;
    }
    self.response_count = 0;

    const req = request orelse return size;
    if (send_outgoing) {
        req.writeOutgoingRequest(self.request_streams.iterator(), buffer[offset..][0..outgoing_size]);
        offset += outgoing_size;
    }
    if (send_incoming) req.writeIncomingRequest(self.request_streams.iterator(), buffer[offset..][0..incoming_size]);
    self.deadline = now + rto;
    self.retransmit = false;

    return size;
}

fn queueRequest(
    self: *StreamReset,
    allocator: Allocator,
    streams: []const u16,
    direction: Direction,
    resp_seq_num: u32,
    implicit_response: bool,
) Allocator.Error!void {
    if (direction.outgoing and streams.len != 0) try self.streams.ensureUnusedCapacity(allocator, @intCast(streams.len));
    try self.requests.ensureUnusedCapacity(allocator, 1);
    try self.request_streams.ensureUnusedCapacity(allocator, streams.len);

    if (direction.outgoing) for (streams) |stream| self.streams.putAssumeCapacity(stream, {});
    if (direction.outgoing and streams.len == 0)
        self.reset_all_streams = true
    else
        self.request_streams.pushBackSliceAssumeCapacity(streams);

    self.requests.pushBackAssumeCapacity(.{
        .seq_num = self.req_seq_num,
        .resp_seq_num = resp_seq_num,
        .last_assigned_tsn = undefined,
        .stream_count = @intCast(streams.len),
        .flags = .{ .direction = direction, .implicit_response = implicit_response },
    });
    self.req_seq_num +%= @as(u32, @intFromBool(direction.outgoing)) + @intFromBool(direction.incoming);
}

fn nextRequest(self: *const StreamReset) ?*const Request {
    const request = self.requests.frontPtr() orelse return null;
    if (!request.flags.started or !self.retransmit) return null;
    if (self.response_count == 0) return request;
    // A response is only bundled with an Outgoing SSN Reset Request, and two of them fill the chunk
    return if (self.response_count < self.responses.len and !request.isIncomingPending()) request else null;
}

fn completePart(self: *StreamReset) void {
    const request = self.requests.front().?;
    if (request.isOutgoingPending() or request.isIncomingPending()) return;

    _ = self.requests.popFront();
    for (0..request.stream_count) |_| _ = self.request_streams.popFront();
    self.deadline = std.math.maxInt(i64);
    self.retransmit = false;
}

fn rebuildStreams(self: *StreamReset) void {
    self.streams.clearRetainingCapacity();
    self.reset_all_streams = false;

    var offset: u32 = 0;
    var it = self.requests.iterator();
    while (it.next()) |request| {
        if (request.isOutgoingPending()) {
            if (request.stream_count == 0) {
                self.reset_all_streams = true;
                return;
            }

            for (offset..offset + request.stream_count) |index| {
                self.streams.putAssumeCapacity(self.request_streams.at(index), {});
            }
        }
        offset += request.stream_count;
    }
}

fn implicitResponseSeqNum(self: *const StreamReset) ?u32 {
    var it = self.requests.iterator();
    while (it.next()) |request| if (request.flags.implicit_response) return request.resp_seq_num;
    return null;
}

/// RFC6525 - 5.2.1. a chunk carries up to two requests, a retransmission may repeat both.
fn checkSeqNum(self: *const StreamReset, seq_num: u32) SeqNumCheck {
    if (seq_num == self.resp_seq_num +% 1) return .new;
    if (seq_num == self.resp_seq_num or seq_num == self.resp_seq_num -% 1) return .retransmission;
    return .bad;
}

fn pushResponse(self: *StreamReset, seq_num: u32, result: message.ReConfigResponse.Result) void {
    for (self.responses[0..self.response_count]) |*response| {
        if (response.seq_num != seq_num) continue;
        response.result = result;
        return;
    }

    // The peer retransmits the request whose response is dropped
    if (self.response_count == self.responses.len) {
        self.responses[0] = self.responses[1];
        self.response_count -= 1;
    }
    self.responses[self.response_count] = .{ .seq_num = seq_num, .result = result };
    self.response_count += 1;
}

fn testIsBuffered(self: *StreamReset, stream_id: u16) !bool {
    return self.bufferMessage(std.testing.allocator, "data", .{ .stream_id = stream_id, .ppid = 0 });
}

test "StreamReset.resetStreams: queues a request for the outgoing streams" {
    var stream_reset: StreamReset = .init(10);
    defer stream_reset.deinit(std.testing.allocator);
    stream_reset.setPeerInitTsn(100);

    try stream_reset.resetStreams(std.testing.allocator, &.{ 1, 3 }, .{ .outgoing = true, .incoming = false });

    const request = stream_reset.requests.front().?;
    try std.testing.expectEqual(10, request.seq_num);
    try std.testing.expectEqual(99, request.resp_seq_num);
    try std.testing.expectEqual(2, request.stream_count);
    try std.testing.expectEqual(Direction{ .outgoing = true, .incoming = false }, request.flags.direction);
    try std.testing.expect(!request.flags.started);
    try std.testing.expect(!request.flags.implicit_response);
    try std.testing.expectEqual(11, stream_reset.req_seq_num);

    try std.testing.expect(try testIsBuffered(&stream_reset, 1));
    try std.testing.expect(try testIsBuffered(&stream_reset, 3));
    try std.testing.expect(!try testIsBuffered(&stream_reset, 2));
}

test "StreamReset.resetStreams: both directions use two sequence numbers" {
    var stream_reset: StreamReset = .init(10);
    defer stream_reset.deinit(std.testing.allocator);

    try stream_reset.resetStreams(std.testing.allocator, &.{1}, .{ .outgoing = true, .incoming = true });
    try stream_reset.resetStreams(std.testing.allocator, &.{2}, .{ .outgoing = true, .incoming = false });

    try std.testing.expectEqual(2, stream_reset.requests.len);
    try std.testing.expectEqual(11, stream_reset.requests.front().?.incomingSeqNum());
    try std.testing.expectEqual(12, stream_reset.requests.at(1).seq_num);
    try std.testing.expectEqual(13, stream_reset.req_seq_num);
    try std.testing.expectEqualSlices(u16, &.{ 1, 2 }, &.{ stream_reset.request_streams.at(0), stream_reset.request_streams.at(1) });
}

test "StreamReset.resetStreams: incoming streams do not hold back outgoing messages" {
    var stream_reset: StreamReset = .init(10);
    defer stream_reset.deinit(std.testing.allocator);

    // Specific streams
    try stream_reset.resetStreams(std.testing.allocator, &.{1}, .{ .outgoing = false, .incoming = true });
    try std.testing.expect(!try testIsBuffered(&stream_reset, 1));

    // All the streams
    try stream_reset.resetStreams(std.testing.allocator, &.{}, .{ .outgoing = false, .incoming = true });
    try std.testing.expect(!stream_reset.reset_all_streams);
    try std.testing.expect(!try testIsBuffered(&stream_reset, 1));

    try std.testing.expectEqual(2, stream_reset.requests.len);
    try std.testing.expectEqual(12, stream_reset.req_seq_num);
}

test "StreamReset.resetStreams: an empty stream list resets all the outgoing streams" {
    var stream_reset: StreamReset = .init(10);
    defer stream_reset.deinit(std.testing.allocator);

    try stream_reset.resetStreams(std.testing.allocator, &.{}, .{ .outgoing = true, .incoming = false });

    try std.testing.expect(stream_reset.reset_all_streams);
    try std.testing.expectEqual(0, stream_reset.requests.front().?.stream_count);
    try std.testing.expectEqual(0, stream_reset.request_streams.len);
    try std.testing.expect(try testIsBuffered(&stream_reset, 1));
    try std.testing.expect(try testIsBuffered(&stream_reset, 0xFFFF));
    try std.testing.expectEqual(null, stream_reset.popMessage());
}

test "StreamReset.resetStreams: rejected requests are not queued" {
    var stream_reset: StreamReset = .init(10);
    defer stream_reset.deinit(std.testing.allocator);

    // Too many streams
    {
        const streams: [max_streams + 1]u16 = @splat(1);
        try std.testing.expectError(
            error.TooManyStreams,
            stream_reset.resetStreams(std.testing.allocator, &streams, .{ .outgoing = true, .incoming = false }),
        );
    }

    // No direction
    try stream_reset.resetStreams(std.testing.allocator, &.{1}, .unset);

    try std.testing.expectEqual(0, stream_reset.requests.len);
    try std.testing.expectEqual(10, stream_reset.req_seq_num);
    try std.testing.expect(!try testIsBuffered(&stream_reset, 1));
}

test "StreamReset.resetStreams: accepts the maximum number of streams" {
    var stream_reset: StreamReset = .init(10);
    defer stream_reset.deinit(std.testing.allocator);

    var streams: [max_streams]u16 = undefined;
    for (&streams, 0..) |*stream, index| stream.* = @intCast(index);
    try stream_reset.resetStreams(std.testing.allocator, &streams, .{ .outgoing = true, .incoming = false });

    try std.testing.expectEqual(max_streams, stream_reset.requests.front().?.stream_count);
    try std.testing.expectEqual(max_streams, stream_reset.streams.count());
}
