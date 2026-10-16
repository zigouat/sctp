const std = @import("std");
const message = @import("message.zig");
const MessageAssembler = @import("message_assembler.zig");
const MessageQueue = @import("message_queue.zig");
const Helper = @import("helper.zig");

const Association = @This();
const Io = std.Io;

const max_init_retransmits = 8;
const assoc_max_retransmits = 10;
const max_rto = 60; // in seconds
const max_heartbeat_info = 200;
const initial_rto = std.time.ms_per_s;
/// The minimum buffer size that can be passed to `pollTransmit`.
const min_buffer_size = 128;

pub const Error = error{InvalidState} || std.mem.Allocator.Error;

pub const Config = struct {
    source_port: u16,
    dest_port: u16,
    random: std.Random,
    outbound_streams: u16 = std.math.maxInt(u16),
    inbound_streams: u16 = std.math.maxInt(u16),
    receive_buffer_size: u32 = 1024 * 1024, // 1 MiB
};

pub const Event = union(enum) {
    comm_up: void,
    comm_down: void,
};

pub const MessageIterator = struct {
    assoc: *Association,

    pub fn next(it: *MessageIterator) ?message.UserMessage {
        return it.assoc.message_assembler.drainReady();
    }
};

const InnerState = enum {
    closed,
    established,
    cookie_wait,
    cookie_echoed,
    shutdown_pending,
    shutdown_sent,
    shutdown_received,
    shutdown_ack_sent,
};

const Transmit = union(enum) {
    init,
    init_ack: u32,
    sack,
    cookie_ack,
    abort: u32,
    cookie_echo: []const u8,
    heartbeat_ack: []const u8,
    shutdown: u32,
    shutdown_ack,
    shutdown_complete: bool,
};

const RetransmissionTimer = struct {
    srtt: u32,
    rttvar: u32,
    rto: u16,
    deadline: i64,
    idle_deadline: i64,

    rtt_tsn: ?u32,
    rtt_start: i64,
    first_sample: bool,

    // consecutive expirations, reset when the peer acknowledges new data
    error_count: u8,

    const init = RetransmissionTimer{
        .srtt = 0,
        .rttvar = 0,
        .rto = initial_rto,
        .deadline = std.math.maxInt(i64),
        .idle_deadline = std.math.maxInt(i64),
        .rtt_tsn = null,
        .rtt_start = 0,
        .first_sample = true,
        .error_count = 0,
    };

    fn calculateRto(timer: *RetransmissionTimer, now: i64) void {
        const rtt: u32 = @intCast(now - timer.rtt_start);
        if (timer.first_sample) {
            timer.first_sample = false;
            timer.srtt = rtt;
            timer.rttvar = rtt / 2;
        } else {
            // alpha = 1/8, beta = 1/4
            const abs = @abs(@as(i64, timer.srtt) - rtt);
            timer.rttvar = @intCast((3 * timer.rttvar +| abs) / 4);
            timer.srtt = (7 * timer.srtt +| rtt) / 8;
        }

        timer.rto = @min(max_rto * std.time.ms_per_s, timer.srtt +| 4 *| timer.rttvar);
        timer.rto = @max(timer.rto, initial_rto);
        timer.rtt_tsn = null;
        timer.rtt_start = 0;
    }

    fn clearCurrentSample(timer: *RetransmissionTimer) void {
        timer.rtt_tsn = null;
        timer.rtt_start = 0;
    }

    fn setDeadline(timer: *RetransmissionTimer, now: i64) void {
        if (timer.deadline == std.math.maxInt(i64)) timer.deadline = now + timer.rto;
    }

    fn setSample(timer: *RetransmissionTimer, tsn: u32, now: i64) void {
        if (timer.rtt_tsn != null) return;
        timer.rtt_tsn = tsn;
        timer.rtt_start = now;
    }

    fn restart(timer: *RetransmissionTimer, now: i64) void {
        timer.deadline = now + timer.rto;
    }

    fn stop(timer: *RetransmissionTimer) void {
        timer.deadline = std.math.maxInt(i64);
    }

    fn startIdle(timer: *RetransmissionTimer, now: i64) void {
        timer.idle_deadline = now + timer.rto;
    }

    fn stopIdle(timer: *RetransmissionTimer) void {
        timer.idle_deadline = std.math.maxInt(i64);
    }

    fn backoffAndRestart(timer: *RetransmissionTimer, now: i64) void {
        timer.rto = @min(max_rto * std.time.ms_per_s, timer.rto *| 2);
        timer.restart(now);
    }

    pub fn format(self: @This(), writer: *std.Io.Writer) std.Io.Writer.Error!void {
        try writer.print(
            "srtt: {}, rttvar: {}, rto: {}, t3_deadline: {}, rtt_tsn: {?}, rtt_start: {}",
            .{
                self.srtt,
                self.rttvar,
                self.rto,
                self.deadline,
                self.rtt_tsn,
                self.rtt_start,
            },
        );
    }
};

allocator: std.mem.Allocator,
state: InnerState,
source_port: u16,
dest_port: u16,
outbound_streams: u16,
inbound_streams: u16,
initial_tsn: u32,
verification_tag: u32,
peer_verification_tag: u32,
cookie: [32]u8,
peer_cookie: []const u8,

// Congestion control state
cwnd: u32,
ssthresh: u32,
partial_bytes_acked: u32,

fast_recovery: bool,
fast_retransmit: bool,
fast_recovery_exit_tsn: u32,

init_deadline: i64,
init_rto: u8,
init_attempts: u8,

message_queue: MessageQueue,
message_assembler: MessageAssembler,
transmits: std.Deque(Transmit),
events: std.Deque(Event),

t3_timer: RetransmissionTimer,
/// If `true`, the in-flight chunk is a window probe: it was sent while the window was smaller than its size.
zero_wnd_probe: bool,

pub fn init(allocator: std.mem.Allocator, config: Config) Association {
    const initial_tsn = config.random.int(u32);

    return Association{
        .allocator = allocator,
        .state = .closed,
        .source_port = config.source_port,
        .dest_port = config.dest_port,
        .outbound_streams = config.outbound_streams,
        .inbound_streams = config.inbound_streams,
        .initial_tsn = initial_tsn,
        .verification_tag = config.random.int(u32),
        .peer_verification_tag = 0,
        .cookie = config.random.array(u8, 32),
        .peer_cookie = &.{},
        .cwnd = @min(4 * message.PMDCS, @max(2 * message.PMDCS, 4404)),
        .ssthresh = std.math.maxInt(u32),
        .partial_bytes_acked = 0,
        .fast_recovery = false,
        .fast_retransmit = false,
        .fast_recovery_exit_tsn = 0,
        .transmits = .empty,
        .events = .empty,
        .message_assembler = .init(config.receive_buffer_size),
        .init_attempts = 0,
        .init_rto = 1,
        .init_deadline = std.math.maxInt(i64),
        .message_queue = .init(initial_tsn),
        .t3_timer = .init,
        .zero_wnd_probe = false,
    };
}

pub fn deinit(self: *Association) void {
    self.transmits.deinit(self.allocator);
    self.events.deinit(self.allocator);
    self.message_assembler.deinit(self.allocator);
    self.message_queue.deinit(self.allocator);
    self.allocator.free(self.peer_cookie);
}

pub fn connect(self: *Association) Error!void {
    if (self.state != .closed) return error.InvalidState;
    try self.transmits.pushBack(self.allocator, .init);
    self.peer_verification_tag = 0;
    self.state = .cookie_wait;
}

pub fn shutdown(self: *Association) Error!void {
    if (self.state != .established) return error.InvalidState;
    self.state = .shutdown_pending;
    try self.maybeSendShutdown();
}

/// Write a message to the association.
///
/// The `data` is copied internally.
pub fn handleWrite(self: *Association, data: []const u8, options: message.UserMessageConfig) Error!void {
    std.debug.assert(data.len > 0);
    switch (self.state) {
        .established, .cookie_wait, .cookie_echoed => {},
        else => return error.InvalidState,
    }
    try self.message_queue.pushData(self.allocator, data, options);
}

/// Handle an incoming sctp packet.
///
/// The return value is an iterator over the messages that have been fully reassembled and are ready to be read.
///
/// Back-pressure can be implemented by not reading from the iterator. Reading messages will open the receive window and the
/// association can accept more messages from the sender.
pub fn handleRead(self: *Association, data: []const u8, now: i64) error{ InvalidPacket, OutOfMemory }!?MessageIterator {
    const packet = try message.Packet.parse(data);

    if (packet.source_port != self.dest_port or packet.destination_port != self.source_port) {
        @branchHint(.unlikely);
        return null;
    }

    if (!self.isValidVerificationTag(&packet)) {
        @branchHint(.unlikely);
        return null;
    }

    var chunk_it = packet.iterateChunks();
    var received_data = false;
    var immediate_sack = false;
    while (true) {
        const chunk = chunk_it.next() catch |err| switch (err) {
            error.InvalidChunk => break,
            error.UnrecognizedChunkType => break, // send error chunk
        } orelse break;

        switch (chunk) {
            .init => |c| try self.handleInitChunk(c),
            .init_ack => |c| switch (self.state) {
                .cookie_wait => self.handleInitAckChunk(c) catch |err| switch (err) {
                    error.MissingCookie => continue,
                    error.InvalidParameter => continue,
                    else => |e| return e,
                },
                else => {},
            },
            .cookie_echo => |cookie| {
                try self.transmits.ensureUnusedCapacity(self.allocator, 2);
                if (!std.mem.eql(u8, cookie, &self.cookie)) break;

                self.transmits.pushBackAssumeCapacity(.cookie_ack);
                if (self.state == .closed) try self.setStateToEstablished();
            },
            .cookie_ack => if (self.state == .cookie_echoed) {
                try self.setStateToEstablished();
                self.resetInitTimer();
            },
            .data => |d| {
                switch (self.state) {
                    .established, .shutdown_pending, .shutdown_sent => {},
                    else => break,
                }

                immediate_sack |= self.message_assembler.receiveData(self.allocator, d) catch |err| switch (err) {
                    error.NoUserData => {
                        try self.transmits.pushBack(self.allocator, .{ .abort = d.tsn });
                        try self.setStateToClosed();
                        return self.messageIterator();
                    },
                    else => |e| return e,
                };
                received_data = true;
            },
            .sack => |sack| switch (self.state) {
                .established, .shutdown_pending, .shutdown_received, .cookie_echoed => try self.handleSack(sack, now),
                else => {},
            },
            .heartbeat => |hb| if (self.state == .established and hb.len <= max_heartbeat_info) {
                try self.transmits.pushBack(self.allocator, .{ .heartbeat_ack = hb });
            },
            .abort => if (self.state != .closed) {
                try self.setStateToClosed();
                return null;
            },
            .shutdown => |cumulative_tsn| try self.handleShutdownChunk(cumulative_tsn, now),
            .shutdown_ack => try self.handleShutdownAck(),
            .shutdown_complete => if (self.state == .shutdown_ack_sent) {
                try self.setStateToClosed();
                return null;
            },
        }
    }

    if (received_data and self.state == .shutdown_sent) {
        try self.transmits.pushBack(self.allocator, .{ .shutdown = self.message_assembler.getCumulativeTsn() });
        if (self.message_assembler.hasGaps()) try self.pushSack();
        return self.messageIterator();
    }

    if (!received_data or (self.state != .established and self.state != .shutdown_pending)) return null;

    if (self.message_assembler.scheduleSack(immediate_sack, now)) try self.pushSack();

    return self.messageIterator();
}

pub fn handleTimeout(self: *Association, now: i64) std.mem.Allocator.Error!void {
    if (now >= self.init_deadline) {
        if (self.init_attempts > max_init_retransmits) {
            try self.setStateToClosed();
            return;
        }

        switch (self.state) {
            .cookie_wait => try self.transmits.pushBack(self.allocator, .init),
            .cookie_echoed => try self.transmits.pushBack(self.allocator, .{ .cookie_echo = self.peer_cookie }),
            else => {},
        }
    }

    if (now >= self.message_assembler.sack_deadline) try self.pushSack();

    // T3-rtx and T2-shutdown share the deadline, no data is in flight once shutdown starts
    if (now >= self.t3_timer.deadline) {
        self.t3_timer.error_count += 1;
        if (self.t3_timer.error_count > assoc_max_retransmits) {
            try self.setStateToClosed();
            return;
        }

        switch (self.state) {
            .shutdown_sent => try self.transmits.pushBack(self.allocator, .{ .shutdown = self.message_assembler.getCumulativeTsn() }),
            .shutdown_ack_sent => try self.transmits.pushBack(self.allocator, .shutdown_ack),
            else => {
                self.ssthresh = @max(self.cwnd / 2, 4 * message.PMDCS);
                self.partial_bytes_acked = 0;
                self.cwnd = message.PMDCS;
                self.message_queue.handleTimeout();
                self.fast_retransmit = false;
            },
        }
        self.t3_timer.backoffAndRestart(now);
        self.t3_timer.clearCurrentSample();
    }

    if (now >= self.t3_timer.idle_deadline) {
        // RFC9260 - 7.2.1. cwnd decays per RTO while no data is transmitted, the idle timer stops once cwnd no longer decays.
        const curr_cwnd = self.cwnd;
        self.cwnd = @min(self.cwnd, @max(self.cwnd / 2, 4 * message.PMDCS));
        if (curr_cwnd != self.cwnd) self.t3_timer.startIdle(now) else self.t3_timer.stopIdle();
    }
}

pub fn pollEvent(self: *Association) ?Event {
    return self.events.popFront();
}

pub const TransmitError = error{ WriteFailed, OutOfMemory };

/// Poll the association for data to transmit.
///
/// Chunks are bundled into packets of at most `message.mtu` bytes. `buffer` must be at least 128 bytes;
/// `error.WriteFailed` is returned when the buffer is too small to write one chunk.
pub fn pollTransmit(self: *Association, buffer: []u8, now: i64) TransmitError!?[]const u8 {
    if (buffer.len < min_buffer_size) return error.WriteFailed;
    var written: usize = message.packet_header_size;
    var slice = buffer[written..@min(buffer.len, message.MTU)];
    self.writeCommonHeader(buffer);

    while (true) {
        const next = self.nextChunk(slice, now, written == message.packet_header_size) catch |err| {
            if (written == message.packet_header_size) return err;
            break;
        };
        const maybe_control_chunk, const w = next orelse if (written == message.packet_header_size) return null else break;
        written += w;
        slice = slice[w..];

        if (maybe_control_chunk) |chunk_type| {
            switch (chunk_type) {
                .init_ack => |tag| std.mem.writeInt(u32, buffer[4..8], tag, .big),
                .shutdown_complete => |reflected| if (reflected) std.mem.writeInt(u32, buffer[4..8], self.verification_tag, .big),
                else => {},
            }

            switch (chunk_type) {
                .init, .init_ack, .shutdown_complete => break,
                else => {},
            }
        }

        if (slice.len < min_buffer_size) break;
    }

    Helper.checkSum(buffer[0..written]);
    return buffer[0..written];
}

pub fn pollTimeout(self: *Association) ?i64 {
    const deadline = @min(
        @min(self.message_assembler.sack_deadline, self.init_deadline),
        @min(self.t3_timer.deadline, self.t3_timer.idle_deadline),
    );
    return if (deadline == std.math.maxInt(i64)) null else deadline;
}

fn nextChunk(self: *Association, buffer: []u8, now: i64, first_chunk: bool) TransmitError!?struct { ?Transmit, usize } {
    if (self.fast_retransmit) {
        if (self.nextFastRetransmit(buffer, now) catch |err| {
            if (first_chunk) return err;
            self.fast_retransmit = false;
            return null;
        }) |size| return .{ null, size };
        self.fast_retransmit = false;
    }

    const chunk_type = self.transmits.front() orelse {
        switch (self.state) {
            .established, .shutdown_received, .shutdown_pending => {},
            else => return null,
        }

        if (self.message_queue.flight_size >= self.cwnd) return null;

        if (self.fast_recovery) if (try self.nextFastRetransmit(buffer, now)) |size| return .{ null, size };
        const result = try self.message_queue.nextChunk(self.allocator, buffer) orelse return null;
        self.zero_wnd_probe |= result.zero_wnd_probe;

        self.t3_timer.setDeadline(now);
        self.t3_timer.stopIdle();
        if (!result.retransmit) self.t3_timer.setSample(self.message_queue.highestTsnSent(), now);
        return .{ null, result.written };
    };

    if (!first_chunk) switch (chunk_type) {
        .init, .init_ack, .shutdown_complete => return null,
        else => {},
    };

    const written = switch (chunk_type) {
        .init => self.writeInit(buffer),
        .init_ack => self.writeInitAck(buffer),
        .cookie_echo => |cookie| try writeCookieEcho(cookie, buffer),
        .cookie_ack => self.writeCookieAck(buffer),
        .abort => |tsn| self.writeNoUserDataAbort(tsn, buffer),
        .sack => self.message_assembler.writeSack(buffer),
        .heartbeat_ack => |data| try writeHeartbeatAck(data, buffer),
        .shutdown => |tsn| writeShutdown(buffer, tsn),
        .shutdown_ack => writeShutdownAck(buffer),
        .shutdown_complete => |reflected| writeShutdownComplete(buffer, reflected),
    };

    switch (chunk_type) {
        .init, .cookie_echo => self.setInitTimer(now),
        .shutdown, .shutdown_ack => self.t3_timer.restart(now),
        else => {},
    }

    _ = self.transmits.popFront();
    return .{ chunk_type, written };
}

fn nextFastRetransmit(self: *Association, buffer: []u8, now: i64) error{WriteFailed}!?u32 {
    const retransmit = try self.message_queue.nextFastRetransmit(buffer) orelse return null;
    // RFC9260 - 7.2.4. restarting T3-rtx for any other tsn would keep delaying the timeout of the lowest outstanding one.
    if (retransmit.tsn == self.message_queue.lowestOutstandingTsn()) self.t3_timer.restart(now);
    // RFC9260 - 6.3.1. C5 (Karn's algorithm), a retransmitted tsn gives an ambiguous rtt measurement.
    if (self.t3_timer.rtt_tsn == retransmit.tsn) self.t3_timer.clearCurrentSample();
    return retransmit.size;
}

fn messageIterator(self: *Association) ?MessageIterator {
    return if (self.message_assembler.ready.len != 0) .{ .assoc = self } else null;
}

fn setStateToClosed(self: *Association) !void {
    try self.events.pushBack(self.allocator, .comm_down);
    self.state = .closed;
    self.resetInitTimer();
    self.message_queue.close(self.allocator, self.initial_tsn);
    self.message_assembler.reset(self.allocator);
    self.t3_timer = .init;
    self.zero_wnd_probe = false;
}

fn setStateToEstablished(self: *Association) !void {
    if (self.state == .established) return;
    try self.events.ensureUnusedCapacity(self.allocator, 1);
    self.state = .established;
    self.events.pushBackAssumeCapacity(.comm_up);
}

fn updateT3Timer(self: *Association, cumulative_tsn_advanced: bool, now: i64) void {
    if (cumulative_tsn_advanced or self.zero_wnd_probe) self.t3_timer.error_count = 0;
    if (cumulative_tsn_advanced) self.zero_wnd_probe = false;

    if (self.message_queue.isAllAcked()) {
        // SACKs received while idle must not postpone the decay
        if (self.t3_timer.idle_deadline == std.math.maxInt(i64)) self.t3_timer.startIdle(now);
        self.t3_timer.stop();
        self.partial_bytes_acked = 0;
    } else if (cumulative_tsn_advanced or self.t3_timer.deadline == std.math.maxInt(i64)) {
        // a stopped timer with unacked data means the peer reneged on gap acked tsns
        self.t3_timer.restart(now);
    }
}

fn handleInitChunk(self: *Association, chunk: message.Init) !void {
    try self.transmits.pushBack(self.allocator, .{ .init_ack = chunk.initiate_tag });

    // In cookie wait or cookie echoed state, we should not update the state.
    if (self.state == .closed) {
        self.inbound_streams = @min(self.inbound_streams, chunk.outbound_streams);
        self.outbound_streams = @min(self.outbound_streams, chunk.inbound_streams);
        self.message_assembler.setInitTsn(chunk.initial_tsn);
        self.peer_verification_tag = chunk.initiate_tag;
        self.message_queue.rwnd = chunk.a_rwnd;
    }
}

fn handleInitAckChunk(self: *Association, chunk: message.Init) !void {
    try self.transmits.ensureUnusedCapacity(self.allocator, 1);

    const cookie = blk: {
        var it = chunk.iterateParameters();
        while (try it.next()) |param| switch (param) {
            .state_cookie => |cookie_param| break :blk cookie_param,
            else => {},
        } else return error.MissingCookie;
    };

    if (self.peer_cookie.len != 0) {
        self.allocator.free(self.peer_cookie);
        self.peer_cookie = &.{};
    }
    self.peer_cookie = try self.allocator.dupe(u8, cookie);

    self.inbound_streams = @min(self.inbound_streams, chunk.outbound_streams);
    self.outbound_streams = @min(self.outbound_streams, chunk.inbound_streams);
    self.message_assembler.setInitTsn(chunk.initial_tsn);
    self.peer_verification_tag = chunk.initiate_tag;
    self.message_queue.rwnd = chunk.a_rwnd;
    self.state = .cookie_echoed;
    self.resetInitTimer();
    self.transmits.pushBackAssumeCapacity(.{ .cookie_echo = self.peer_cookie });
}

fn handleSack(self: *Association, sack: message.Sack, now: i64) !void {
    const prev_flight_size = self.message_queue.flight_size;
    const result = self.message_queue.handleSack(self.allocator, sack, self.fast_recovery) orelse return;

    if (self.cwnd <= self.ssthresh) {
        // Slow start
        if (prev_flight_size >= self.cwnd and !self.fast_recovery) self.cwnd += @min(message.PMDCS, result.acked_data);
    } else {
        // Congestion avoidance
        self.partial_bytes_acked +|= result.acked_data +| result.duplicate_data;
        if (self.partial_bytes_acked >= self.cwnd) {
            if (prev_flight_size < self.cwnd)
                self.partial_bytes_acked = self.cwnd
            else {
                self.partial_bytes_acked -= self.cwnd;
                self.cwnd +|= message.PMDCS;
            }
        }
    }

    if (self.fast_recovery) {
        if (self.message_queue.isCumulativelyAcked(self.fast_recovery_exit_tsn)) {
            self.fast_recovery = false;
            self.fast_recovery_exit_tsn = 0;
        }
    } else if (result.has_fast_retransmit) {
        self.fast_recovery = true;
        self.fast_retransmit = true;
        self.fast_recovery_exit_tsn = self.message_queue.highestTsnSent();

        self.ssthresh = @max(self.cwnd / 2, 4 * message.PMDCS);
        self.cwnd = self.ssthresh;
        self.partial_bytes_acked = 0;
    }

    if (self.t3_timer.rtt_tsn) |tsn| if (self.message_queue.isAcked(tsn)) self.t3_timer.calculateRto(now);
    self.updateT3Timer(result.cumulative_ack_advanced, now);
    try self.maybeSendShutdown();
}

fn handleShutdownChunk(self: *Association, cumulative_tsn: u32, now: i64) !void {
    switch (self.state) {
        .established, .shutdown_pending, .shutdown_received, .shutdown_sent => {},
        else => return,
    }
    const cumulative_ack_advanced = switch (self.message_queue.handleShutdownChunk(self.allocator, cumulative_tsn)) {
        .invalid => return,
        .unchanged => false,
        .advanced => true,
    };
    self.updateT3Timer(cumulative_ack_advanced, now);

    if (self.state == .shutdown_sent) {
        self.state = .shutdown_ack_sent;
        try self.transmits.pushBack(self.allocator, .shutdown_ack);
    } else {
        self.state = .shutdown_received;
        try self.maybeSendShutdown();
    }
}

fn handleShutdownAck(self: *Association) !void {
    switch (self.state) {
        .shutdown_sent, .shutdown_ack_sent => {
            try self.transmits.pushBack(self.allocator, .{ .shutdown_complete = false });
            try self.setStateToClosed();
        },
        .closed => try self.transmits.pushBack(self.allocator, .{ .shutdown_complete = true }),
        else => return,
    }
}

fn maybeSendShutdown(self: *Association) !void {
    if (!self.message_queue.isEmpty()) return;
    switch (self.state) {
        .shutdown_received => {
            try self.transmits.pushBack(self.allocator, .shutdown_ack);
            self.state = .shutdown_ack_sent;
        },
        .shutdown_pending => {
            try self.transmits.pushBack(self.allocator, .{ .shutdown = self.message_assembler.getCumulativeTsn() });
            self.state = .shutdown_sent;
        },
        else => {},
    }
}

fn isValidVerificationTag(self: *const Association, packet: *const message.Packet) bool {
    if (packet.chunks.len < 2) return packet.verification_tag == self.verification_tag;
    const reflected = packet.chunks[1] & 0x01 != 0;
    return switch (@as(message.ChunkType, @enumFromInt(packet.chunks[0]))) {
        .init => packet.verification_tag == 0,
        .abort, .shutdown_complete => packet.verification_tag == if (reflected) self.peer_verification_tag else self.verification_tag,
        else => packet.verification_tag == self.verification_tag,
    };
}

fn pushSack(self: *Association) !void {
    try self.transmits.pushBack(self.allocator, .sack);
    self.message_assembler.clearDelayedSack();
}

fn resetInitTimer(self: *Association) void {
    self.init_attempts = 0;
    self.init_rto = 1;
    self.init_deadline = std.math.maxInt(i64);
}

fn setInitTimer(self: *Association, now: i64) void {
    self.init_deadline = now + @as(i64, self.init_rto) * std.time.ms_per_s;
    self.init_rto = @min(self.init_rto * 2, max_rto);
    self.init_attempts += 1;
}

fn writeInit(self: *Association, buffer: []u8) usize {
    buffer[0] = @intFromEnum(message.ChunkType.init);
    buffer[1] = 0; // flags
    std.mem.writeInt(u16, buffer[2..4], 20, .big);
    std.mem.writeInt(u32, buffer[4..8], self.verification_tag, .big);
    std.mem.writeInt(u32, buffer[8..12], self.message_assembler.capacity, .big);
    std.mem.writeInt(u16, buffer[12..14], self.outbound_streams, .big);
    std.mem.writeInt(u16, buffer[14..16], self.inbound_streams, .big);
    std.mem.writeInt(u32, buffer[16..20], self.initial_tsn, .big);

    return 20;
}

fn writeInitAck(self: *Association, buffer: []u8) usize {
    buffer[0] = @intFromEnum(message.ChunkType.init_ack);
    buffer[1] = 0; // flags

    std.mem.writeInt(u32, buffer[4..8], self.verification_tag, .big);
    std.mem.writeInt(u32, buffer[8..12], self.message_assembler.capacity, .big);
    std.mem.writeInt(u16, buffer[12..14], self.outbound_streams, .big);
    std.mem.writeInt(u16, buffer[14..16], self.inbound_streams, .big);
    std.mem.writeInt(u32, buffer[16..20], self.initial_tsn, .big);

    // Cookie is 32 bytes and the smallest buffer we can write to is 128 bytes, so this should never fail.
    const written = message.Parameter.writeBuffer(
        .{ .state_cookie = &self.cookie },
        buffer[20..],
    ) catch unreachable;
    std.mem.writeInt(u16, buffer[2..4], @intCast(20 + written), .big);
    return 20 + written;
}

fn writeCookieEcho(cookie: []const u8, buffer: []u8) error{WriteFailed}!usize {
    const size = std.mem.alignForward(usize, cookie.len + 4, 4);
    if (buffer.len < size) return error.WriteFailed;
    buffer[0] = @intFromEnum(message.ChunkType.cookie_echo);
    buffer[1] = 0; // flags
    std.mem.writeInt(u16, buffer[2..4], @intCast(4 + cookie.len), .big);
    @memcpy(buffer[4..][0..cookie.len], cookie);
    @memset(buffer[4 + cookie.len .. size], 0);

    return size;
}

fn writeCookieAck(self: *Association, buffer: []u8) usize {
    _ = self;
    buffer[0] = @intFromEnum(message.ChunkType.cookie_ack);
    buffer[1] = 0; // flags
    std.mem.writeInt(u16, buffer[2..4], 4, .big);

    return 4;
}

fn writeNoUserDataAbort(self: *Association, tsn: u32, buffer: []u8) usize {
    _ = self;
    buffer[0] = @intFromEnum(message.ChunkType.abort);
    buffer[1] = 0; // flags
    const written = message.Error.writeNoUserDataErrorBuffer(buffer[4..], tsn);
    std.mem.writeInt(u16, buffer[2..4], @intCast(4 + written), .big);

    return 4 + written;
}

fn writeHeartbeatAck(data: []const u8, buffer: []u8) !usize {
    buffer[0] = @intFromEnum(message.ChunkType.heartbeat_ack);
    buffer[1] = 0; // flags
    const written = try message.Parameter.writeBuffer(.{ .heartbeat_info = data }, buffer[4..]);
    std.mem.writeInt(u16, buffer[2..4], @intCast(written + 4), .big);
    return written + 4;
}

fn writeShutdown(buffer: []u8, tsn: u32) usize {
    buffer[0] = @intFromEnum(message.ChunkType.shutdown);
    buffer[1] = 0; // flags
    std.mem.writeInt(u16, buffer[2..4], 8, .big);
    std.mem.writeInt(u32, buffer[4..8], tsn, .big);
    return 8;
}

fn writeShutdownAck(buffer: []u8) usize {
    buffer[0] = @intFromEnum(message.ChunkType.shutdown_ack);
    buffer[1] = 0; // flags
    std.mem.writeInt(u16, buffer[2..4], 4, .big);
    return 4;
}

fn writeShutdownComplete(buffer: []u8, reflected: bool) usize {
    buffer[0] = @intFromEnum(message.ChunkType.shutdown_complete);
    buffer[1] = @intFromBool(reflected);
    std.mem.writeInt(u16, buffer[2..4], 4, .big);
    return 4;
}

fn writeCommonHeader(self: *const Association, buffer: []u8) void {
    std.mem.writeInt(u16, buffer[0..2], self.source_port, .big);
    std.mem.writeInt(u16, buffer[2..4], self.dest_port, .big);
    std.mem.writeInt(u32, buffer[4..8], self.peer_verification_tag, .big);
    std.mem.writeInt(u32, buffer[8..12], 0, .big); // checksum
}

const testing = std.testing;

var test_prng = std.Random.DefaultPrng.init(0);

fn testAssoc() Association {
    var assoc = Association.init(testing.allocator, .{
        .source_port = 5000,
        .dest_port = 5000,
        .random = test_prng.random(),
    });
    assoc.peer_verification_tag = assoc.verification_tag;
    assoc.message_assembler.setInitTsn(assoc.initial_tsn);
    assoc.message_queue.rwnd = 1024 * 1024;
    return assoc;
}

fn testEstablishedAssoc() !Association {
    var assoc = testAssoc();
    errdefer assoc.deinit();
    try assoc.setStateToEstablished();
    _ = assoc.pollEvent();
    return assoc;
}

fn testPacket(buffer: *[16]u8, vtag: u32, chunk_type: message.ChunkType, flags: u8) []const u8 {
    std.mem.writeInt(u16, buffer[0..2], 5000, .big);
    std.mem.writeInt(u16, buffer[2..4], 5000, .big);
    std.mem.writeInt(u32, buffer[4..8], vtag, .big);
    std.mem.writeInt(u32, buffer[8..12], 0, .big);
    buffer[12] = @intFromEnum(chunk_type);
    buffer[13] = flags;
    std.mem.writeInt(u16, buffer[14..16], 4, .big);
    Helper.checkSum(buffer);
    return buffer;
}

fn testDataPacket(buffer: *[32]u8, vtag: u32, tsn: u32) []const u8 {
    std.mem.writeInt(u16, buffer[0..2], 5000, .big);
    std.mem.writeInt(u16, buffer[2..4], 5000, .big);
    std.mem.writeInt(u32, buffer[4..8], vtag, .big);
    std.mem.writeInt(u32, buffer[8..12], 0, .big);
    const data = message.Data{
        .flags = .{ .start_fragment = true, .end_fragment = true },
        .tsn = tsn,
        .stream_id = 0,
        .stream_seq = 0,
        .ppid = 0,
        .user_data = "ping",
    };
    _ = data.write(buffer[message.packet_header_size..]);
    Helper.checkSum(buffer);
    return buffer;
}

fn testDataPacket2(assoc: *const Association, data: message.Data, buffer: []u8) []const u8 {
    assoc.writeCommonHeader(buffer);
    const written = data.write(buffer[message.packet_header_size..]);
    Helper.checkSum(buffer[0 .. message.packet_header_size + written]);
    return buffer[0 .. message.packet_header_size + written];
}

fn testRead(assoc: *Association, packet: []const u8, now: i64, lens: []usize) ![]usize {
    var it = try assoc.handleRead(packet, now) orelse return lens[0..0];
    var n: usize = 0;
    while (it.next()) |msg| : (n += 1) {
        defer msg.deinit(testing.allocator);
        lens[n] = msg.data.len;
    }
    return lens[0..n];
}

fn testSackPacket(buffer: []u8, assoc: *const Association, cumulative_tsn: u32, gaps: []const u16, a_rwnd: u32) []const u8 {
    assoc.writeCommonHeader(buffer);
    buffer[12] = @intFromEnum(message.ChunkType.sack);
    buffer[13] = 0;
    std.mem.writeInt(u32, buffer[16..20], cumulative_tsn, .big);
    std.mem.writeInt(u32, buffer[20..24], a_rwnd, .big);
    std.mem.writeInt(u16, buffer[24..26], @intCast(gaps.len / 2), .big);
    std.mem.writeInt(u16, buffer[26..28], 0, .big);
    for (0..gaps.len / 2) |idx| {
        std.mem.writeInt(u16, buffer[28 + idx * 4 ..][0..2], gaps[idx * 2], .big);
        std.mem.writeInt(u16, buffer[30 + idx * 4 ..][0..2], gaps[idx * 2 + 1], .big);
    }
    const len: u16 = @intCast(16 + gaps.len * 2);
    std.mem.writeInt(u16, buffer[14..16], len, .big);
    const packet = buffer[0 .. message.packet_header_size + len];
    Helper.checkSum(packet);
    return packet;
}

test "Association.pollTransmits: a timed out message that was gap acked is not retransmitted" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });
    try assoc.handleWrite("bbbb", .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    while (try assoc.pollTransmit(&out, 0)) |_| {}

    var buffer: [32]u8 = undefined;
    _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn -% 1, &.{ 2, 2 }, assoc.message_assembler.advertised_wnd), 0);
    try assoc.handleTimeout(10_000);

    const packet = (try assoc.pollTransmit(&out, 10_000)).?;
    try testing.expectEqual(assoc.initial_tsn, std.mem.readInt(u32, packet[16..20], .big));
    try testing.expectEqual(null, try assoc.pollTransmit(&out, 10_000));
}

test "Association.pollTransmit: control chunks are sent before retransmissions" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();
    assoc.message_assembler.setInitTsn(100);

    try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    while (try assoc.pollTransmit(&out, 0)) |_| {}

    var buffer: [32]u8 = undefined;
    var lens: [1]usize = undefined;
    const messages_len = try testRead(
        &assoc,
        testDataPacket(&buffer, assoc.verification_tag, 100),
        0,
        &lens,
    );
    try testing.expectEqual(1, messages_len.len);

    // both the delayed sack and T3 expire
    try assoc.handleTimeout(10_000);

    const packet_data = (try assoc.pollTransmit(&out, 10_000)).?;
    var packet = try message.Packet.parse(packet_data);
    var it = packet.iterateChunks();
    var chunk = try it.next() orelse return error.UnexpectedFailedTest;
    try testing.expect(std.meta.activeTag(chunk) == .sack);

    chunk = try it.next() orelse return error.UnexpectedFailedTest;
    try testing.expect(std.meta.activeTag(chunk) == .data);
    try testing.expect(try it.next() == null);
}

test "Association.pollTransmit: a retransmission that does not fit is sent in the next packet" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();
    assoc.message_assembler.setInitTsn(100);

    const data: [message.MTU - message.packet_header_size - message.data_chunk_header_size]u8 = @splat('a');
    try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    while (try assoc.pollTransmit(&out, 0)) |_| {}

    try assoc.pushSack();
    try assoc.handleTimeout(10_000);

    var packet = try message.Packet.parse((try assoc.pollTransmit(&out, 10_000)).?);
    var it = packet.iterateChunks();
    try testing.expect(std.meta.activeTag(try it.next() orelse return error.UnexpectedFailedTest) == .sack);
    try testing.expect(try it.next() == null);

    const retransmit = (try assoc.pollTransmit(&out, 10_000)).?;
    try testing.expectEqual(message.MTU, retransmit.len);
    try testing.expectEqual(assoc.initial_tsn, std.mem.readInt(u32, retransmit[16..20], .big));
}

test "Association.pollTransmit: cookie echo is padded when bundled" {
    var assoc = testAssoc();
    defer assoc.deinit();

    try assoc.transmits.pushBack(testing.allocator, .{ .cookie_echo = "abcde" });
    try assoc.transmits.pushBack(testing.allocator, .cookie_ack);

    var out: [message.MTU]u8 = undefined;
    const packet = (try assoc.pollTransmit(&out, 0)).?;
    try testing.expectEqual(message.packet_header_size + 12 + 4, packet.len);
    try testing.expectEqual(9, std.mem.readInt(u16, packet[14..16], .big));
    try testing.expectEqualSlices(u8, &.{ 0, 0, 0 }, packet[21..24]);
    try testing.expectEqual(@intFromEnum(message.ChunkType.cookie_ack), packet[24]);
}

test "Association.pollTransmit: a cookie echo that does not fit does not arm the init timer" {
    var assoc = testAssoc();
    defer assoc.deinit();

    const cookie: [min_buffer_size]u8 = @splat('c');
    try assoc.transmits.pushBack(testing.allocator, .{ .cookie_echo = &cookie });

    var out: [min_buffer_size]u8 = undefined;
    try testing.expectError(error.WriteFailed, assoc.pollTransmit(&out, 0));
    try testing.expectError(error.WriteFailed, assoc.pollTransmit(&out, 0));
    try testing.expectEqual(0, assoc.init_attempts);
    try testing.expectEqual(std.math.maxInt(i64), assoc.init_deadline);
    try testing.expectEqual(1, assoc.transmits.len);
}

test "Association.pollTransmit: data chunk padding fits in a buffer not aligned to 4 bytes" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    const data: [500]u8 = @splat('a');
    try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    var out: [min_buffer_size + 2]u8 = undefined;
    const packet = (try assoc.pollTransmit(&out, 0)).?;
    try testing.expectEqual(0, packet.len % 4);
    try testing.expect(packet.len <= out.len);
}

test "Association.pollTransmit: init, init_ack and shutdown_complete must not be bundled" {
    var assoc = testAssoc();
    defer assoc.deinit();

    try assoc.transmits.pushBack(testing.allocator, .init);
    try assoc.transmits.pushBack(testing.allocator, .{ .init_ack = assoc.verification_tag });
    try assoc.transmits.pushBack(testing.allocator, .{ .heartbeat_ack = &.{ 1, 2, 3 } });
    try assoc.transmits.pushBack(testing.allocator, .{ .shutdown_complete = true });

    var out: [message.MTU]u8 = undefined;
    for (0..4) |_| try testing.expect(try assoc.pollTransmit(&out, 0) != null);
    try testing.expectEqual(null, try assoc.pollTransmit(&out, 0));
}

test "Association.pollTransmit: bundle control messages with data" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });
    try assoc.handleWrite("bbbb", .{ .stream_id = 0, .ppid = 0 });

    try assoc.transmits.pushBack(testing.allocator, .init);
    try assoc.transmits.pushBack(testing.allocator, .cookie_ack);

    var out: [message.MTU]u8 = undefined;
    _ = (try assoc.pollTransmit(&out, 0)).?;
    const data = (try assoc.pollTransmit(&out, 0)).?;
    var packet = try message.Packet.parse(data);
    var it = packet.iterateChunks();
    try testing.expect(std.meta.activeTag(try it.next() orelse return error.UnexpectedFailedTest) == .cookie_ack);
    try testing.expect(std.meta.activeTag(try it.next() orelse return error.UnexpectedFailedTest) == .data);
    try testing.expect(std.meta.activeTag(try it.next() orelse return error.UnexpectedFailedTest) == .data);
    try testing.expect(try it.next() == null);

    try testing.expect(try assoc.pollTransmit(&out, 0) == null);
}

test "Association.handleRead: a sack acknowledging unsent tsns is ignored" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    _ = (try assoc.pollTransmit(&out, 0)).?;

    try assoc.handleWrite("bbbb", .{ .stream_id = 0, .ppid = 0 });

    var buffer: [32]u8 = undefined;
    _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn +% 1, &.{}, assoc.message_assembler.advertised_wnd), 0);
    _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn -% 1, &.{ 1, 2 }, assoc.message_assembler.advertised_wnd), 0);

    try testing.expectEqual(null, assoc.pollEvent());
    try testing.expectEqual(2, assoc.message_queue.messages.len);
    try testing.expect(try assoc.pollTransmit(&out, 0) != null);
}

test "Association.handleRead: a shutdown acknowledging unsent tsns is ignored" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });

    var buffer: [20]u8 = undefined;
    assoc.writeCommonHeader(&buffer);
    buffer[12] = @intFromEnum(message.ChunkType.shutdown);
    buffer[13] = 0;
    std.mem.writeInt(u16, buffer[14..16], 8, .big);
    std.mem.writeInt(u32, buffer[16..20], assoc.initial_tsn, .big);
    Helper.checkSum(&buffer);
    _ = try assoc.handleRead(&buffer, 0);

    try testing.expectEqual(.established, assoc.state);
    try testing.expectEqual(1, assoc.message_queue.messages.len);
}

test "Association.handleRead: a shutdown acknowledging gap acked chunks does not subtract them twice from the flight size" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    const data: [100]u8 = @splat('a');
    for (0..4) |_| try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    while (try assoc.pollTransmit(&out, 0)) |_| {}

    var sack_buffer: [32]u8 = undefined;
    _ = try assoc.handleRead(testSackPacket(&sack_buffer, &assoc, assoc.initial_tsn, &.{ 2, 2 }, 1000), 0);
    try testing.expectEqual(200, assoc.message_queue.flight_size);

    var buffer: [20]u8 = undefined;
    assoc.writeCommonHeader(&buffer);
    buffer[12] = @intFromEnum(message.ChunkType.shutdown);
    buffer[13] = 0;
    std.mem.writeInt(u16, buffer[14..16], 8, .big);
    std.mem.writeInt(u32, buffer[16..20], assoc.initial_tsn +% 2, .big);
    Helper.checkSum(&buffer);
    _ = try assoc.handleRead(&buffer, 0);

    try testing.expectEqual(100, assoc.message_queue.flight_size);
}

test "Association.handleRead: Abort with the T bit and the peer tag closes the association" {
    var assoc = testAssoc();
    defer assoc.deinit();
    assoc.state = .established;
    assoc.peer_verification_tag = 0x11223344;

    var buffer: [16]u8 = undefined;
    _ = try assoc.handleRead(testPacket(&buffer, 0x11223344, .abort, 0x01), 0);

    try testing.expectEqual(.closed, assoc.state);
    try testing.expect(assoc.pollEvent().? == .comm_down);
}

test "Association.handleRead: Abort with the T bit and our own tag is ignored" {
    var assoc = testAssoc();
    defer assoc.deinit();
    assoc.state = .established;
    assoc.peer_verification_tag = 0x11223344;

    var buffer: [16]u8 = undefined;
    _ = try assoc.handleRead(testPacket(&buffer, assoc.verification_tag, .abort, 0x01), 0);

    try testing.expectEqual(.established, assoc.state);
    try testing.expectEqual(null, assoc.pollEvent());
}

test "Association.handleRead: shutdown complete with the T bit and the peer tag closes the association" {
    var assoc = testAssoc();
    defer assoc.deinit();
    assoc.state = .shutdown_ack_sent;
    assoc.peer_verification_tag = 0x11223344;

    var buffer: [16]u8 = undefined;
    _ = try assoc.handleRead(testPacket(&buffer, 0x11223344, .shutdown_complete, 0x01), 0);

    try testing.expectEqual(.closed, assoc.state);
}

test "Association.handleRead: data in shutdown sent is answered with a shutdown" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();
    assoc.message_assembler.setInitTsn(100);
    assoc.state = .shutdown_sent;

    var buffer: [32]u8 = undefined;
    var lens: [1]usize = undefined;
    const messages_len = try testRead(
        &assoc,
        testDataPacket(&buffer, assoc.verification_tag, 100),
        0,
        &lens,
    );
    try testing.expectEqual(1, messages_len.len);

    try testing.expectEqual(.shutdown_sent, assoc.state);
    try testing.expectEqual(Transmit{ .shutdown = 100 }, assoc.transmits.popFront().?);
}

test "Association.handleRead: data in shutdown pending is delivered and acknowledged" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();
    assoc.message_assembler.setInitTsn(100);
    assoc.state = .shutdown_pending;

    var buffer: [32]u8 = undefined;
    var lens: [1]usize = undefined;
    const messages_len = try testRead(
        &assoc,
        testDataPacket(&buffer, assoc.verification_tag, 100),
        0,
        &lens,
    );
    try testing.expectEqual(1, messages_len.len);
    try testing.expect(assoc.message_assembler.send_sack);
}

test "Association.handleRead: shutdown in shutdown pending moves to shutdown received" {
    var assoc = testAssoc();
    defer assoc.deinit();
    assoc.state = .shutdown_pending;

    var buffer: [20]u8 = undefined;
    assoc.writeCommonHeader(&buffer);
    buffer[12] = @intFromEnum(message.ChunkType.shutdown);
    buffer[13] = 0;
    std.mem.writeInt(u16, buffer[14..16], 8, .big);
    std.mem.writeInt(u32, buffer[16..20], assoc.initial_tsn -% 1, .big);
    Helper.checkSum(&buffer);
    _ = try assoc.handleRead(&buffer, 0);

    try testing.expectEqual(.shutdown_ack_sent, assoc.state);
    try testing.expectEqual(Transmit.shutdown_ack, assoc.transmits.popFront().?);
}

test "Association.handleRead: shutdown ack in shutdown ack sent closes the association" {
    var assoc = testAssoc();
    defer assoc.deinit();
    assoc.state = .shutdown_ack_sent;

    var buffer: [16]u8 = undefined;
    _ = try assoc.handleRead(testPacket(&buffer, assoc.verification_tag, .shutdown_ack, 0), 0);

    try testing.expectEqual(.closed, assoc.state);
    try testing.expectEqual(Transmit{ .shutdown_complete = false }, assoc.transmits.popFront().?);
}

test "Association.handleRead: out of the blue shutdown ack is answered with a reflected shutdown complete" {
    var assoc = testAssoc();
    defer assoc.deinit();
    assoc.peer_verification_tag = 0x11223344;

    var buffer: [16]u8 = undefined;
    _ = try assoc.handleRead(testPacket(&buffer, assoc.verification_tag, .shutdown_ack, 0), 0);

    try testing.expectEqual(.closed, assoc.state);
    try testing.expectEqual(null, assoc.pollEvent());

    var out: [message.MTU]u8 = undefined;
    const packet = (try assoc.pollTransmit(&out, 0)).?;
    try testing.expectEqual(assoc.verification_tag, std.mem.readInt(u32, packet[4..8], .big));
    try testing.expectEqual(@intFromEnum(message.ChunkType.shutdown_complete), packet[12]);
    try testing.expectEqual(0x01, packet[13]);
}

test "Association.handleRead: all acked chunks with gap blocks stop the timer" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    for (0..6) |_| try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    while (try assoc.pollTransmit(&out, 0)) |_| {}

    var buffer: [32]u8 = undefined;
    var sack = testSackPacket(&buffer, &assoc, assoc.initial_tsn +% 2, &.{}, assoc.message_assembler.advertised_wnd);
    _ = try assoc.handleRead(sack, 5);

    try testing.expectEqual(0, assoc.t3_timer.error_count);
    try testing.expect(assoc.t3_timer.deadline != std.math.maxInt(i64));

    sack = testSackPacket(&buffer, &assoc, assoc.initial_tsn +% 3, &.{ 1, 2 }, assoc.message_assembler.advertised_wnd);
    _ = try assoc.handleRead(sack, 10);
    try testing.expect(assoc.t3_timer.deadline == std.math.maxInt(i64));
}

test "Association.handleTimeout: cwnd decays per rto while idle" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    var out: [message.MTU]u8 = undefined;
    var buffer: [32]u8 = undefined;
    const wnd = assoc.message_assembler.advertised_wnd;

    {
        try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });
        while (try assoc.pollTransmit(&out, 0)) |_| {}
        const sack = testSackPacket(&buffer, &assoc, assoc.initial_tsn, &.{}, wnd);
        _ = try assoc.handleRead(sack, 10);
        const idle_deadline = 10 + assoc.t3_timer.rto;
        try testing.expectEqual(idle_deadline, assoc.pollTimeout().?);

        // a duplicate sack doesn't postpone the idle deadline
        _ = try assoc.handleRead(sack, 20);
        try testing.expectEqual(idle_deadline, assoc.pollTimeout().?);

        assoc.cwnd = 16 * message.PMDCS;
        try assoc.handleTimeout(idle_deadline);
        try testing.expectEqual(8 * message.PMDCS, assoc.cwnd);
        try testing.expectEqual(idle_deadline + assoc.t3_timer.rto, assoc.pollTimeout().?);

        // the idle timer stops once cwnd no longer decays
        try assoc.handleTimeout(idle_deadline + assoc.t3_timer.rto);
        try testing.expectEqual(4 * message.PMDCS, assoc.cwnd);
        try testing.expectEqual(idle_deadline + 2 * assoc.t3_timer.rto, assoc.pollTimeout().?);
        try assoc.handleTimeout(idle_deadline + 2 * assoc.t3_timer.rto);
        try testing.expectEqual(4 * message.PMDCS, assoc.cwnd);
        try testing.expectEqual(null, assoc.pollTimeout());
    }

    {
        try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });
        while (try assoc.pollTransmit(&out, 5000)) |_| {}
        try testing.expectEqual(std.math.maxInt(i64), assoc.t3_timer.idle_deadline);
        const sack = testSackPacket(&buffer, &assoc, assoc.initial_tsn +% 1, &.{}, wnd);
        _ = try assoc.handleRead(sack, 5010);

        // a cwnd below the floor doesn't grow while idle
        assoc.cwnd = message.PMDCS;
        try assoc.handleTimeout(assoc.pollTimeout().?);
        try testing.expectEqual(message.PMDCS, assoc.cwnd);
        try testing.expectEqual(null, assoc.pollTimeout());
    }
}

test "Association.pollTransmit: a fast retransmit restarts T3-rtx only for the lowest outstanding tsn" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    for (0..8) |_| try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    while (try assoc.pollTransmit(&out, 0)) |_| {}
    try testing.expectEqual(assoc.initial_tsn, assoc.t3_timer.rtt_tsn.?);

    var buffer: [64]u8 = undefined;
    const cumulative_tsn = assoc.initial_tsn -% 1;
    const wnd = assoc.message_assembler.advertised_wnd;

    // the lowest outstanding tsn
    for ([_]u16{ 2, 3, 4 }) |end| _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, cumulative_tsn, &.{ 2, end }, wnd), 100);
    var packet = (try assoc.pollTransmit(&out, 100)).?;
    try testing.expectEqual(assoc.initial_tsn, std.mem.readInt(u32, packet[16..20], .big));
    try testing.expectEqual(100 + initial_rto, assoc.t3_timer.deadline);
    try testing.expectEqual(null, assoc.t3_timer.rtt_tsn);

    // another tsn
    for ([_]u16{ 6, 7, 8 }) |end| _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, cumulative_tsn, &.{ 2, 4, 6, end }, wnd), 200);
    packet = (try assoc.pollTransmit(&out, 200)).?;
    try testing.expectEqual(assoc.initial_tsn +% 4, std.mem.readInt(u32, packet[16..20], .big));
    try testing.expectEqual(100 + initial_rto, assoc.t3_timer.deadline);
}

test "Association.handleRead: a duplicate followed by new data in the same packet is sacked immediately" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    var buffer: [64]u8 = undefined;
    var lens: [2]usize = undefined;
    var data = message.Data{
        .flags = .complete,
        .ppid = 0,
        .stream_id = 0,
        .stream_seq = 0,
        .tsn = assoc.initial_tsn,
        .user_data = "ping",
    };
    try testing.expectEqual(1, (try testRead(&assoc, testDataPacket2(&assoc, data, &buffer), 0, &lens)).len);
    try assoc.handleTimeout(MessageAssembler.sack_delay);
    try testing.expectEqual(Transmit.sack, assoc.transmits.popFront().?);

    assoc.writeCommonHeader(&buffer);
    var len: usize = message.packet_header_size;
    len += data.write(buffer[len..]);
    data.tsn +%= 1;
    data.stream_seq = 1;
    len += data.write(buffer[len..]);
    Helper.checkSum(buffer[0..len]);

    try testing.expectEqual(1, (try testRead(&assoc, buffer[0..len], MessageAssembler.sack_delay, &lens)).len);
    try testing.expectEqual(Transmit.sack, assoc.transmits.popFront().?);
    try testing.expect(!assoc.message_assembler.send_sack);
}

test "Association.shutdown: an empty queue sends a shutdown right away" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();
    assoc.message_assembler.setInitTsn(100);

    try assoc.shutdown();

    try testing.expectEqual(.shutdown_sent, assoc.state);
    try testing.expectEqual(Transmit{ .shutdown = 99 }, assoc.transmits.popFront().?);
}

test "Association.rwnd: sending subtracts the user data size, not the chunk size" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();
    assoc.message_queue.rwnd = 1000;

    const data: [101]u8 = @splat('a');
    try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    _ = (try assoc.pollTransmit(&out, 0)).?;

    try testing.expectEqual(899, assoc.message_queue.rwnd);
}

test "Association.rwnd: a sack subtracts the outstanding data from the advertised window" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    const data: [100]u8 = @splat('a');
    for (0..3) |_| try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    _ = (try assoc.pollTransmit(&out, 0)).?;

    var buffer: [32]u8 = undefined;
    _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn, &.{}, 1000), 0);
    try testing.expectEqual(800, assoc.message_queue.rwnd);
}

test "Association.rwnd: gap acked chunks are not outstanding" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    const data: [100]u8 = @splat('a');
    for (0..3) |_| try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    _ = (try assoc.pollTransmit(&out, 0)).?;

    // acks the second chunk only
    var buffer: [32]u8 = undefined;
    _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn -% 1, &.{ 2, 2 }, 1000), 0);
    try testing.expectEqual(800, assoc.message_queue.rwnd);
}

test "Association.rwnd: a sack with a window smaller than the outstanding data saturates to zero" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    const data: [100]u8 = @splat('a');
    for (0..3) |_| try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    _ = (try assoc.pollTransmit(&out, 0)).?;

    var buffer: [32]u8 = undefined;
    _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn -% 1, &.{}, 100), 0);
    try testing.expectEqual(0, assoc.message_queue.rwnd);
}

test "Association.rwnd: a timeout adds back the unacked data and the retransmission subtracts it" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    const data: [100]u8 = @splat('a');
    for (0..2) |_| try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    _ = (try assoc.pollTransmit(&out, 0)).?;

    var buffer: [32]u8 = undefined;
    _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn -% 1, &.{ 2, 2 }, 1000), 0);
    try testing.expectEqual(900, assoc.message_queue.rwnd);

    try assoc.handleTimeout(10_000);
    try testing.expectEqual(1000, assoc.message_queue.rwnd);

    _ = (try assoc.pollTransmit(&out, 10_000)).?;
    try testing.expectEqual(900, assoc.message_queue.rwnd);
}

test "Association.rwnd: a second timeout during a retransmission does not add the pending chunks twice" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    const data: [message.MTU - message.packet_header_size - message.data_chunk_header_size]u8 = @splat('a');
    for (0..2) |_| try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    while (try assoc.pollTransmit(&out, 0)) |_| {}

    var buffer: [32]u8 = undefined;
    _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn -% 1, &.{}, 10_000), 0);
    try testing.expectEqual(10_000 - 2 * data.len, assoc.message_queue.rwnd);

    try assoc.handleTimeout(10_000);
    try testing.expectEqual(10_000, assoc.message_queue.rwnd);

    // only the first chunk is retransmitted before the timer expires again
    _ = (try assoc.pollTransmit(&out, 10_000)).?;
    try testing.expectEqual(10_000 - data.len, assoc.message_queue.rwnd);

    try assoc.handleTimeout(20_000);
    try testing.expectEqual(10_000, assoc.message_queue.rwnd);
}

test "Association.rwnd: adding back the outstanding data on a timeout saturates" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    const data: [100]u8 = @splat('a');
    try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    _ = (try assoc.pollTransmit(&out, 0)).?;

    assoc.message_queue.rwnd = std.math.maxInt(u32) - 10;
    try assoc.handleTimeout(10_000);
    try testing.expectEqual(std.math.maxInt(u32), assoc.message_queue.rwnd);
}

test "Association.rwnd: new data is not sent while the window is smaller than the next chunk" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    const data: [100]u8 = @splat('a');
    try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    _ = (try assoc.pollTransmit(&out, 0)).?;

    var buffer: [32]u8 = undefined;
    _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn -% 1, &.{}, 150), 0);
    try testing.expectEqual(50, assoc.message_queue.rwnd);

    try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    try testing.expectEqual(null, try assoc.pollTransmit(&out, 0));

    _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn, &.{}, 1000), 0);
    try testing.expectEqual(1000, assoc.message_queue.rwnd);

    const packet = (try assoc.pollTransmit(&out, 0)).?;
    try testing.expectEqual(assoc.initial_tsn +% 1, std.mem.readInt(u32, packet[16..20], .big));
    try testing.expectEqual(900, assoc.message_queue.rwnd);
}

test "Association.pollTransmit: send zero window probe" {
    var assoc = try testEstablishedAssoc();
    defer assoc.deinit();

    const data: [100]u8 = @splat('a');
    for (0..3) |_| try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    var out: [message.MTU]u8 = undefined;
    _ = (try assoc.pollTransmit(&out, 0)).?;

    var buffer: [32]u8 = undefined;
    _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn +% 2, &.{}, 50), 0);
    try testing.expectEqual(50, assoc.message_queue.rwnd);

    try assoc.handleWrite(&data, .{ .stream_id = 0, .ppid = 0 });
    try testing.expect(try assoc.pollTransmit(&out, 0) != null);
    try testing.expect(assoc.zero_wnd_probe);

    for (0..10) |_| {
        const now = assoc.t3_timer.deadline + 1;
        try assoc.handleTimeout(now);
        try testing.expectEqual(1, assoc.t3_timer.error_count);
        _ = try assoc.pollTransmit(&out, now);
        _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn +% 2, &.{}, 50), now);
        try testing.expectEqual(0, assoc.t3_timer.error_count);
        try testing.expect(assoc.zero_wnd_probe);
    }

    _ = try assoc.handleRead(testSackPacket(&buffer, &assoc, assoc.initial_tsn +% 3, &.{}, 50), 0);
    try testing.expect(!assoc.zero_wnd_probe);
}
