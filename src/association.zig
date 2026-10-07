const std = @import("std");
const message = @import("message.zig");
const SackGenerator = @import("sack_generator.zig");
const SackHandler = @import("sack_handler.zig");
const Reassembler = @import("reassembler.zig");
const MessageQueue = @import("message_queue.zig");
const Helper = @import("helper.zig");

const Association = @This();
const Io = std.Io;

const a_rwnd = 1_000_000;
const sack_delay = 200;
const max_init_retransmits = 8;
const max_init_rto = 60; // in seconds
const max_heartbeat_info = 200;
const initial_rto = std.time.ms_per_s;

pub const Error = error{InvalidState} || std.mem.Allocator.Error;

pub const Config = struct {
    source_port: u16,
    dest_port: u16,
    outbound_streams: u16 = std.math.maxInt(u16),
    inbound_streams: u16 = std.math.maxInt(u16),
    random: std.Random,
};

pub const Event = union(enum) {
    message: message.UserMessage,
    release: []const u8,
    comm_up: void,
    comm_down: void,
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

    rtt_tsn: ?u32,
    rtt_start: i64,
    first_sample: bool,

    const init = RetransmissionTimer{
        .srtt = 0,
        .rttvar = 0,
        .rto = initial_rto,
        .deadline = std.math.maxInt(i64),
        .rtt_tsn = null,
        .rtt_start = 0,
        .first_sample = true,
    };

    fn calculateRto(timer: *RetransmissionTimer, sack: *const SackHandler, now: i64) void {
        if (timer.rtt_tsn == null) return;
        if (!sack.isAcked(timer.rtt_tsn.?)) return;

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

        timer.rto = @min(max_init_rto * std.time.ms_per_s, timer.srtt +| 4 *| timer.rttvar);
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

    fn doubleRto(timer: *RetransmissionTimer, now: i64) void {
        timer.rto = @min(max_init_rto * std.time.ms_per_s, timer.rto *| 2);
        timer.deadline = now + timer.rto;
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
intial_tsn: u32,
peer_initial_tsn: u32,
verification_tag: u32,
peer_verification_tag: u32,
cookie: [32]u8,
peer_cookie: []const u8,
rwnd: u32,
cwnd: u32,

init_deadline: i64,
init_rto: u8,
init_attempts: u8,

sack_generator: SackGenerator,
sack_handler: SackHandler,
send_sack: bool = false,
sack_deadline: i64 = std.math.maxInt(i64),

message_queue: MessageQueue,
reassembler: Reassembler = .init(),
transmits: std.Deque(Transmit),
events: std.Deque(Event) = .empty,

t3_timer: RetransmissionTimer,

pub fn init(allocator: std.mem.Allocator, config: Config) Association {
    const initial_tsn = config.random.int(u32);

    return Association{
        .allocator = allocator,
        .transmits = .empty,
        .state = .closed,
        .source_port = config.source_port,
        .dest_port = config.dest_port,
        .outbound_streams = config.outbound_streams,
        .inbound_streams = config.inbound_streams,
        .intial_tsn = initial_tsn,
        .peer_initial_tsn = 0,
        .verification_tag = config.random.int(u32),
        .peer_verification_tag = 0,
        .cookie = config.random.array(u8, 32),
        .peer_cookie = &.{},
        .rwnd = 0,
        .cwnd = a_rwnd,
        .init_attempts = 0,
        .init_rto = 1,
        .init_deadline = std.math.maxInt(i64),
        .sack_generator = undefined,
        .sack_handler = .init(initial_tsn),
        .message_queue = .init(initial_tsn),
        .t3_timer = .init,
    };
}

pub fn deinit(self: *Association) void {
    self.transmits.deinit(self.allocator);
    while (self.events.popFront()) |event| switch (event) {
        .message => |msg| msg.deinit(self.allocator),
        else => {},
    };
    self.events.deinit(self.allocator);
    self.reassembler.deinit(self.allocator);
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

pub fn handleWrite(self: *Association, data: []const u8, options: message.UserMessageConfig) Error!void {
    std.debug.assert(data.len > 0);
    switch (self.state) {
        .established, .cookie_wait, .cookie_echoed => {},
        else => return error.InvalidState,
    }
    try self.message_queue.pushData(self.allocator, data, options);
}

pub fn handleRead(self: *Association, data: []const u8, now: i64) !void {
    const packet = try message.Packet.parse(data);

    if (packet.source_port != self.dest_port or packet.destination_port != self.source_port) {
        @branchHint(.unlikely);
        return;
    }

    if (!self.isValidVerificationTag(&packet)) {
        @branchHint(.unlikely);
        return;
    }

    var chunk_it = packet.iterateChunks();
    var received_data = false;
    var immediate_sack = false;
    while (try chunk_it.next()) |chunk| switch (chunk) {
        .init => |c| try self.handleInitChunk(c),
        .init_ack => |c| switch (self.state) {
            .cookie_wait => try self.handleInitAckChunk(c),
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

            received_data = true;

            if (d.user_data.len == 0) {
                try self.transmits.pushBack(self.allocator, .{ .abort = d.tsn });
                try self.setStateToClosed();
                return;
            }

            self.sack_generator.receiveTsn(d.tsn) catch |err| {
                std.log.warn("Error for packet {}: {}", .{ d.tsn, err });
                immediate_sack = true;
                continue;
            };

            immediate_sack |= d.flags.immediate;

            if (try self.reassembler.receiveData(self.allocator, d)) |msg| {
                try self.events.pushBack(self.allocator, .{ .message = msg });
            } else while (self.reassembler.drainReady(d.stream_id)) |msg| {
                try self.events.pushBack(self.allocator, .{ .message = msg });
            }
        },
        .sack => |sack| switch (self.state) {
            .established, .shutdown_pending, .shutdown_received, .cookie_echoed => {
                const prev_cumulative_tsn = self.sack_handler.cumulative_tsn;
                self.sack_handler.handleSack(&sack, self.message_queue.curr_tsn -% 1) catch continue;
                self.rwnd = sack.a_rwnd;
                try self.releaseAcknowledged();
                try self.maybeSendShutdown();
                self.t3_timer.calculateRto(&self.sack_handler, now);
                self.updateT3Timer(prev_cumulative_tsn, now);
            },
            else => {},
        },
        .heartbeat => |hb| if (self.state == .established and hb.len <= max_heartbeat_info) {
            try self.transmits.pushBack(self.allocator, .{ .heartbeat_ack = hb });
        },
        .abort => if (self.state != .closed) {
            try self.setStateToClosed();
            return;
        },
        .shutdown => |cumulative_tsn| try self.handleShutdownChunk(cumulative_tsn, now),
        .shutdown_ack => try self.handleShutdownAck(),
        .shutdown_complete => if (self.state == .shutdown_ack_sent) {
            try self.setStateToClosed();
            return;
        },
    };

    if (received_data and self.state == .shutdown_sent) {
        self.resetInitTimer();
        try self.transmits.pushBack(self.allocator, .{ .shutdown = self.sack_generator.cumulative_tsn });
        if (self.sack_generator.cumulative_tsn != self.sack_generator.highest_tsn_received) {
            try self.pushSack();
        }
        return;
    }

    if (!received_data or (self.state != .established and self.state != .shutdown_pending)) return;

    const has_gaps = self.sack_generator.highest_tsn_received != self.sack_generator.cumulative_tsn;
    if (self.send_sack or immediate_sack or has_gaps) {
        try self.pushSack();
    } else {
        self.send_sack = true;
        self.sack_deadline = now + sack_delay;
    }
}

pub fn handleTimeout(self: *Association, now: i64) !void {
    if (now >= self.init_deadline) {
        if (self.init_attempts > max_init_retransmits) {
            try self.setStateToClosed();
            return;
        }

        switch (self.state) {
            .cookie_wait => try self.transmits.pushBack(self.allocator, .init),
            .cookie_echoed => try self.transmits.pushBack(self.allocator, .{ .cookie_echo = self.peer_cookie }),
            .shutdown_sent => try self.transmits.pushBack(self.allocator, .{ .shutdown = self.sack_generator.cumulative_tsn }),
            .shutdown_ack_sent => try self.transmits.pushBack(self.allocator, .shutdown_ack),
            else => {},
        }
    }

    if (now >= self.sack_deadline) try self.pushSack();

    if (now >= self.t3_timer.deadline) {
        self.message_queue.retransmit_index = 0;
        self.t3_timer.doubleRto(now);
        self.t3_timer.clearCurrentSample();
    }
}

pub fn pollEvent(self: *Association) ?Event {
    return self.events.popFront();
}

pub fn pollTransmit(self: *Association, buffer: []u8, now: i64) ?[]const u8 {
    std.debug.assert(buffer.len >= MessageQueue.mtu);

    const chunk_type = self.transmits.popFront() orelse {
        if (self.message_queue.nextRetransmit(&self.sack_handler, buffer[message.packet_header_size..])) |written| {
            self.writeCommonHeader(buffer);
            const packet = buffer[0 .. message.packet_header_size + written];
            Helper.checkSum(packet);
            return packet;
        }

        switch (self.state) {
            .established, .shutdown_received, .shutdown_pending => {},
            else => return null,
        }

        const written = self.message_queue.nextChunk(
            self.allocator,
            buffer[message.packet_header_size..],
        ) orelse return null;
        self.writeCommonHeader(buffer);
        const packet = buffer[0 .. message.packet_header_size + written];
        Helper.checkSum(packet);

        self.t3_timer.setDeadline(now);
        self.t3_timer.setSample(self.message_queue.curr_tsn -% 1, now);
        return packet;
    };

    switch (chunk_type) {
        .init, .cookie_echo, .shutdown, .shutdown_ack => self.setInitTimer(now),
        else => {},
    }

    const written = self.writeControlChunk(chunk_type, buffer);
    return buffer[0..written];
}

pub fn pollTimeout(self: *Association) ?i64 {
    const deadline = @min(@min(self.sack_deadline, self.init_deadline), self.t3_timer.deadline);
    return if (deadline == std.math.maxInt(i64)) null else deadline;
}

fn setStateToClosed(self: *Association) !void {
    const pending_messages = self.message_queue.messages.len;
    try self.events.ensureUnusedCapacity(self.allocator, pending_messages + 1);

    while (self.message_queue.messages.popFront()) |msg| self.events.pushBackAssumeCapacity(.{ .release = msg.data });
    self.events.pushBackAssumeCapacity(.comm_down);

    self.state = .closed;
    self.sack_generator = .init(0);
    self.sack_handler = .init(self.intial_tsn);
    self.sack_deadline = std.math.maxInt(i64);
    self.send_sack = false;
    self.resetInitTimer();
    self.message_queue.close(self.allocator, self.intial_tsn);
    self.reassembler.reset(self.allocator);
    self.t3_timer = .init;
}

fn releaseAcknowledged(self: *Association) !void {
    while (true) {
        try self.events.ensureUnusedCapacity(self.allocator, 1);
        const data = self.message_queue.dropAcknowledged(self.sack_handler.cumulative_tsn) orelse break;
        self.events.pushBackAssumeCapacity(.{ .release = data });
    }
}

fn updateT3Timer(self: *Association, prev_cumulative_tsn: u32, now: i64) void {
    if (self.message_queue.chunks.len == 0) {
        self.t3_timer.stop();
    } else if (self.sack_handler.cumulative_tsn != prev_cumulative_tsn) {
        self.t3_timer.restart(now);
    }
}

fn setStateToEstablished(self: *Association) !void {
    if (self.state == .established) return;
    try self.events.ensureUnusedCapacity(self.allocator, 1);
    self.state = .established;
    self.sack_generator = .init(self.peer_initial_tsn);
    self.events.pushBackAssumeCapacity(.comm_up);
}

fn handleInitChunk(self: *Association, chunk: message.Init) !void {
    try self.transmits.pushBack(self.allocator, .{ .init_ack = chunk.initiate_tag });

    // In cookie wait or cookie echoed state, we should not update the state.
    if (self.state == .closed) {
        self.inbound_streams = @min(self.inbound_streams, chunk.outbound_streams);
        self.outbound_streams = @min(self.outbound_streams, chunk.inbound_streams);
        self.peer_initial_tsn = chunk.initial_tsn;
        self.peer_verification_tag = chunk.initiate_tag;
        self.rwnd = chunk.a_rwnd;
    }
}

fn handleInitAckChunk(self: *Association, chunk: message.Init) !void {
    try self.transmits.ensureUnusedCapacity(self.allocator, 1);

    const cookie = blk: {
        var it = chunk.iterateParameters();
        while (try it.next()) |param| {
            switch (param) {
                .state_cookie => |cookie_param| break :blk cookie_param,
                else => {},
            }
        } else return error.InvalidPacket;
    };

    if (self.peer_cookie.len != 0) {
        self.allocator.free(self.peer_cookie);
        self.peer_cookie = &.{};
    }
    self.peer_cookie = try self.allocator.dupe(u8, cookie);

    self.inbound_streams = @min(self.inbound_streams, chunk.outbound_streams);
    self.outbound_streams = @min(self.outbound_streams, chunk.inbound_streams);
    self.peer_initial_tsn = chunk.initial_tsn;
    self.peer_verification_tag = chunk.initiate_tag;
    self.rwnd = chunk.a_rwnd;
    self.state = .cookie_echoed;
    self.resetInitTimer();
    self.transmits.pushBackAssumeCapacity(.{ .cookie_echo = self.peer_cookie });
}

fn handleShutdownChunk(self: *Association, cumulative_tsn: u32, now: i64) !void {
    switch (self.state) {
        .established, .shutdown_pending, .shutdown_received, .shutdown_sent => {},
        else => return,
    }
    if (Helper.tsnGt(cumulative_tsn, self.message_queue.curr_tsn -% 1)) return;

    const prev_cumulative_tsn = self.sack_handler.cumulative_tsn;
    self.sack_handler.handleCumulativeTsn(cumulative_tsn);
    try self.releaseAcknowledged();
    self.updateT3Timer(prev_cumulative_tsn, now);

    if (self.state == .shutdown_sent) {
        self.state = .shutdown_ack_sent;
        self.resetInitTimer();
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
            try self.transmits.pushBack(self.allocator, .{ .shutdown = self.sack_generator.cumulative_tsn });
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
    self.send_sack = false;
    self.sack_deadline = std.math.maxInt(i64);
}

fn resetInitTimer(self: *Association) void {
    self.init_attempts = 0;
    self.init_rto = 1;
    self.init_deadline = std.math.maxInt(i64);
}

fn setInitTimer(self: *Association, now: i64) void {
    self.init_deadline = now + @as(i64, self.init_rto) * std.time.ms_per_s;
    self.init_rto = @min(self.init_rto * 2, max_init_rto);
    self.init_attempts += 1;
}

fn writeControlChunk(self: *Association, chunk_type: Transmit, buffer: []u8) usize {
    var written: usize = message.packet_header_size;
    self.writeCommonHeader(buffer);

    switch (chunk_type) {
        .init_ack => |tag| std.mem.writeInt(u32, buffer[4..8], tag, .big),
        .shutdown_complete => |reflected| if (reflected) std.mem.writeInt(u32, buffer[4..8], self.verification_tag, .big),
        else => {},
    }

    written += switch (chunk_type) {
        .init => self.writeInit(buffer[written..]),
        .init_ack => self.writeInitAck(buffer[written..]),
        .cookie_echo => |cookie| self.writeCookieEcho(cookie, buffer[written..]),
        .cookie_ack => self.writeCookieAck(buffer[written..]),
        .abort => |tsn| self.writeNoUserDataAbort(tsn, buffer[written..]),
        .sack => self.writeSack(buffer[written..]),
        .heartbeat_ack => |data| writeHeartbeatAck(data, buffer[written..]),
        .shutdown => |tsn| writeShutdown(buffer[written..], tsn),
        .shutdown_ack => writeShutdownAck(buffer[written..]),
        .shutdown_complete => |reflected| writeShutdownComplete(buffer[written..], reflected),
    };

    Helper.checkSum(buffer[0..written]);
    return written;
}

fn writeSack(self: *Association, buffer: []u8) usize {
    const wnd = a_rwnd -| self.reassembler.buffered_data;
    const written = self.sack_generator.writeSack(buffer, wnd);
    self.sack_generator.clearDuplicates();
    return written;
}

fn writeInit(self: *Association, buffer: []u8) usize {
    buffer[0] = @intFromEnum(message.ChunkType.init);
    buffer[1] = 0; // flags
    std.mem.writeInt(u16, buffer[2..4], 20, .big);
    std.mem.writeInt(u32, buffer[4..8], self.verification_tag, .big);
    std.mem.writeInt(u32, buffer[8..12], a_rwnd, .big);
    std.mem.writeInt(u16, buffer[12..14], self.outbound_streams, .big);
    std.mem.writeInt(u16, buffer[14..16], self.inbound_streams, .big);
    std.mem.writeInt(u32, buffer[16..20], self.intial_tsn, .big);

    return 20;
}

fn writeInitAck(self: *Association, buffer: []u8) usize {
    buffer[0] = @intFromEnum(message.ChunkType.init_ack);
    buffer[1] = 0; // flags

    std.mem.writeInt(u32, buffer[4..8], self.verification_tag, .big);
    std.mem.writeInt(u32, buffer[8..12], a_rwnd, .big);
    std.mem.writeInt(u16, buffer[12..14], self.outbound_streams, .big);
    std.mem.writeInt(u16, buffer[14..16], self.inbound_streams, .big);
    std.mem.writeInt(u32, buffer[16..20], self.intial_tsn, .big);

    const written = message.Parameter.writeBuffer(.{ .state_cookie = &self.cookie }, buffer[20..]);
    std.mem.writeInt(u16, buffer[2..4], @intCast(20 + written), .big);
    return 20 + written;
}

fn writeCookieEcho(self: *Association, cookie: []const u8, buffer: []u8) usize {
    _ = self;
    buffer[0] = @intFromEnum(message.ChunkType.cookie_echo);
    buffer[1] = 0; // flags
    std.mem.writeInt(u16, buffer[2..4], @intCast(4 + cookie.len), .big);
    @memcpy(buffer[4..][0..cookie.len], cookie);

    return 4 + cookie.len;
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

fn writeHeartbeatAck(data: []const u8, buffer: []u8) usize {
    buffer[0] = @intFromEnum(message.ChunkType.heartbeat_ack);
    buffer[1] = 0; // flags
    const written = message.Parameter.writeBuffer(.{ .heartbeat_info = data }, buffer[4..]);
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

fn testSackPacket(buffer: *[32]u8, vtag: u32, cumulative_tsn: u32, gap: ?[2]u16) []const u8 {
    const len: u16 = if (gap != null) 20 else 16;
    std.mem.writeInt(u16, buffer[0..2], 5000, .big);
    std.mem.writeInt(u16, buffer[2..4], 5000, .big);
    std.mem.writeInt(u32, buffer[4..8], vtag, .big);
    std.mem.writeInt(u32, buffer[8..12], 0, .big);
    buffer[12] = @intFromEnum(message.ChunkType.sack);
    buffer[13] = 0;
    std.mem.writeInt(u16, buffer[14..16], len, .big);
    std.mem.writeInt(u32, buffer[16..20], cumulative_tsn, .big);
    std.mem.writeInt(u32, buffer[20..24], a_rwnd, .big);
    std.mem.writeInt(u16, buffer[24..26], @intFromBool(gap != null), .big);
    std.mem.writeInt(u16, buffer[26..28], 0, .big);
    if (gap) |g| {
        std.mem.writeInt(u16, buffer[28..30], g[0], .big);
        std.mem.writeInt(u16, buffer[30..32], g[1], .big);
    }
    const packet = buffer[0 .. message.packet_header_size + len];
    Helper.checkSum(packet);
    return packet;
}

test "Association.pollTransmits: a timed out message that was gap acked is not retransmitted" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    try assoc.setStateToEstablished();
    _ = assoc.pollEvent();

    try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });
    try assoc.handleWrite("bbbb", .{ .stream_id = 0, .ppid = 0 });
    var out: [MessageQueue.mtu]u8 = undefined;
    while (assoc.pollTransmit(&out, 0)) |_| {}

    var buffer: [32]u8 = undefined;
    try assoc.handleRead(testSackPacket(&buffer, assoc.verification_tag, assoc.intial_tsn -% 1, .{ 2, 2 }), 0);
    try assoc.handleTimeout(10_000);

    const packet = assoc.pollTransmit(&out, 10_000).?;
    try testing.expectEqual(assoc.intial_tsn, std.mem.readInt(u32, packet[16..20], .big));
    try testing.expectEqual(null, assoc.pollTransmit(&out, 10_000));
}

test "Association.pollTransmit: control chunks are sent before retransmissions" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    assoc.peer_initial_tsn = 100;
    try assoc.setStateToEstablished();
    _ = assoc.pollEvent();

    try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });
    var out: [MessageQueue.mtu]u8 = undefined;
    while (assoc.pollTransmit(&out, 0)) |_| {}

    var buffer: [32]u8 = undefined;
    try assoc.handleRead(testDataPacket(&buffer, assoc.verification_tag, 100), 0);
    assoc.pollEvent().?.message.deinit(testing.allocator);

    // both the delayed sack and T3 expire
    try assoc.handleTimeout(10_000);

    const sack = assoc.pollTransmit(&out, 10_000).?;
    try testing.expectEqual(@intFromEnum(message.ChunkType.sack), sack[12]);

    const retransmit = assoc.pollTransmit(&out, 10_000).?;
    try testing.expectEqual(@intFromEnum(message.ChunkType.data), retransmit[12]);
    try testing.expectEqual(assoc.intial_tsn, std.mem.readInt(u32, retransmit[16..20], .big));
}

test "Association.handleRead: a sack acknowledging unsent tsns is ignored" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    try assoc.setStateToEstablished();
    _ = assoc.pollEvent();

    try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });
    try assoc.handleWrite("bbbb", .{ .stream_id = 0, .ppid = 0 });
    var out: [MessageQueue.mtu]u8 = undefined;
    _ = assoc.pollTransmit(&out, 0).?;

    var buffer: [32]u8 = undefined;
    try assoc.handleRead(testSackPacket(&buffer, assoc.verification_tag, assoc.intial_tsn +% 1, null), 0);
    try assoc.handleRead(testSackPacket(&buffer, assoc.verification_tag, assoc.intial_tsn -% 1, .{ 1, 2 }), 0);

    try testing.expectEqual(null, assoc.pollEvent());
    try testing.expectEqual(2, assoc.message_queue.messages.len);
    try testing.expect(assoc.pollTransmit(&out, 0) != null);
}

test "Association.handleRead: a shutdown acknowledging unsent tsns is ignored" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    try assoc.setStateToEstablished();
    _ = assoc.pollEvent();

    try assoc.handleWrite("aaaa", .{ .stream_id = 0, .ppid = 0 });

    var buffer: [20]u8 = undefined;
    std.mem.writeInt(u16, buffer[0..2], 5000, .big);
    std.mem.writeInt(u16, buffer[2..4], 5000, .big);
    std.mem.writeInt(u32, buffer[4..8], assoc.verification_tag, .big);
    std.mem.writeInt(u32, buffer[8..12], 0, .big);
    buffer[12] = @intFromEnum(message.ChunkType.shutdown);
    buffer[13] = 0;
    std.mem.writeInt(u16, buffer[14..16], 8, .big);
    std.mem.writeInt(u32, buffer[16..20], assoc.intial_tsn, .big);
    Helper.checkSum(&buffer);
    try assoc.handleRead(&buffer, 0);

    try testing.expectEqual(.established, assoc.state);
    try testing.expectEqual(1, assoc.message_queue.messages.len);
}

test "Association.handleRead: Abort with the T bit and the peer tag closes the association" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    assoc.state = .established;
    assoc.peer_verification_tag = 0x11223344;

    var buffer: [16]u8 = undefined;
    try assoc.handleRead(testPacket(&buffer, 0x11223344, .abort, 0x01), 0);

    try testing.expectEqual(.closed, assoc.state);
    try testing.expect(assoc.pollEvent().? == .comm_down);
}

test "Association.handleRead: Abort with the T bit and our own tag is ignored" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    assoc.state = .established;
    assoc.peer_verification_tag = 0x11223344;

    var buffer: [16]u8 = undefined;
    try assoc.handleRead(testPacket(&buffer, assoc.verification_tag, .abort, 0x01), 0);

    try testing.expectEqual(.established, assoc.state);
    try testing.expectEqual(null, assoc.pollEvent());
}

test "Association.handleRead: shutdown complete with the T bit and the peer tag closes the association" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    assoc.state = .shutdown_ack_sent;
    assoc.peer_verification_tag = 0x11223344;

    var buffer: [16]u8 = undefined;
    try assoc.handleRead(testPacket(&buffer, 0x11223344, .shutdown_complete, 0x01), 0);

    try testing.expectEqual(.closed, assoc.state);
}

test "Association.handleRead: data in shutdown sent is answered with a shutdown" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    assoc.peer_initial_tsn = 100;
    try assoc.setStateToEstablished();
    _ = assoc.pollEvent();
    assoc.state = .shutdown_sent;

    var buffer: [32]u8 = undefined;
    try assoc.handleRead(testDataPacket(&buffer, assoc.verification_tag, 100), 0);

    try testing.expectEqual(.shutdown_sent, assoc.state);
    const event = assoc.pollEvent().?;
    try testing.expect(event == .message);
    event.message.deinit(testing.allocator);
    try testing.expectEqual(Transmit{ .shutdown = 100 }, assoc.transmits.popFront().?);
}

test "Association.handleRead: data in shutdown pending is delivered and acknowledged" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    assoc.peer_initial_tsn = 100;
    try assoc.setStateToEstablished();
    _ = assoc.pollEvent();
    assoc.state = .shutdown_pending;

    var buffer: [32]u8 = undefined;
    try assoc.handleRead(testDataPacket(&buffer, assoc.verification_tag, 100), 0);

    const event = assoc.pollEvent().?;
    try testing.expect(event == .message);
    event.message.deinit(testing.allocator);
    try testing.expect(assoc.send_sack);
}

test "Association.handleRead: shutdown in shutdown pending moves to shutdown received" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    assoc.state = .shutdown_pending;

    var buffer: [20]u8 = undefined;
    std.mem.writeInt(u16, buffer[0..2], 5000, .big);
    std.mem.writeInt(u16, buffer[2..4], 5000, .big);
    std.mem.writeInt(u32, buffer[4..8], assoc.verification_tag, .big);
    std.mem.writeInt(u32, buffer[8..12], 0, .big);
    buffer[12] = @intFromEnum(message.ChunkType.shutdown);
    buffer[13] = 0;
    std.mem.writeInt(u16, buffer[14..16], 8, .big);
    std.mem.writeInt(u32, buffer[16..20], assoc.intial_tsn -% 1, .big);
    Helper.checkSum(&buffer);
    try assoc.handleRead(&buffer, 0);

    try testing.expectEqual(.shutdown_ack_sent, assoc.state);
    try testing.expectEqual(Transmit.shutdown_ack, assoc.transmits.popFront().?);
}

test "Association.handleRead: shutdown ack in shutdown ack sent closes the association" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    assoc.state = .shutdown_ack_sent;

    var buffer: [16]u8 = undefined;
    try assoc.handleRead(testPacket(&buffer, assoc.verification_tag, .shutdown_ack, 0), 0);

    try testing.expectEqual(.closed, assoc.state);
    try testing.expectEqual(Transmit{ .shutdown_complete = false }, assoc.transmits.popFront().?);
}

test "Association.handleRead: out of the blue shutdown ack is answered with a reflected shutdown complete" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    assoc.peer_verification_tag = 0x11223344;

    var buffer: [16]u8 = undefined;
    try assoc.handleRead(testPacket(&buffer, assoc.verification_tag, .shutdown_ack, 0), 0);

    try testing.expectEqual(.closed, assoc.state);
    try testing.expectEqual(null, assoc.pollEvent());

    var out: [MessageQueue.mtu]u8 = undefined;
    const packet = assoc.pollTransmit(&out, 0).?;
    try testing.expectEqual(assoc.verification_tag, std.mem.readInt(u32, packet[4..8], .big));
    try testing.expectEqual(@intFromEnum(message.ChunkType.shutdown_complete), packet[12]);
    try testing.expectEqual(0x01, packet[13]);
}

test "Association.shutdown: an empty queue sends a shutdown right away" {
    var assoc = Association.init(testing.allocator, .{ .source_port = 5000, .dest_port = 5000, .random = test_prng.random() });
    defer assoc.deinit();
    assoc.peer_initial_tsn = 100;
    try assoc.setStateToEstablished();

    try assoc.shutdown();

    try testing.expectEqual(.shutdown_sent, assoc.state);
    try testing.expectEqual(Transmit{ .shutdown = 99 }, assoc.transmits.popFront().?);
}
