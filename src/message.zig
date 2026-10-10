const std = @import("std");

const Helper = @import("helper.zig");

pub const packet_header_size = 12;
pub const data_chunk_header_size = 16;
pub const mtu = 1200;

pub const Packet = struct {
    source_port: u16,
    destination_port: u16,
    verification_tag: u32,
    checksum: u32,
    chunks: []const u8,

    pub fn parse(data: []const u8) error{InvalidPacket}!Packet {
        if (data.len < 12) {
            @branchHint(.unlikely);
            return error.InvalidPacket;
        }

        const source_port = std.mem.readInt(u16, data[0..2], .big);
        const destination_port = std.mem.readInt(u16, data[2..4], .big);
        const verification_tag = std.mem.readInt(u32, data[4..8], .big);
        const checksum = std.mem.readInt(u32, data[8..12], .little);

        if (Helper.getCheckSum(data) != checksum) return error.InvalidPacket;

        const chunks = data[12..];

        return Packet{
            .source_port = source_port,
            .destination_port = destination_port,
            .verification_tag = verification_tag,
            .checksum = checksum,
            .chunks = chunks,
        };
    }

    pub fn iterateChunks(packet: *const Packet) ChunkIterator {
        return ChunkIterator.init(packet.chunks);
    }
};

pub const ChunkIterator = struct {
    slice: []const u8,

    pub fn init(slice: []const u8) ChunkIterator {
        return ChunkIterator{ .slice = slice };
    }

    pub fn next(self: *ChunkIterator) !?Chunk {
        while (self.slice.len != 0) {
            if (try self.parseChunk()) |chunk| return chunk;
        }
        return null;
    }

    pub const ParseError = error{
        /// Parsing failed due to insufficient data or invalid length.
        InvalidChunk,
        /// Chunk type is not recognized, stop processing the remaining chunks.
        UnrecognizedChunkType,
    };

    fn parseChunk(self: *ChunkIterator) ParseError!?Chunk {
        if (self.slice.len < 4) return error.InvalidChunk;

        const chunk_type: ChunkType = @enumFromInt(self.slice[0]);
        const flags = self.slice[1];
        const length = std.mem.readInt(u16, self.slice[2..4], .big);
        const padded_length = (length + 3) & ~@as(u16, 3); // Align to 4 bytes

        if (length < 4 or padded_length > self.slice.len) return error.InvalidChunk;

        var chunk: ?Chunk = null;
        switch (chunk_type) {
            .init, .init_ack => {
                const init_chunk = try Init.parse(self.slice[4..length]);
                chunk = if (chunk_type == .init) .{ .init = init_chunk } else .{ .init_ack = init_chunk };
            },
            .sack => chunk = .{ .sack = try Sack.parse(self.slice[4..length]) },
            .cookie_echo => chunk = .{ .cookie_echo = self.slice[4..length] },
            .cookie_ack => {
                if (length != 4) return error.InvalidChunk;
                chunk = .cookie_ack;
            },
            .data => chunk = .{ .data = try Data.parse(flags, self.slice[4..length]) },
            .heartbeat => chunk = .{ .heartbeat = try parseHeartbeat(self.slice[4..length]) },
            .abort => chunk = .{ .abort = Abort.parse(flags, self.slice[4..length]) },
            .shutdown => {
                if (length != 8) return error.InvalidChunk;
                chunk = .{ .shutdown = std.mem.readInt(u32, self.slice[4..8], .big) };
            },
            .shutdown_complete => {
                if (length != 4) return error.InvalidChunk;
                chunk = .{ .shutdown_complete = (flags & 0x01) != 0 };
            },
            .shutdown_ack => {
                if (length != 4) return error.InvalidChunk;
                chunk = .shutdown_ack;
            },
            .heartbeat_ack, .@"error", .ecne, .cwr => {},
            _ => if (self.slice[0] & 0x80 == 0) return error.UnrecognizedChunkType,
        }

        self.slice = self.slice[padded_length..];
        return chunk;
    }
};

pub const ChunkType = enum(u8) {
    data = 0,
    init,
    init_ack,
    sack,
    heartbeat,
    heartbeat_ack,
    abort,
    shutdown,
    shutdown_ack,
    @"error",
    cookie_echo,
    cookie_ack,
    ecne,
    cwr,
    shutdown_complete,
    _,
};

pub const Chunk = union(enum) {
    data: Data,
    init: Init,
    init_ack: Init,
    sack: Sack,
    cookie_echo: []const u8,
    cookie_ack: void,
    heartbeat: []const u8,
    abort: Abort,
    shutdown: u32,
    shutdown_complete: bool, // T bit
    shutdown_ack,
};

pub const Init = struct {
    initiate_tag: u32,
    a_rwnd: u32,
    outbound_streams: u16,
    inbound_streams: u16,
    initial_tsn: u32,
    parameters: []const u8,

    pub fn parse(data: []const u8) !Init {
        if (data.len < 16) {
            @branchHint(.unlikely);
            return error.InvalidChunk;
        }

        return Init{
            .initiate_tag = std.mem.readInt(u32, data[0..4], .big),
            .a_rwnd = std.mem.readInt(u32, data[4..8], .big),
            .outbound_streams = std.mem.readInt(u16, data[8..10], .big),
            .inbound_streams = std.mem.readInt(u16, data[10..12], .big),
            .initial_tsn = std.mem.readInt(u32, data[12..16], .big),
            .parameters = data[16..],
        };
    }

    pub fn iterateParameters(init: *const Init) ParameterIterator {
        return ParameterIterator.init(init.parameters);
    }
};

pub const Data = struct {
    flags: Flags,
    tsn: u32,
    stream_id: u16,
    stream_seq: u16,
    ppid: u32,
    user_data: []const u8,

    pub const Flags = packed struct(u8) {
        end_fragment: bool = false,
        start_fragment: bool = false,
        unordered: bool = false,
        immediate: bool = false,
        _pad: u4 = 0,
    };

    pub fn parse(flags: u8, data: []const u8) error{InvalidChunk}!Data {
        if (data.len < 12) {
            @branchHint(.unlikely);
            return error.InvalidChunk;
        }

        return Data{
            .flags = @bitCast(flags),
            .tsn = std.mem.readInt(u32, data[0..4], .big),
            .stream_id = std.mem.readInt(u16, data[4..6], .big),
            .stream_seq = std.mem.readInt(u16, data[6..8], .big),
            .ppid = std.mem.readInt(u32, data[8..12], .big),
            .user_data = data[12..],
        };
    }

    pub fn write(self: *const Data, buffer: []u8) usize {
        buffer[0] = @intFromEnum(ChunkType.data);
        buffer[1] = @bitCast(self.flags);
        std.mem.writeInt(u16, buffer[2..4], @intCast(self.user_data.len + data_chunk_header_size), .big);
        std.mem.writeInt(u32, buffer[4..8], self.tsn, .big);
        std.mem.writeInt(u16, buffer[8..10], self.stream_id, .big);
        std.mem.writeInt(u16, buffer[10..12], self.stream_seq, .big);
        std.mem.writeInt(u32, buffer[12..16], self.ppid, .big);
        @memcpy(buffer[data_chunk_header_size..][0..self.user_data.len], self.user_data);

        const pad = (4 - (self.user_data.len % 4)) % 4;
        @memset(buffer[data_chunk_header_size + self.user_data.len ..][0..pad], 0);
        return data_chunk_header_size + self.user_data.len + pad;
    }

    pub fn isComplete(self: *const Data) bool {
        return self.flags.start_fragment and self.flags.end_fragment;
    }

    pub fn toUserMessage(chunk: *const Data) UserMessage {
        return UserMessage{
            .stream_id = chunk.stream_id,
            .stream_seq = chunk.stream_seq,
            .ppid = chunk.ppid,
            .unordered = chunk.flags.unordered,
            .data = chunk.user_data,
        };
    }
};

pub const Sack = struct {
    cumulative_tsn: u32,
    a_rwnd: u32,
    gap_ack_blocks: []const u8,
    duplicate_tsns: []const u8,

    pub fn parse(data: []const u8) error{InvalidChunk}!Sack {
        if (data.len < 12) {
            @branchHint(.unlikely);
            return error.InvalidChunk;
        }

        const num_gap_ack_blocks = std.mem.readInt(u16, data[8..10], .big);
        const num_duplicate_tsns = std.mem.readInt(u16, data[10..12], .big);

        const expected_length = @as(u32, num_gap_ack_blocks + num_duplicate_tsns) * 4 + 12;
        if (data.len != expected_length) {
            @branchHint(.unlikely);
            return error.InvalidChunk;
        }

        return Sack{
            .cumulative_tsn = std.mem.readInt(u32, data[0..4], .big),
            .a_rwnd = std.mem.readInt(u32, data[4..8], .big),
            .gap_ack_blocks = data[12..(12 + num_gap_ack_blocks * 4)],
            .duplicate_tsns = data[(12 + num_gap_ack_blocks * 4)..expected_length],
        };
    }

    pub fn iterateGapAckBlocks(sack: *const Sack) GapAckBlockIterator {
        return GapAckBlockIterator.init(sack.gap_ack_blocks, sack.cumulative_tsn);
    }

    pub const GapAckBlockIterator = struct {
        slice: []const u8,
        tsn: u32,
        offset: u32,
        end: u32,

        pub fn init(slice: []const u8, tsn: u32) GapAckBlockIterator {
            return GapAckBlockIterator{
                .slice = slice,
                .tsn = tsn,
                .offset = 1,
                .end = 0,
            };
        }

        pub const Block = struct { start: u32, end: u32 };

        pub fn nextBlock(self: *GapAckBlockIterator) error{InvalidGapAckBlock}!?Block {
            if (self.slice.len == 0) return null;
            if (self.slice.len < 4) return error.InvalidGapAckBlock;

            const start = std.mem.readInt(u16, self.slice[0..2], .big);
            const end = std.mem.readInt(u16, self.slice[2..4], .big);
            if (start == 0 or start > end) return error.InvalidGapAckBlock;

            self.slice = self.slice[4..];
            return .{ .start = self.tsn +% start, .end = self.tsn +% end };
        }

        pub fn next(self: *GapAckBlockIterator) error{InvalidGapAckBlock}!?u32 {
            if (self.offset > self.end) {
                const block = try self.nextBlock() orelse return null;
                self.offset = block.start -% self.tsn;
                self.end = block.end -% self.tsn;
            }

            const tsn = self.tsn +% self.offset;
            self.offset += 1;
            return tsn;
        }
    };
};

pub const Abort = struct {
    verification_tag_reflected: bool,
    errors: []const u8,

    pub fn parse(flags: u8, data: []const u8) Abort {
        return Abort{
            .verification_tag_reflected = (flags & 0x01) != 0,
            .errors = data,
        };
    }
};

pub const ErrorCode = enum(u16) {
    invalid_stream_id = 1,
    missing_mandatory_param,
    stale_cookie,
    out_of_resource,
    unresolvable_address,
    unrecognized_chunk_type,
    invalid_mandatory_param,
    unrecognized_parameters,
    no_user_data,
    cookie_received_while_shutting_down,
    restart_of_an_association,
    user_initiated_abort,
    protocol_violation,

    pub fn fromInt(t: u16) !ErrorCode {
        switch (t) {
            1...13 => return @enumFromInt(t),
            else => return error.InvalidErrorCode,
        }
    }
};

pub const Error = union(ErrorCode) {
    invalid_stream_id: void,
    missing_mandatory_param: void,
    stale_cookie: void,
    out_of_resource: void,
    unresolvable_address: void,
    unrecognized_chunk_type: void,
    invalid_mandatory_param: void,
    unrecognized_parameters: void,
    no_user_data: u32,
    cookie_received_while_shutting_down: void,
    restart_of_an_association: void,
    user_initiated_abort: void,
    protocol_violation: void,

    pub fn writeNoUserDataErrorBuffer(buffer: []u8, tsn: u32) usize {
        std.mem.writeInt(u16, buffer[0..2], @intFromEnum(ErrorCode.no_user_data), .big);
        std.mem.writeInt(u16, buffer[2..4], 8, .big);
        std.mem.writeInt(u32, buffer[4..8], tsn, .big);
        return 8;
    }
};

pub const ParameterType = enum(u16) {
    heartbeat_info = 0x0001,
    state_cookie = 0x0007,
    _,
};

pub const Parameter = union(enum) {
    heartbeat_info: []const u8,
    state_cookie: []const u8,
    unknown: struct { param_type: u16, value: []const u8 },

    pub fn writeBuffer(self: Parameter, buffer: []u8) usize {
        return switch (self) {
            .heartbeat_info => |value| writeTLV(ParameterType.heartbeat_info, value, buffer),
            .state_cookie => |value| writeTLV(ParameterType.state_cookie, value, buffer),
            .unknown => |param| writeTLV(@enumFromInt(param.param_type), param.value, buffer),
        };
    }

    fn writeTLV(typ: ParameterType, data: []const u8, buffer: []u8) usize {
        std.mem.writeInt(u16, buffer[0..2], @intFromEnum(typ), .big);
        std.mem.writeInt(u16, buffer[2..4], @intCast(data.len + 4), .big);
        @memcpy(buffer[4..][0..data.len], data);

        const pad = (4 - (data.len % 4)) % 4;
        @memset(buffer[4 + data.len ..][0..pad], 0);
        return data.len + 4 + pad;
    }
};

pub const ParameterIterator = struct {
    slice: []const u8,

    pub fn init(slice: []const u8) ParameterIterator {
        return ParameterIterator{ .slice = slice };
    }

    pub fn next(self: *ParameterIterator) !?Parameter {
        if (self.slice.len == 0) return null;
        if (self.slice.len < 4) return error.InvalidParameter;

        const param_type: ParameterType = @enumFromInt(std.mem.readInt(u16, self.slice[0..2], .big));
        const length = std.mem.readInt(u16, self.slice[2..4], .big);
        const padded_length = (length + 3) & ~@as(u16, 3); // Align to 4 bytes

        if (length < 4 or length > self.slice.len) return error.InvalidParameter;

        const value = self.slice[4..length];
        self.slice = if (self.slice.len >= padded_length) self.slice[padded_length..] else self.slice[length..];

        return switch (param_type) {
            .heartbeat_info => .{ .heartbeat_info = value },
            .state_cookie => .{ .state_cookie = value },
            else => .{ .unknown = .{ .param_type = @intFromEnum(param_type), .value = value } },
        };
    }
};

pub const UserMessageConfig = struct {
    stream_id: u16,
    ppid: u32,
    unordered: bool = false,
};

pub const UserMessage = struct {
    stream_id: u16,
    stream_seq: u16,
    ppid: u32,
    unordered: bool,
    data: []const u8,

    pub fn deinit(self: UserMessage, allocator: std.mem.Allocator) void {
        allocator.free(self.data);
    }
};

fn parseHeartbeat(data: []const u8) error{InvalidChunk}![]const u8 {
    var it = ParameterIterator.init(data);
    const param = (it.next() catch return error.InvalidChunk) orelse return error.InvalidChunk;
    if (param != .heartbeat_info) return error.InvalidChunk;
    return param.heartbeat_info;
}

fn collectGapAcks(blocks: []const u8, out: []u32) ![]u32 {
    const sack = Sack{ .cumulative_tsn = 10, .a_rwnd = 0, .gap_ack_blocks = blocks, .duplicate_tsns = &.{} };
    var it = sack.iterateGapAckBlocks();
    var n: usize = 0;
    while (try it.next()) |tsn| : (n += 1) out[n] = tsn;
    return out[0..n];
}

test "Packet.parse" {
    const packet = [_]u8{
        0x13, 0x88, 0x13, 0x88, 0x00, 0x00, 0x00,
        0x00, 0x99, 0x6e, 0x24, 0x4d, 0x01, 0x00,
        0x00, 0x14, 0x12, 0x34, 0x56, 0x78, 0x00,
        0x02, 0x00, 0x00, 0x04, 0x00, 0x04, 0x00,
        0xab, 0xcd, 0xef, 0x01,
    };

    {
        const p = try Packet.parse(packet[0..]);
        try std.testing.expectEqual(5000, p.source_port);
        try std.testing.expectEqual(5000, p.destination_port);
        try std.testing.expectEqual(0, p.verification_tag);
        try std.testing.expectEqual(0x4d246e99, p.checksum);
        try std.testing.expectEqualSlices(u8, packet[packet_header_size..], p.chunks);
    }

    {
        try std.testing.expectError(error.InvalidPacket, Packet.parse(packet[0..11]));
    }

    {
        var p = packet;
        p[8] = 0x00; // Corrupt the checksum
        try std.testing.expectError(error.InvalidPacket, Packet.parse(p[0..]));
    }
}

test "Data.parse" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x2a, 0x00, 0x03, 0x00, 0x07,
        0x00, 0x00, 0x00, 0x33, 'h',  'e',  'l',  'l',
        'o',
    };

    {
        const d = try Data.parse(0x03, &payload);
        try std.testing.expectEqual(Data.Flags{ .end_fragment = true, .start_fragment = true }, d.flags);
        try std.testing.expectEqual(42, d.tsn);
        try std.testing.expectEqual(3, d.stream_id);
        try std.testing.expectEqual(7, d.stream_seq);
        try std.testing.expectEqual(51, d.ppid);
        try std.testing.expectEqualSlices(u8, "hello", d.user_data);
    }

    {
        const d = try Data.parse(0x0c, &payload);
        try std.testing.expectEqual(Data.Flags{ .unordered = true, .immediate = true }, d.flags);
    }

    {
        const d = try Data.parse(0, payload[0..12]);
        try std.testing.expectEqual(0, d.user_data.len);
    }

    {
        try std.testing.expectError(error.InvalidChunk, Data.parse(0, payload[0..11]));
    }
}

test "Data.write" {
    {
        const d = Data{
            .flags = .{ .end_fragment = true, .start_fragment = true, .unordered = true },
            .tsn = 0x01020304,
            .stream_id = 0x0506,
            .stream_seq = 0x0708,
            .ppid = 0x090a0b0c,
            .user_data = "hello",
        };

        var buffer: [32]u8 = @splat(0xff);
        const n = d.write(&buffer);
        try std.testing.expectEqual(24, n);
        try std.testing.expectEqualSlices(u8, &.{
            0x00, 0x07, 0x00, 0x15, 0x01, 0x02, 0x03, 0x04,
            0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b, 0x0c,
            'h',  'e',  'l',  'l',  'o',  0x00, 0x00, 0x00,
        }, buffer[0..n]);
        try std.testing.expectEqual(0xff, buffer[n]);
    }

    {
        const d = Data{ .flags = .{}, .tsn = 1, .stream_id = 0, .stream_seq = 0, .ppid = 0, .user_data = "abcd" };
        var buffer: [20]u8 = undefined;
        const n = d.write(&buffer);
        try std.testing.expectEqual(20, n);
        try std.testing.expectEqual(20, std.mem.readInt(u16, buffer[2..4], .big));
        try std.testing.expectEqualSlices(u8, "abcd", buffer[16..20]);
    }
}

test "Data round trip through ChunkIterator" {
    const original = Data{
        .flags = .{ .start_fragment = true },
        .tsn = 0xfffffffe,
        .stream_id = 9,
        .stream_seq = 65535,
        .ppid = 46,
        .user_data = "sctp!",
    };

    var buffer: [64]u8 = undefined;
    var n = original.write(&buffer);
    n += original.write(buffer[n..]);

    var it = ChunkIterator.init(buffer[0..n]);
    for (0..2) |_| {
        const chunk = (try it.next()).?;
        const d = chunk.data;
        try std.testing.expectEqual(original.flags, d.flags);
        try std.testing.expectEqual(original.tsn, d.tsn);
        try std.testing.expectEqual(original.stream_id, d.stream_id);
        try std.testing.expectEqual(original.stream_seq, d.stream_seq);
        try std.testing.expectEqual(original.ppid, d.ppid);
        try std.testing.expectEqualSlices(u8, original.user_data, d.user_data);
    }
    try std.testing.expectEqual(null, try it.next());
}

test "Sack.parse" {
    const payload = [_]u8{
        0x00, 0x00, 0x00, 0x64, 0x00, 0x01, 0x00, 0x00,
        0x00, 0x02, 0x00, 0x01, 0x00, 0x02, 0x00, 0x03,
        0x00, 0x05, 0x00, 0x07, 0x00, 0x00, 0x00, 0x63,
    };

    {
        const s = try Sack.parse(&payload);
        try std.testing.expectEqual(100, s.cumulative_tsn);
        try std.testing.expectEqual(0x10000, s.a_rwnd);
        try std.testing.expectEqualSlices(u8, payload[12..20], s.gap_ack_blocks);
        try std.testing.expectEqualSlices(u8, payload[20..24], s.duplicate_tsns);
    }

    {
        const s = try Sack.parse(&.{ 0, 0, 0, 1, 0, 0, 0x10, 0, 0, 0, 0, 0 });
        try std.testing.expectEqual(1, s.cumulative_tsn);
        try std.testing.expectEqual(0x1000, s.a_rwnd);
        try std.testing.expectEqual(0, s.gap_ack_blocks.len);
        try std.testing.expectEqual(0, s.duplicate_tsns.len);
    }

    {
        const s = try Sack.parse(&.{ 0, 0, 0, 1, 0, 0, 0, 0, 0, 0, 0, 1, 0, 0, 0, 1 });
        try std.testing.expectEqual(0, s.gap_ack_blocks.len);
        try std.testing.expectEqualSlices(u8, &.{ 0, 0, 0, 1 }, s.duplicate_tsns);
    }

    try std.testing.expectError(error.InvalidChunk, Sack.parse(payload[0..11]));
    try std.testing.expectError(error.InvalidChunk, Sack.parse(payload[0..20]));

    {
        const extended = payload ++ [_]u8{ 0, 0, 0, 0 };
        try std.testing.expectError(error.InvalidChunk, Sack.parse(&extended));
    }
}

test "Sack.iterateGapAckBlocks" {
    // yields every tsn of every block
    {
        var out: [8]u32 = undefined;
        const acked = try collectGapAcks(&.{ 0, 2, 0, 3, 0, 5, 0, 5 }, &out);
        try std.testing.expectEqualSlices(u32, &.{ 12, 13, 15 }, acked);
    }

    // handles a block ending at the maximum offset
    {
        var out: [2]u32 = undefined;
        const acked = try collectGapAcks(&.{ 0xFF, 0xFE, 0xFF, 0xFF }, &out);
        try std.testing.expectEqualSlices(u32, &.{ 10 +% 0xFFFE, 10 +% 0xFFFF }, acked);
    }

    // zero start is rejected
    {
        var out: [8]u32 = undefined;
        try std.testing.expectError(error.InvalidGapAckBlock, collectGapAcks(&.{ 0, 0, 0, 2 }, &out));
    }

    // start after end is rejected
    {
        var out: [8]u32 = undefined;
        try std.testing.expectError(error.InvalidGapAckBlock, collectGapAcks(&.{ 0, 5, 0, 3 }, &out));
    }
}

test "ChunkIterator: parse hearbeat" {
    const data = [_]u8{
        0x04, 0x00, 0x00, 0x0F, 0x00,
        0x01, 0x00, 0x0b, 0x00, 0x00,
        0x00, 0x00, 0x00, 0x00, 0x00,
        0x00,
    };

    var it = ChunkIterator.init(data[0..]);
    const chunk = (try it.next()).?;

    try std.testing.expect(chunk == .heartbeat);
    try std.testing.expectEqualSlices(u8, data[8..15], chunk.heartbeat);
}

test "ChunkIterator: skips unrecognized chunk with high bit set" {
    const data = [_]u8{
        0x80, 0x00, 0x00, 0x05, 0xAA, 0x00, 0x00, 0x00,
        0x0B, 0x00, 0x00, 0x04,
    };

    var it = ChunkIterator.init(data[0..]);
    const chunk = (try it.next()).?;

    try std.testing.expect(chunk == .cookie_ack);
    try std.testing.expectEqual(null, try it.next());
}

test "ChunkIterator: stops on unrecognized chunk with high bit clear" {
    const data = [_]u8{ 0x40, 0x00, 0x00, 0x04, 0x0B, 0x00, 0x00, 0x04 };

    var it = ChunkIterator.init(data[0..]);
    try std.testing.expectError(error.UnrecognizedChunkType, it.next());
}

test "ChunkIterator: skips known unhandled chunks" {
    const data = [_]u8{ 0x05, 0x00, 0x00, 0x04, 0x0B, 0x00, 0x00, 0x04 };

    var it = ChunkIterator.init(data[0..]);
    const chunk = (try it.next()).?;

    try std.testing.expect(chunk == .cookie_ack);
}

test "ParameterIterator rejects lengths shorter than the header" {
    for (0..4) |length| {
        const data = [_]u8{ 0x00, 0x01, 0x00, @intCast(length), 0x00, 0x00, 0x00, 0x00 };
        var it = ParameterIterator.init(&data);
        try std.testing.expectError(error.InvalidParameter, it.next());
    }
}
