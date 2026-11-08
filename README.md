# SCTP

Zig implementation of SCTP (Stream Control Transmission Protocol) [RFC 9260](https://www.rfc-editor.org/rfc/rfc9260.html).

This library implement a subset of the SCTP protocol, focusing on the parts needed by WebRTC data channels.

## Standards

| RFC | Title | Status |
|---|---|---|
| [RFC 9260](https://www.rfc-editor.org/rfc/rfc9260.html) | Stream Control Transmission Protocol | Single-homed associations: handshake, data transfer, fragmentation, congestion control, graceful shutdown and abort. Heartbeats are answered but not sent; ECN is ignored. |
| [RFC 3758](https://www.rfc-editor.org/rfc/rfc3758.html) | Partial Reliability Extension (PR-SCTP) | FORWARD TSN, with the max-retransmit and deadline policies. Enabled with `supported_extensions.partial_reliability`. |
| [RFC 6525](https://www.rfc-editor.org/rfc/rfc6525.html) | Stream Reconfiguration | Outgoing and incoming SSN reset requests. SSN/TSN reset and adding streams are not supported. Enabled with `supported_extensions.stream_reconfiguration`. |
| [RFC 5061](https://www.rfc-editor.org/rfc/rfc5061.html) | Dynamic Address Reconfiguration | Only the Supported Extensions parameter, used to negotiate the extensions above. |

Not implemented: multi-homing, I-DATA ([RFC 8260](https://www.rfc-editor.org/rfc/rfc8260.html)), AUTH ([RFC 4895](https://www.rfc-editor.org/rfc/rfc4895.html)).

## Architecture

This is a sans-IO implementation of SCTP, meaning that it does not handle any I/O operations. Instead, it provides a set of functions to encode and decode SCTP packets, as well as to manage the state of an SCTP association.

## Installation

Requires Zig `0.16.0`.

```sh
zig fetch --save git+https://github.com/zigouat/sctp.git#v0.1.0
```

In `build.zig`:

```zig
const sctp = b.dependency("sctp", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("sctp", sctp.module("sctp"));
```

## Usage

`sctp.Association` is a state machine. The caller owns the transport and the clock. `now` is a monotonic timestamp in milliseconds (`i64`).

Two associations in the same process, packets are handed from one to the other instead of going through a socket:

```zig
const std = @import("std");
const sctp = @import("sctp");

pub fn main(init: std.process.Init) !void {
    var prng = std.Random.DefaultPrng.init(0);
    var client = sctp.Association.init(init.gpa, .{ .source_port = 5000, .dest_port = 5001, .random = prng.random() });
    defer client.deinit();
    var server = sctp.Association.init(init.gpa, .{ .source_port = 5001, .dest_port = 5000, .random = prng.random() });
    defer server.deinit();

    try client.connect();

    const now: i64 = 0;
    var buffer: [sctp.message.MTU]u8 = undefined;
    var open = true;
    while (open) {
        while (client.pollEvent()) |event| switch (event) {
            .comm_up => {
                try client.handleWrite("hello", .{ .stream_id = 0, .ppid = 51 });
                try client.shutdown();
            },
            .comm_down => open = false,
            else => {},
        };
        while (try client.pollTransmit(&buffer, now)) |packet| {
            var messages = try server.handleRead(packet, now) orelse continue;
            while (messages.next()) |msg| std.debug.print("received: {s}\n", .{msg.data});
        }
        while (server.pollEvent()) |_| {}
        while (try server.pollTransmit(&buffer, now)) |packet| _ = try client.handleRead(packet, now);
    }
}
```

Over a real transport, the loop also waits for the next packet or `pollTimeout()` and calls `handleTimeout(now)` once the deadline passes.

### Input

- `handleRead(packet, now) !?MessageIterator`: feeds one received SCTP packet (common header + chunks) and returns an iterator over the messages ready to be read, or `null` when there are none. This is the only way to receive messages. A message's `data` is valid until the next call to `next` or `handleRead`. Messages left unread keep the receive window closed, which is how back-pressure is applied.
- `handleWrite(data, options)`: queues a user message. **`data` is copied**.
- `handleTimeout(now)`: handle expired timers.

### Output

Drain every output after each input call.

- `pollEvent() ?Event`: returns the next application event, or `null` when none is left.
- `pollTransmit(buffer, now) TransmitError!?[]const u8`: returns the next packet to send as a slice of `buffer` or `null` when nothing is left.
- `pollTimeout() ?i64`: returns the earliest absolute deadline, or `null` when no timer is armed.

`Association` is not thread-safe, so serialize all calls.

