# SCTP

Zig implementation of SCTP (Stream Control Transmission Protocol) [RFC 9260](https://www.rfc-editor.org/rfc/rfc9260.html).

This library implement a subset of the SCTP protocol, focusing on the parts needed by WebRTC data channels.

## Architecture

This is a sans-IO implementation of SCTP, meaning that it does not handle any I/O operations. Instead, it provides a set of functions to encode and decode SCTP packets, as well as to manage the state of an SCTP association.

## Installation

Requires Zig `>= 0.16.0`.

```sh
zig fetch --save git+https://github.com/zigouat/sctp
```

In `build.zig`:

```zig
const sctp = b.dependency("sctp", .{ .target = target, .optimize = optimize });
exe.root_module.addImport("sctp", sctp.module("sctp"));
```

## Usage

`sctp.Association` is a state machine. The caller owns the transport and the clock. `now` is a monotonic timestamp in milliseconds (`i64`).

### Input

- `handleRead(packet, now)`: feeds one received SCTP packet (common header + chunks).
- `handleWrite(data, options)`: queues a user message. **`data` is not copied**: keep it alive until it is returned by a `.release` event.
- `handleTimeout(now)`: handle expired timers.

### Output

Drain every output after each input call.

- `pollEvent() ?Event`: returns the next application event, or `null` when none is left.
- `pollTransmits(buffer, now) ?[]const u8`: returns the next packet to send as a slice of `buffer` or `null` when nothing is left.
- `pollTimeout() ?i64`: returns the earliest absolute deadline, or `null` when no timer is armed.

`Association` is not thread-safe, so serialize all calls.

