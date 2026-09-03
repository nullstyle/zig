const std = @import("std");
const Io = std.Io;

/// Windows has no SO_REUSEPORT; `reuse_port` is refused before a socket is
/// created rather than silently ignored.
fn testUdpReusePortRefused(io: Io) !void {
    const bind_address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    if (Io.net.IpAddress.bind(&bind_address, io, .{ .mode = .dgram, .reuse_port = true })) |bound| {
        var socket = bound;
        socket.close(io);
        return error.TestUnexpectedResult;
    } else |err| switch (err) {
        error.OptionUnsupported => {},
        else => |e| return e,
    }
}

pub fn main() !void {
    var threaded: Io.Threaded = .init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    try testUdpReusePortRefused(threaded.io());
}

// run
// target=x86_64-windows,aarch64-windows
