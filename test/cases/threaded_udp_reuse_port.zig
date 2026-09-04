const std = @import("std");
const Io = std.Io;

const expectEqualStrings = std.testing.expectEqualStrings;

/// Two sockets on one UDP port through `reuse_port`; a datagram sent to the
/// port reaches one of them. Linux picks by flow hash, BSD hands everything
/// to one socket of the group. The receive order below is fixed, so on Linux
/// about half the runs wait out the 500 ms timeout on the socket the hash
/// did not pick before reading from the other.
fn testUdpReusePort(io: Io) !void {
    const bind_address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var first = try Io.net.IpAddress.bind(&bind_address, io, .{ .mode = .dgram, .reuse_port = true });
    defer first.close(io);

    // The same port without the option is still refused.
    if (Io.net.IpAddress.bind(&first.address, io, .{ .mode = .dgram })) |plain| {
        var socket = plain;
        socket.close(io);
        return error.TestUnexpectedResult;
    } else |err| switch (err) {
        error.AddressInUse => {},
        else => |e| return e,
    }

    var second = try Io.net.IpAddress.bind(&first.address, io, .{ .mode = .dgram, .reuse_port = true });
    defer second.close(io);
    if (second.address.ip4.port != first.address.ip4.port) return error.TestUnexpectedResult;

    var sender = try Io.net.IpAddress.bind(&bind_address, io, .{ .mode = .dgram });
    defer sender.close(io);
    try sender.send(io, &first.address, "shared port");

    var buf: [64]u8 = undefined;
    for ([_]*Io.net.Socket{ &second, &first }) |socket| {
        var messages: [1]Io.net.IncomingMessage = .{.init};
        const opt_err, const n = socket.receiveManyTimeout(io, &messages, &buf, .{}, .{
            .duration = .{ .raw = .fromMilliseconds(500), .clock = .awake },
        });
        if (n == 1) {
            try expectEqualStrings("shared port", messages[0].data);
            return;
        }
        if (opt_err) |err| switch (err) {
            error.Timeout => {},
            else => |e| return e,
        };
    }
    return error.TestUnexpectedResult;
}

pub fn main() !void {
    var threaded: Io.Threaded = .init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    try testUdpReusePort(threaded.io());
}

// run
// target=aarch64-linux,x86_64-linux,aarch64-macos,x86_64-macos
