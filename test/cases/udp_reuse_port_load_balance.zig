const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const posix = std.posix;

fn expectOption(socket: Io.net.Socket, option: u32, expected: bool) !void {
    var value: c_int = undefined;
    var len: posix.socklen_t = @sizeOf(c_int);
    if (posix.errno(posix.system.getsockopt(socket.handle, posix.SOL.SOCKET, @intCast(option), @ptrCast(&value), &len)) != .SUCCESS)
        return error.GetSocketOptionFailed;
    try std.testing.expectEqual(expected, value != 0);
}

fn testLoadBalance(io: Io) !void {
    const address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    const option = switch (builtin.os.tag) {
        .linux => posix.SO.REUSEPORT,
        .freebsd => posix.SO.REUSEPORT_LB,
        else => unreachable,
    };
    // The additive option does not turn on load balancing for an existing
    // FreeBSD caller requesting only ordinary port reuse.
    {
        var plain = try address.bind(io, .{ .mode = .dgram, .reuse_port = true });
        defer plain.close(io);
        try expectOption(plain, posix.SO.REUSEPORT, true);
        if (builtin.os.tag == .freebsd) try expectOption(plain, option, false);
    }
    var first = try address.bind(io, .{ .mode = .dgram, .reuse_port_load_balance = true });
    defer first.close(io);
    try expectOption(first, option, true);
    // Supplying both flags has the same requested load-balancing semantics.
    var second = try first.address.bind(io, .{
        .mode = .dgram,
        .reuse_port = true,
        .reuse_port_load_balance = true,
    });
    defer second.close(io);
    try expectOption(second, option, true);
    try std.testing.expectError(error.AddressInUse, first.address.bind(io, .{ .mode = .dgram }));

    // Keep the source sockets open to guarantee distinct source ports. A
    // single flow consistently selects one receiver; many independent flows
    // must exercise both members of the kernel's load-balancing group.
    var senders: [64]Io.net.Socket = undefined;
    var count: usize = 0;
    defer for (senders[0..count]) |*sender| sender.close(io);
    for (&senders) |*sender| {
        sender.* = try address.bind(io, .{ .mode = .dgram });
        count += 1;
        try sender.send(io, &first.address, "flow");
    }
    try io.sleep(.fromMilliseconds(50), .awake);
    var received: [2]usize = .{ 0, 0 };
    for ([_]*Io.net.Socket{ &first, &second }, &received) |socket, *total| {
        while (true) {
            var messages: [64]Io.net.IncomingMessage = @splat(.init);
            var data: [64 * 32]u8 = undefined;
            const err, const n = socket.receiveManyTimeout(io, &messages, &data, .{}, .{
                .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake },
            });
            total.* += n;
            if (err) |e| switch (e) {
                error.Timeout => break,
                else => return e,
            };
            for (messages[0..n]) |message| try std.testing.expectEqualStrings("flow", message.data);
        }
    }
    try std.testing.expectEqual(senders.len, received[0] + received[1]);
    try std.testing.expect(received[0] > 0 and received[1] > 0);
}

fn testUnsupported(io: Io) !void {
    const address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    for ([_]bool{ false, true }) |reuse_port| {
        try std.testing.expectError(error.OptionUnsupported, address.bind(io, .{
            .mode = .dgram,
            .reuse_port = reuse_port,
            .reuse_port_load_balance = true,
        }));
    }
}

fn testBackend(io: Io) !void {
    switch (builtin.os.tag) {
        .linux, .freebsd => try testLoadBalance(io),
        else => try testUnsupported(io),
    }
}

pub fn main() !void {
    var threaded: Io.Threaded = .init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    try testBackend(threaded.io());
    if (builtin.os.tag == .linux) {
        var uring: Io.Uring = undefined;
        try uring.init(std.heap.page_allocator, .{ .thread_limit = 0 });
        defer uring.deinit();
        try testBackend(uring.io());
    }
    if (builtin.os.tag == .macos) {
        var dispatch: Io.Dispatch = undefined;
        try dispatch.init(std.heap.page_allocator, .{});
        defer dispatch.deinit();
        try testBackend(dispatch.io());
    }
    switch (builtin.os.tag) {
        .freebsd, .openbsd, .macos => {
            var kqueue: Io.Kqueue = undefined;
            try kqueue.init(std.heap.page_allocator, .{ .n_threads = 1 });
            defer kqueue.deinit();
            try testBackend(kqueue.io());
        },
        else => {},
    }
}

// run
// target=aarch64-linux,x86_64-linux,aarch64-freebsd,x86_64-freebsd,aarch64-openbsd,x86_64-openbsd,aarch64-macos,x86_64-macos,x86_64-windows
// link_libc=true
