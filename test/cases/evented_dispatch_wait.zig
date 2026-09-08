const std = @import("std");
const Io = std.Io;
const expect = std.testing.expect;
const expectError = std.testing.expectError;

fn acceptTask(io: Io, server: *Io.net.Server) !void {
    const stream = try server.accept(io);
    defer stream.close(io);
}

fn readTask(io: Io, stream: Io.net.Stream) !u8 {
    var buffer: [1]u8 = undefined;
    var data: [1][]u8 = .{&buffer};
    const count = try stream.read(io, &data);
    if (count != 1) return error.TestUnexpectedResult;
    return buffer[0];
}

fn receiveTask(io: Io, socket: Io.net.Socket) !void {
    var message: [1]Io.net.IncomingMessage = .{.init};
    var buffer: [32]u8 = undefined;
    const result = try io.operate(.{ .net_receive = .{
        .socket_handle = socket.handle,
        .message_buffer = &message,
        .data_buffer = &buffer,
        .flags = .{},
    } });
    if (result.net_receive[0]) |err| return err;
}

fn writeTask(io: Io, stream: Io.net.Stream) !void {
    const result = try io.operate(.{ .net_write = .{
        .socket_handle = stream.socket.handle,
        .header = "",
        .data = &.{"x"},
        .splat = 1,
    } });
    _ = try result.net_write;
}

fn sendByte(io: Io, stream: Io.net.Stream) !void {
    const result = try io.operate(.{ .net_write = .{
        .socket_handle = stream.socket.handle,
        .header = "",
        .data = &.{"x"},
        .splat = 1,
    } });
    try expect(try result.net_write == 1);
}

fn sendLater(io: Io, stream: Io.net.Stream) !void {
    try Io.sleep(io, .fromMilliseconds(60), .awake);
    try sendByte(io, stream);
}

fn protectedRead(io: Io, stream: Io.net.Stream, entered: *std.atomic.Value(bool)) !void {
    const previous = io.swapCancelProtection(.blocked);
    const byte = result: {
        defer _ = io.swapCancelProtection(previous);
        entered.store(true, .release);
        break :result try readTask(io, stream);
    };
    try expect(byte == 'x');
    try io.checkCancel();
    return error.TestUnexpectedResult;
}

fn testAcceptCancellation(io: Io) !void {
    const address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try address.listen(io, .{});
    defer server.deinit(io);
    var parked = Io.async(io, acceptTask, .{ io, &server });
    try Io.sleep(io, .fromMilliseconds(20), .awake);
    try expectError(error.Canceled, parked.cancel(io));
    // Race cancellation with entry into the wait (including a request that
    // arrives before the wait's dispatch sources have been activated).
    for (0..32) |_| {
        var immediate = Io.async(io, acceptTask, .{ io, &server });
        try expectError(error.Canceled, immediate.cancel(io));
    }
}

fn testConnectedWaits(io: Io) !void {
    const address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try address.listen(io, .{});
    defer server.deinit(io);
    const client = try server.socket.address.connect(io, .{
        .mode = .stream,
        .timeout = .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } },
    });
    defer client.close(io);
    const peer = try server.accept(io);
    defer peer.close(io);

    var read = Io.async(io, readTask, .{ io, peer });
    try Io.sleep(io, .fromMilliseconds(20), .awake);
    try expectError(error.Canceled, read.cancel(io));

    var entered: std.atomic.Value(bool) = .init(false);
    var protected = Io.async(io, protectedRead, .{ io, peer, &entered });
    while (!entered.load(.acquire)) try Io.sleep(io, .fromMilliseconds(1), .awake);
    var sender = Io.async(io, sendLater, .{ io, client });
    try expectError(error.Canceled, protected.cancel(io));
    try sender.await(io);

    // Completion and cancellation may each win, but must never resume the
    // same fiber twice or leave a callback pointing into a freed stack.
    for (0..32) |_| {
        var race = Io.async(io, readTask, .{ io, peer });
        try sendByte(io, client);
        const byte = race.cancel(io) catch |err| switch (err) {
            error.Canceled => try readTask(io, peer),
            else => return err,
        };
        try expect(byte == 'x');
    }
}

fn testReceiveCancellation(io: Io) !void {
    const address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    const socket = try address.bind(io, .{ .mode = .dgram });
    defer socket.close(io);
    var receive = Io.async(io, receiveTask, .{ io, socket });
    try Io.sleep(io, .fromMilliseconds(20), .awake);
    try expectError(error.Canceled, receive.cancel(io));
}

fn testWriteCancellation(io: Io) !void {
    // A nonblocking Unix socket pair can be filled without relying on TCP
    // window sizes or a remote peer. The next write then parks on WRITE.
    var sockets: [2]std.c.fd_t = undefined;
    if (std.c.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &sockets) != 0)
        return error.TestUnexpectedResult;
    defer {
        for (sockets) |fd| _ = std.c.close(fd);
    }
    for (sockets) |fd| {
        const flags = std.c.fcntl(fd, std.posix.F.GETFL, @as(usize, 0));
        if (flags < 0 or std.c.fcntl(fd, std.posix.F.SETFL, @as(usize, @intCast(flags)) |
            @as(usize, 1 << @bitOffsetOf(std.posix.O, "NONBLOCK"))) < 0)
            return error.TestUnexpectedResult;
    }
    const buffer: [4096]u8 = @splat(0);
    while (true) {
        const n = std.c.write(sockets[0], &buffer, buffer.len);
        switch (std.c.errno(n)) {
            .SUCCESS => {},
            .INTR => continue,
            .AGAIN => break,
            else => return error.TestUnexpectedResult,
        }
    }
    const stream: Io.net.Stream = .{ .socket = .{
        .handle = sockets[0],
        .address = .{ .ip4 = .loopback(0) },
    } };
    var write = Io.async(io, writeTask, .{ io, stream });
    try Io.sleep(io, .fromMilliseconds(20), .awake);
    try expectError(error.Canceled, write.cancel(io));
}

fn testConnectRefusedWithTimeout(io: Io) !void {
    const address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try address.listen(io, .{});
    const closed = server.socket.address;
    server.deinit(io);
    try expectError(error.ConnectionRefused, closed.connect(io, .{
        .mode = .stream,
        .timeout = .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } },
    }));
}

pub fn main() !void {
    var evented: Io.Evented = undefined;
    try Io.Evented.init(&evented, std.heap.page_allocator, .{ .leeway = .zero });
    defer evented.deinit();
    const io = evented.io();
    try testAcceptCancellation(io);
    try testConnectedWaits(io);
    try testReceiveCancellation(io);
    try testWriteCancellation(io);
    try testConnectRefusedWithTimeout(io);
}

// run
// backend=llvm
// target=aarch64-macos,x86_64-macos
// link_libc=true
