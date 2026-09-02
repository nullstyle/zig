const std = @import("std");
const Io = std.Io;

const expectEqualStrings = std.testing.expectEqualStrings;

fn clientTask(io: Io, address: Io.net.IpAddress, iterations: usize) !usize {
    var stream = try Io.net.IpAddress.connect(&address, io, .{ .mode = .stream });
    defer stream.close(io);
    var wbuf: [64]u8 = undefined;
    var rbuf: [64]u8 = undefined;
    var total: usize = 0;
    for (0..iterations) |i| {
        var writer = stream.writer(io, &wbuf);
        try writer.interface.print("msg-{d}", .{i});
        try writer.interface.flush();
        var vec: [1][]u8 = .{&rbuf};
        const n = try stream.read(io, &vec);
        if (n == 0) return error.EndOfStream;
        total += n;
    }
    try stream.shutdown(io, .both);
    return total;
}

fn testTcpEcho(io: Io) !void {
    const listen_address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try Io.net.IpAddress.listen(&listen_address, io, .{});
    defer server.deinit(io);

    const iterations = 50;
    var client_future = Io.async(io, clientTask, .{ io, server.socket.address, iterations });

    // The server runs concurrently with the client fiber, reading through a
    // batched net_read operation.
    var conn = try server.accept(io);
    defer conn.close(io);
    var echoed: usize = 0;
    echo: while (true) {
        var storage: [1]Io.Operation.Storage = undefined;
        var batch: Io.Batch = .init(&storage);
        var rbuf: [64]u8 = undefined;
        var vec: [1][]u8 = .{&rbuf};
        _ = batch.add(.{ .net_read = .{
            .socket_handle = conn.socket.handle,
            .data = &vec,
        } });
        try batch.awaitAsync(io);
        const completion = batch.next().?;
        const n = try completion.result.net_read;
        if (n == 0) break :echo;
        var wbuf: [64]u8 = undefined;
        var writer = conn.writer(io, &wbuf);
        try writer.interface.print("{s}", .{rbuf[0..n]});
        try writer.interface.flush();
        echoed += n;
    }

    const received = try client_future.await(io);
    if (received != echoed) return error.TestExpectedEqual;
}

fn testUnixEcho(io: Io) !void {
    const sock_path = "/tmp/zig-evented-dispatch-net-test.sock";
    _ = std.posix.system.unlink(sock_path);
    const unix_address = try Io.net.UnixAddress.init(sock_path);
    var server = try unix_address.listen(io, .{});
    defer server.deinit(io);
    var client = try unix_address.connect(io);
    defer client.close(io);
    var conn = try server.accept(io);
    defer conn.close(io);

    var wbuf: [64]u8 = undefined;
    var writer = client.writer(io, &wbuf);
    try writer.interface.print("unix hello", .{});
    try writer.interface.flush();

    var rbuf: [64]u8 = undefined;
    var vec: [1][]u8 = .{&rbuf};
    const n = try conn.read(io, &vec);
    try expectEqualStrings("unix hello", rbuf[0..n]);

    var ebuf: [64]u8 = undefined;
    var echo_writer = conn.writer(io, &ebuf);
    try echo_writer.interface.print("{s}", .{rbuf[0..n]});
    try echo_writer.interface.flush();

    var reply: [64]u8 = undefined;
    var reply_vec: [1][]u8 = .{&reply};
    const rn = try client.read(io, &reply_vec);
    try expectEqualStrings("unix hello", reply[0..rn]);

    _ = std.posix.system.unlink(sock_path);
}

fn testConnectRefused(io: Io) !void {
    // Bind then close a listener so that connecting to the ephemeral port is
    // refused; the nonblocking connect must surface ECONNREFUSED through
    // SO_ERROR once the socket becomes writable.
    const listen_address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var server = try Io.net.IpAddress.listen(&listen_address, io, .{});
    const address = server.socket.address;
    server.deinit(io);
    var attempts: usize = 0;
    while (attempts < 10) : (attempts += 1) {
        if (Io.net.IpAddress.connect(&address, io, .{ .mode = .stream })) |stream| {
            stream.close(io);
            continue;
        } else |err| switch (err) {
            error.ConnectionRefused => return,
            else => return err,
        }
    }
    return error.TestUnexpectedResult;
}

fn testUdpDatagram(io: Io) !void {
    const bind_address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var receiver = try Io.net.IpAddress.bind(&bind_address, io, .{ .mode = .dgram });
    defer receiver.close(io);
    const sender_address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var sender = try Io.net.IpAddress.bind(&sender_address, io, .{ .mode = .dgram });
    defer sender.close(io);

    try sender.send(io, &receiver.address, "udp ping");
    var buf: [64]u8 = undefined;
    const incoming = try receiver.receive(io, &buf);
    try expectEqualStrings("udp ping", incoming.data);
    if (incoming.from.ip4.port == 0) return error.TestUnexpectedResult;
}

fn testInterfaceResolve(io: Io) !void {
    var name: Io.net.Interface.Name = undefined;
    @memcpy(name.bytes[0..3], "lo0");
    name.bytes[3] = 0;
    const iface = try name.resolve(io);
    if (iface.index == 0) return error.TestUnexpectedResult;
}

fn testHostLookup(io: Io) !void {
    try Io.net.HostName.validate("localhost");
    const host_name: Io.net.HostName = .{ .bytes = "localhost" };
    var results: [16]Io.net.HostName.LookupResult = undefined;
    var resolved = Io.Queue(Io.net.HostName.LookupResult).init(&results);
    try Io.net.HostName.lookup(host_name, io, &resolved, .{ .port = 80 });
    var count: usize = 0;
    while (true) {
        _ = resolved.getOneUncancelable(io) catch |err| switch (err) {
            error.Closed => break,
        };
        count += 1;
    }
    if (count == 0) return error.TestUnexpectedResult;
}

pub fn main() !void {
    var evented: Io.Evented = undefined;
    try Io.Evented.init(&evented, std.heap.page_allocator, .{});
    defer evented.deinit();
    const io = evented.io();
    try testTcpEcho(io);
    try testUnixEcho(io);
    try testConnectRefused(io);
    try testUdpDatagram(io);
    try testInterfaceResolve(io);
    try testHostLookup(io);
}

// run
// target=aarch64-macos,x86_64-macos
// link_libc=true
