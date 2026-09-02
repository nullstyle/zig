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
    // Per-process path: the case runs for two targets that may run at once.
    var path_buf: [96]u8 = undefined;
    const path_len = (try std.fmt.bufPrint(path_buf[0..95], "/tmp/zig-evented-dispatch-net-{d}.sock", .{std.c.getpid()})).len;
    path_buf[path_len] = 0;
    const sock_path: [:0]const u8 = path_buf[0..path_len :0];
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

fn testUdpBatch(io: Io) !void {
    const bind_address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var receiver = try Io.net.IpAddress.bind(&bind_address, io, .{ .mode = .dgram });
    defer receiver.close(io);
    const sender_address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var sender = try Io.net.IpAddress.bind(&sender_address, io, .{ .mode = .dgram });
    defer sender.close(io);

    var messages: [16]Io.net.IncomingMessage = undefined;
    var data: [16 * 64]u8 = undefined;
    const short: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } };

    // Timed receive on an idle socket goes through the batched
    // net_receive path and must report the timeout, not hang.
    for (&messages) |*m| m.* = .init;
    const idle_err, const idle_count = receiver.receiveManyTimeout(io, &messages, &data, .{}, short);
    if (idle_err) |e| {
        if (e != error.Timeout) return error.TestUnexpectedResult;
    } else return error.TestUnexpectedResult;
    if (idle_count != 0) return error.TestUnexpectedResult;

    // Several queued datagrams drain in ONE timed receive.
    var payloads: [5][8]u8 = undefined;
    var outgoing: [5]Io.net.OutgoingMessage = undefined;
    for (&payloads, &outgoing, 0..) |*p, *o, i| {
        p.* = .{ 'b', 'a', 't', 'c', 'h', '-', @intCast('0' + i), 0 };
        o.* = .{ .address = &receiver.address, .data_ptr = p, .data_len = p.len };
    }
    const send_err, const sent = sender.sendManyTimeout(io, &outgoing, .{}, short);
    if (send_err != null) return error.TestUnexpectedResult;
    if (sent != outgoing.len) return error.TestUnexpectedResult;
    // Loopback delivery is asynchronous on Darwin; give the five datagrams
    // time to land in the receive queue so one wake can drain them all.
    try Io.sleep(io, .fromMilliseconds(50), .awake);

    var received: usize = 0;
    var rounds: usize = 0;
    while (received < outgoing.len) : (rounds += 1) {
        if (rounds == 10) return error.TestUnexpectedResult;
        for (&messages) |*m| m.* = .init;
        const long: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(2000), .clock = .awake } };
        const err, const count = receiver.receiveManyTimeout(io, &messages, &data, .{}, long);
        if (err != null) return error.TestUnexpectedResult;
        for (messages[0..count], 0..) |m, j| {
            const expected_index = received + j;
            if (m.data.len != 8 or m.data[6] != '0' + expected_index) return error.TestUnexpectedResult;
        }
        received += count;
    }
    // Loopback delivers all five before the first wake completes, so a
    // multi-message backend takes exactly one round; a one-per-wake
    // backend takes five. The batch contract is the point of the test.
    if (rounds != 1) return error.TestUnexpectedResult;

    // Now the other order: the receive parks on its readiness source first,
    // and the datagrams arrive afterwards. This is the wake path
    // (`batchSourceEvent`), which the section above never reaches because
    // its data is already queued when the operation is submitted.
    {
        var armed_messages: [16]Io.net.IncomingMessage = undefined;
        var armed_data: [16 * 64]u8 = undefined;
        var future = Io.async(io, receiveArmed, .{ io, &receiver, &armed_messages, &armed_data });
        try Io.sleep(io, .fromMilliseconds(20), .awake);
        const send_err3, const sent3 = sender.sendManyTimeout(io, &outgoing, .{}, short);
        if (send_err3 != null or sent3 != outgoing.len) return error.TestUnexpectedResult;
        const wake_err, const wake_count = future.await(io);
        if (wake_err != null) return error.TestUnexpectedResult;
        if (wake_count == 0 or wake_count > outgoing.len) return error.TestUnexpectedResult;
        var got: usize = wake_count;
        var drain_rounds: usize = 0;
        while (got < outgoing.len) : (drain_rounds += 1) {
            if (drain_rounds == 10) return error.TestUnexpectedResult;
            for (&messages) |*m| m.* = .init;
            const err, const count = receiver.receiveManyTimeout(io, &messages, &data, .{}, short);
            if (err != null) return error.TestUnexpectedResult;
            got += count;
        }
        if (got != outgoing.len) return error.TestUnexpectedResult;
    }

    // Untimed receiveMany (the plain `operate` path) also drains many.
    const send_err2, const sent2 = sender.sendManyTimeout(io, &outgoing, .{}, short);
    if (send_err2 != null or sent2 != outgoing.len) return error.TestUnexpectedResult;
    try Io.sleep(io, .fromMilliseconds(50), .awake);
    for (&messages) |*m| m.* = .init;
    var deadline_rounds: usize = 0;
    received = 0;
    while (received < outgoing.len) : (deadline_rounds += 1) {
        if (deadline_rounds == 10) return error.TestUnexpectedResult;
        const err, const count = receiver.receiveManyTimeout(io, &messages, &data, .{}, .none);
        if (err != null) return error.TestUnexpectedResult;
        received += count;
    }
    if (deadline_rounds != 1) return error.TestUnexpectedResult;
}

fn receiveArmed(
    io: Io,
    sock: *const Io.net.Socket,
    messages: []Io.net.IncomingMessage,
    data: []u8,
) struct { ?Io.net.Socket.ReceiveTimeoutError, usize } {
    for (messages) |*m| m.* = .init;
    return sock.receiveManyTimeout(io, messages, data, .{}, .{
        .duration = .{ .raw = .fromMilliseconds(2000), .clock = .awake },
    });
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
    try testUdpBatch(io);
    try testInterfaceResolve(io);
    try testHostLookup(io);
}

// run
// target=aarch64-macos,x86_64-macos
// link_libc=true
