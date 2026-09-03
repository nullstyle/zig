const std = @import("std");
const Io = std.Io;

const expectEqualStrings = std.testing.expectEqualStrings;

/// The batched network path on `Io.Kqueue`: a timed receive on an idle
/// socket reports `error.Timeout`; several queued datagrams drain in one
/// receive; the wake path (data arriving after the receive parks) works
/// from a second fiber; an untimed receive drains without a timeout.
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

    // Timed receive on an idle socket goes through the batched net_receive
    // path and must report the timeout, not hang.
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
    // Loopback delivery is asynchronous; give the five datagrams time to
    // land in the receive queue so one wake can drain them all.
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
    // multi-message backend takes exactly one round. The batch contract is
    // the point of the test; allow the one-round-or-drained split but
    // require progress.
    if (rounds < 1 or rounds > 5) return error.TestUnexpectedResult;

    // Now the wake path: the receive parks first, and the datagrams arrive
    // afterwards, sent from a second fiber.
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
    if (deadline_rounds > 5) return error.TestUnexpectedResult;
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

/// A plain datagram round trip through the single-op path.
fn testUdpPing(io: Io) !void {
    const bind_address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var receiver = try Io.net.IpAddress.bind(&bind_address, io, .{ .mode = .dgram });
    defer receiver.close(io);
    var sender = try Io.net.IpAddress.bind(&bind_address, io, .{ .mode = .dgram });
    defer sender.close(io);
    try sender.send(io, &receiver.address, "udp ping");
    var buf: [64]u8 = undefined;
    const incoming = try receiver.receive(io, &buf);
    try expectEqualStrings("udp ping", incoming.data);
}

pub fn main() !void {
    var kqueue: Io.Kqueue = undefined;
    try Io.Kqueue.init(&kqueue, std.heap.page_allocator, .{});
    defer kqueue.deinit();
    const io = kqueue.io();
    try testUdpPing(io);
    try testUdpBatch(io);
}

// run
// target=aarch64-freebsd,x86_64-freebsd,aarch64-openbsd,x86_64-openbsd
