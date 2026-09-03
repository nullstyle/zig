const std = @import("std");
const Io = std.Io;

const expectEqualStrings = std.testing.expectEqualStrings;

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
    // Drain-at-submit: the whole queue comes back from one call.
    if (rounds != 1) return error.TestUnexpectedResult;

    // The other order: the receive waits in the ring first, and the
    // datagrams arrive afterwards. This is the wake path
    // (`batchDrainReady`), which the section above never reaches.
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

const stress_receivers = 12;
const stress_rounds = 150;

const StressStats = struct {
    sent: std.atomic.Value(usize) = .init(0),
    received: std.atomic.Value(usize) = .init(0),
    done: std.atomic.Value(bool) = .init(false),
};

/// A QUIC-loop-shaped receiver: 1 ms timed receives, forever until the
/// sender is done and two consecutive timeouts show the queue is drained.
fn stressReceiver(io: Io, sock: *const Io.net.Socket, stats: *StressStats) anyerror!void {
    var messages: [8]Io.net.IncomingMessage = undefined;
    var data: [8 * 64]u8 = undefined;
    var idle_after_done: usize = 0;
    var rounds: usize = 0;
    while (rounds < 100_000) : (rounds += 1) {
        for (&messages) |*m| m.* = .init;
        const err, const count = sock.receiveManyTimeout(io, &messages, &data, .{}, .{
            .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake },
        });
        if (err) |e| {
            if (e != error.Timeout) return e;
        }
        _ = stats.received.fetchAdd(count, .monotonic);
        if (stats.done.load(.acquire)) {
            if (count == 0) {
                idle_after_done += 1;
                if (idle_after_done == 3) return;
            } else idle_after_done = 0;
        }
    }
    return error.TestUnexpectedResult;
}

fn stressSender(io: Io, targets: []const Io.net.IpAddress, stats: *StressStats) anyerror!void {
    const bind_address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var sock = try Io.net.IpAddress.bind(&bind_address, io, .{ .mode = .dgram });
    defer sock.close(io);
    var payload: [16]u8 = @splat('s');
    for (0..stress_rounds) |round| {
        std.mem.writeInt(u64, payload[0..8], round, .little);
        for (targets) |*target| {
            try sock.send(io, target, &payload);
            _ = stats.sent.fetchAdd(1, .monotonic);
        }
        // Slightly off the receivers' 1 ms cadence so sends land at every
        // phase of their timeouts.
        try Io.sleep(io, .fromNanoseconds(700_000), .awake);
    }
    stats.done.store(true, .release);
}

/// Many fibers doing short timed receives while another fiber sends to all of
/// them: exercises timer-versus-completion races, cancels reaching a request
/// armed on another thread's ring, and datagrams consumed during a cancel.
/// Every datagram sent must be received.
fn testUdpStress(io: Io) !void {
    var socks: [stress_receivers]Io.net.Socket = undefined;
    var targets: [stress_receivers]Io.net.IpAddress = undefined;
    var bound: usize = 0;
    defer for (socks[0..bound]) |*sock| sock.close(io);
    for (&socks) |*sock| {
        const bind_address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
        sock.* = try Io.net.IpAddress.bind(&bind_address, io, .{ .mode = .dgram });
        targets[bound] = sock.address;
        bound += 1;
    }
    var stats: StressStats = .{};
    var receivers: [stress_receivers]Io.Future(anyerror!void) = undefined;
    for (&receivers, &socks) |*future, *sock| future.* = Io.async(io, stressReceiver, .{ io, sock, &stats });
    var sender = Io.async(io, stressSender, .{ io, &targets, &stats });
    var failed = false;
    sender.await(io) catch {
        failed = true;
    };
    for (&receivers) |*future| future.await(io) catch {
        failed = true;
    };
    if (failed) return error.TestUnexpectedResult;
    const sent = stats.sent.load(.acquire);
    const received = stats.received.load(.acquire);
    if (sent != stress_receivers * stress_rounds) return error.TestUnexpectedResult;
    if (received != sent) return error.TestUnexpectedResult;
}

pub fn main() !void {
    var evented: Io.Evented = undefined;
    try Io.Evented.init(&evented, std.heap.page_allocator, .{});
    defer evented.deinit();
    const io = evented.io();
    try testUdpDatagram(io);
    try testUdpBatch(io);
    try testUdpStress(io);
}

// run
// target=aarch64-linux,x86_64-linux
