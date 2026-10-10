const std = @import("std");
const Io = std.Io;
const expect = std.testing.expect;

const WaitMode = enum { async, concurrent, timed };
const long_timeout: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(30), .clock = .awake } };

fn bind(io: Io) !Io.net.Socket {
    const address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    return address.bind(io, .{ .mode = .dgram });
}

fn receiveBatch(io: Io, sockets: []const Io.net.Socket, mode: WaitMode, ready: *Io.Event) anyerror!void {
    var storage: [2]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(storage[0..sockets.len]);
    defer batch.cancel(io);
    var messages: [2][1]Io.net.IncomingMessage = .{ .{.init}, .{.init} };
    var data: [2][32]u8 = undefined;
    for (sockets, 0..) |socket, i| {
        _ = batch.add(.{ .net_receive = .{
            .socket_handle = socket.handle,
            .message_buffer = &messages[i],
            .data_buffer = &data[i],
            .flags = .{},
        } });
    }
    ready.set(io);
    switch (mode) {
        .async => try batch.awaitAsync(io),
        .concurrent => try batch.awaitConcurrent(io, .none),
        .timed => try batch.awaitConcurrent(io, long_timeout),
    }
    const completion = batch.next() orelse return error.MissingCompletion;
    const err, const count = completion.result.net_receive;
    if (err) |e| return e;
    try expect(count == 1);
}

fn testCanceledBatch(io: Io, count: usize, mode: WaitMode) !void {
    var sockets: [2]Io.net.Socket = undefined;
    for (sockets[0..count]) |*socket| socket.* = try bind(io);
    defer for (sockets[0..count]) |socket| socket.close(io);

    // One socket uses the cached entry, while two sockets force the
    // general batch queue. Exercise both cancellation before registration
    // and cancellation after an idle receive has started waiting.
    for (0..2) |phase| {
        var ready: Io.Event = .unset;
        var future = try Io.concurrent(io, receiveBatch, .{ io, sockets[0..count], mode, &ready });
        if (phase == 1) {
            try ready.wait(io);
            try Io.sleep(io, .fromMilliseconds(5), .awake);
        }
        try std.testing.expectError(error.Canceled, future.cancel(io));
    }

    // Cancellation must release every pending storage reference and leave
    // the socket's cached readiness source usable by a subsequent batch.
    var sender = try bind(io);
    defer sender.close(io);
    for (sockets[0..count]) |socket| {
        try sender.send(io, &socket.address, "reused");
        var messages: [1]Io.net.IncomingMessage = .{.init};
        var data: [32]u8 = undefined;
        const err, const received = socket.receiveManyTimeout(io, &messages, &data, .{}, long_timeout);
        if (err) |e| return e;
        try expect(received == 1);
        try std.testing.expectEqualStrings("reused", messages[0].data);
    }
}

fn protectedReceive(
    io: Io,
    sockets: []const Io.net.Socket,
    ready: *Io.Event,
    received: *std.atomic.Value(bool),
) anyerror!void {
    const previous = Io.swapCancelProtection(io, .blocked);
    defer _ = Io.swapCancelProtection(io, previous);
    try receiveBatch(io, sockets, .timed, ready);
    received.store(true, .release);
    _ = Io.swapCancelProtection(io, .unblocked);
    try Io.checkCancel(io);
    return error.MissingCancellation;
}

fn cancelFuture(io: Io, future: *Io.Future(anyerror!void), started: *Io.Event) anyerror!void {
    started.set(io);
    return future.cancel(io);
}

fn testProtection(io: Io, count: usize) !void {
    var sockets: [2]Io.net.Socket = undefined;
    for (sockets[0..count]) |*socket| socket.* = try bind(io);
    defer for (sockets[0..count]) |socket| socket.close(io);
    var sender = try bind(io);
    defer sender.close(io);
    var ready: Io.Event = .unset;
    var received: std.atomic.Value(bool) = .init(false);
    var future = try Io.concurrent(io, protectedReceive, .{ io, sockets[0..count], &ready, &received });
    try ready.wait(io);
    var started: Io.Event = .unset;
    var canceler = try Io.concurrent(io, cancelFuture, .{ io, &future, &started });
    try started.wait(io);
    try Io.sleep(io, .fromMilliseconds(5), .awake);
    try expect(!received.load(.acquire));
    try sender.send(io, &sockets[0].address, "protected");
    try std.testing.expectError(error.Canceled, canceler.await(io));
    try expect(received.load(.acquire));
}

fn groupReceive(io: Io, socket: *const Io.net.Socket, ready: *Io.Event, canceled: *std.atomic.Value(usize)) Io.Cancelable!void {
    receiveBatch(io, @as([*]const Io.net.Socket, @ptrCast(socket))[0..1], .timed, ready) catch |err| {
        if (err == error.Canceled) {
            _ = canceled.fetchAdd(1, .monotonic);
            return error.Canceled;
        }
        std.debug.panic("unexpected group receive result: {s}", .{@errorName(err)});
    };
}

fn testGroup(io: Io) !void {
    var sockets = [_]Io.net.Socket{ try bind(io), try bind(io) };
    defer for (sockets) |socket| socket.close(io);
    var ready: [2]Io.Event = .{ .unset, .unset };
    var canceled: std.atomic.Value(usize) = .init(0);
    var group: Io.Group = .init;
    defer group.cancel(io);
    for (&sockets, &ready) |*socket, *event| {
        try group.concurrent(io, groupReceive, .{ io, socket, event, &canceled });
        try event.wait(io);
    }
    try Io.sleep(io, .fromMilliseconds(5), .awake);
    group.cancel(io);
    try expect(canceled.load(.acquire) == sockets.len);
}

fn testCompletionRace(io: Io, count: usize) !void {
    var sender = try bind(io);
    defer sender.close(io);
    for (0..100) |_| {
        var sockets: [2]Io.net.Socket = undefined;
        for (sockets[0..count]) |*socket| socket.* = try bind(io);
        defer for (sockets[0..count]) |socket| socket.close(io);
        var ready: Io.Event = .unset;
        var future = try Io.concurrent(io, receiveBatch, .{ io, sockets[0..count], .timed, &ready });
        try ready.wait(io);
        try sender.send(io, &sockets[0].address, "race");
        future.cancel(io) catch |err| switch (err) {
            error.Canceled => {},
            else => return err,
        };
    }
}

fn testBatchTimeouts(io: Io, count: usize, clock: Io.Clock) !void {
    var sockets: [2]Io.net.Socket = undefined;
    for (sockets[0..count]) |*socket| socket.* = try bind(io);
    defer for (sockets[0..count]) |socket| socket.close(io);
    var storage: [2]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(storage[0..count]);
    defer batch.cancel(io);
    var messages: [2][1]Io.net.IncomingMessage = .{ .{.init}, .{.init} };
    var data: [2][32]u8 = undefined;
    for (sockets[0..count], 0..) |socket, i| {
        _ = batch.add(.{ .net_receive = .{
            .socket_handle = socket.handle,
            .message_buffer = &messages[i],
            .data_buffer = &data[i],
            .flags = .{},
        } });
    }

    // Reuse the pending operations and their timers after every expiry.
    // Wall-clock dispatch encodings decrease as time passes, so comparing
    // their raw integers incorrectly waits forever on an idle real clock.
    for (0..4) |_| {
        const duration: Io.Clock.Duration = .{ .raw = .fromMilliseconds(2), .clock = clock };
        for (0..4) |kind| {
            const timeout: Io.Timeout = switch (kind) {
                0 => .{ .duration = duration },
                1 => .{ .deadline = .fromNow(io, duration) },
                2 => .{ .deadline = .{ .raw = .zero, .clock = clock } },
                3 => .{ .duration = .{ .raw = .fromNanoseconds(-1), .clock = clock } },
                else => unreachable,
            };
            const deadline = timeout.toTimestamp(io).?;
            try std.testing.expectError(error.Timeout, batch.awaitConcurrent(io, timeout));
            try expect(deadline.raw.compare(.lte, clock.now(io)));
            try expect(batch.next() == null);
        }
    }
}

pub fn main() !void {
    var evented: Io.Evented = undefined;
    try Io.Evented.init(&evented, std.heap.page_allocator, .{});
    defer evented.deinit();
    const io = evented.io();
    for ([_]usize{ 1, 2 }) |count| {
        for ([_]WaitMode{ .async, .concurrent, .timed }) |mode|
            try testCanceledBatch(io, count, mode);
        try testProtection(io, count);
        try testCompletionRace(io, count);
        for ([_]Io.Clock{ .awake, .real }) |clock|
            try testBatchTimeouts(io, count, clock);
    }
    try testGroup(io);
}

// run
// backend=llvm
// target=aarch64-macos,x86_64-macos
// link_libc=true
