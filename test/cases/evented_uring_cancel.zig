const std = @import("std");
const Io = std.Io;

fn idleReceive(io: Io, socket: *const Io.net.Socket) !void {
    var messages: [1]Io.net.IncomingMessage = .{.init};
    var data: [32]u8 = undefined;
    const err, const count = socket.receiveManyTimeout(io, &messages, &data, .{}, .{
        .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake },
    });
    if (count != 0) return error.TestUnexpectedResult;
    if (err) |e| return e;
    return error.TestUnexpectedResult;
}

/// A future parked on a batch-tagged receive must acknowledge cancellation
/// promptly, rather than waiting for the linked timeout to expire.
fn testCancelLinkedReceive(io: Io) !void {
    const address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var socket = try address.bind(io, .{ .mode = .dgram });
    defer socket.close(io);
    var future = try Io.concurrent(io, idleReceive, .{ io, &socket });
    try io.sleep(.fromMilliseconds(20), .awake);
    const before = Io.Timestamp.now(io, .awake);
    try std.testing.expectError(error.Canceled, future.cancel(io));
    const elapsed = before.durationTo(Io.Timestamp.now(io, .awake));
    if (elapsed.toMilliseconds() >= 500) return error.CancellationWasDelayed;
}

fn addReceive(batch: *Io.Batch, socket: Io.net.Socket, messages: *[1]Io.net.IncomingMessage, data: []u8) void {
    messages.* = .{.init};
    _ = batch.add(.{ .net_receive = .{
        .socket_handle = socket.handle,
        .message_buffer = messages,
        .data_buffer = data,
        .flags = .{},
    } });
}

fn idleBatch(io: Io, sockets: *const [2]Io.net.Socket, timeout: Io.Timeout, asynchronous: bool) !void {
    var storage: [2]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(&storage);
    defer batch.cancel(io);
    var messages: [2][1]Io.net.IncomingMessage = undefined;
    var data: [2][32]u8 = undefined;
    for (sockets, &messages, &data) |socket, *message, *buffer| addReceive(&batch, socket, message, buffer);
    if (asynchronous) try batch.awaitAsync(io) else try batch.awaitConcurrent(io, timeout);
    return error.TestUnexpectedResult;
}

/// Two requests use a standalone batch timer; an untimed batch has no
/// timer to wake it accidentally. Both must be canceled by Future.cancel.
fn testCancelBatch(io: Io, timeout: Io.Timeout, asynchronous: bool) !void {
    const address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var sockets: [2]Io.net.Socket = undefined;
    sockets[0] = try address.bind(io, .{ .mode = .dgram });
    defer sockets[0].close(io);
    sockets[1] = try address.bind(io, .{ .mode = .dgram });
    defer sockets[1].close(io);
    var future = try Io.concurrent(io, idleBatch, .{ io, &sockets, timeout, asynchronous });
    try io.sleep(.fromMilliseconds(20), .awake);
    const before = Io.Timestamp.now(io, .awake);
    try std.testing.expectError(error.Canceled, future.cancel(io));
    if (before.durationTo(Io.Timestamp.now(io, .awake)).toMilliseconds() >= 500)
        return error.CancellationWasDelayed;
}

fn sendLater(io: Io, sender: *const Io.net.Socket, address: *const Io.net.IpAddress) !void {
    try io.sleep(.fromMilliseconds(1), .awake);
    try sender.send(io, address, "ready");
}

fn receiveAfterHandledCancel(io: Io, socket: Io.net.Socket) !void {
    var storage: [1]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(&storage);
    defer batch.cancel(io);
    var messages: [1]Io.net.IncomingMessage = undefined;
    var data: [32]u8 = undefined;
    addReceive(&batch, socket, &messages, &data);
    try std.testing.expectError(error.Canceled, batch.awaitAsync(io));
    // Cancellation of the wait leaves the operations pending. The caller
    // can handle it and await again without a stale timeout marker.
    try batch.awaitAsync(io);
    const completion = batch.next() orelse return error.TestUnexpectedResult;
    const err, const count = completion.result.net_receive;
    try std.testing.expectEqual(null, err);
    try std.testing.expectEqual(1, count);
    try std.testing.expectEqualStrings("ready", messages[0].data);
}

fn testHandledCancel(io: Io) !void {
    const address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var socket = try address.bind(io, .{ .mode = .dgram });
    defer socket.close(io);
    var sender = try address.bind(io, .{ .mode = .dgram });
    defer sender.close(io);
    const Send = struct {
        fn run(io_: Io, sender_: *const Io.net.Socket, target: *const Io.net.IpAddress) !void {
            try io_.sleep(.fromMilliseconds(50), .awake);
            try sender_.send(io_, target, "ready");
        }
    };
    var send = try Io.concurrent(io, Send.run, .{ io, &sender, &socket.address });
    defer send.cancel(io) catch {};
    var receive = try Io.concurrent(io, receiveAfterHandledCancel, .{ io, socket });
    try io.sleep(.fromMilliseconds(20), .awake);
    try receive.cancel(io);
    try send.await(io);
}

/// Readiness wins against an independent timer, then the other receive
/// is canceled. Reusing the batch stack catches late timer completions;
/// several concurrent instances exercise migration between owning rings.
fn timedBatchRounds(io: Io) anyerror!void {
    const address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var sockets: [2]Io.net.Socket = undefined;
    sockets[0] = try address.bind(io, .{ .mode = .dgram });
    defer sockets[0].close(io);
    sockets[1] = try address.bind(io, .{ .mode = .dgram });
    defer sockets[1].close(io);
    var sender = try address.bind(io, .{ .mode = .dgram });
    defer sender.close(io);
    for (0..40) |_| {
        var storage: [2]Io.Operation.Storage = undefined;
        var batch: Io.Batch = .init(&storage);
        defer batch.cancel(io);
        var messages: [2][1]Io.net.IncomingMessage = undefined;
        var data: [2][32]u8 = undefined;
        for (sockets, &messages, &data) |socket, *message, *buffer| addReceive(&batch, socket, message, buffer);
        var send = try Io.concurrent(io, sendLater, .{ io, &sender, &sockets[0].address });
        defer send.cancel(io) catch {};
        try batch.awaitConcurrent(io, .{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } });
        const completion = batch.next() orelse return error.TestUnexpectedResult;
        try std.testing.expectEqual(0, completion.index);
        const err, const count = completion.result.net_receive;
        try std.testing.expectEqual(null, err);
        try std.testing.expectEqual(1, count);
        try std.testing.expectEqualStrings("ready", messages[0][0].data);
        batch.cancel(io);
        try send.await(io);
        // Also exercise timer-first teardown with two pending requests.
        for (sockets, &messages, &data) |socket, *message, *buffer| addReceive(&batch, socket, message, buffer);
        try std.testing.expectError(error.Timeout, batch.awaitConcurrent(io, .{
            .duration = .{ .raw = .fromMilliseconds(1), .clock = .awake },
        }));
        batch.cancel(io);
        try std.testing.expectEqual(null, batch.next());
    }
}

pub fn main() !void {
    for ([_]usize{ 0, 3 }) |thread_limit| {
        var evented: Io.Uring = undefined;
        try evented.init(std.heap.page_allocator, .{ .thread_limit = thread_limit });
        defer evented.deinit();
        const io = evented.io();
        try testCancelLinkedReceive(io);
        try testCancelBatch(io, .{ .duration = .{ .raw = .fromMilliseconds(1000), .clock = .awake } }, false);
        try testCancelBatch(io, .none, false);
        try testCancelBatch(io, .none, true);
        try testHandledCancel(io);
        var futures: [4]Io.Future(anyerror!void) = undefined;
        for (&futures) |*future| future.* = try Io.concurrent(io, timedBatchRounds, .{io});
        for (&futures) |*future| try future.await(io);
    }
}

// run
// target=aarch64-linux,x86_64-linux
