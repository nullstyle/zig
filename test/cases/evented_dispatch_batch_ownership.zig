const std = @import("std");
const Io = std.Io;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const short: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(2), .clock = .awake } };
const long: Io.Timeout = .{ .duration = .{ .raw = .fromSeconds(3), .clock = .awake } };

fn bind(io: Io) !Io.net.Socket {
    return (Io.net.IpAddress{ .ip4 = .loopback(0) }).bind(io, .{ .mode = .dgram });
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

fn socketPair() ![2]std.c.fd_t {
    var pair: [2]std.c.fd_t = undefined;
    if (std.c.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &pair) != 0)
        return error.SocketPairFailed;
    errdefer {
        for (pair) |fd| _ = std.c.close(fd);
    }
    for (pair) |fd| {
        const old = std.c.fcntl(fd, std.posix.F.GETFL, @as(usize, 0));
        if (old < 0 or std.c.fcntl(fd, std.posix.F.SETFL, @as(usize, @intCast(old)) | (1 << @bitOffsetOf(std.posix.O, "NONBLOCK"))) < 0)
            return error.SetFlagsFailed;
    }
    return pair;
}

/// Adding to a partially pending batch must keep its original queue and
/// readiness registrations, even when a new network request fits the cache.
fn testIncrementalBatch(io: Io) !void {
    const receiver = try bind(io);
    defer receiver.close(io);
    const sender = try bind(io);
    defer sender.close(io);
    var storage: [2]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(&storage);
    defer batch.cancel(io);
    var messages: [1]Io.net.IncomingMessage = undefined;
    var data: [32]u8 = undefined;
    addReceive(&batch, receiver, &messages, &data);
    try std.testing.expectError(error.Timeout, batch.awaitConcurrent(io, short));
    var outgoing = [_]Io.net.OutgoingMessage{.{ .address = &receiver.address, .data_ptr = "added", .data_len = 5 }};
    _ = batch.add(.{ .net_send = .{ .socket_handle = sender.handle, .messages = &outgoing, .flags = .{} } });
    try batch.awaitConcurrent(io, long);
    const sent = batch.next() orelse return error.MissingCompletion;
    try expectEqual(1, sent.index);
    try expectEqual(null, sent.result.net_send[0]);
    try expectEqual(1, sent.result.net_send[1]);
    try batch.awaitConcurrent(io, long);
    const received = batch.next() orelse return error.MissingCompletion;
    try expectEqual(0, received.index);
    try expectEqual(null, received.result.net_receive[0]);
    try expectEqual(1, received.result.net_receive[1]);
    try std.testing.expectEqualStrings("added", messages[0].data);

    // Keep a file source pending while adding a network request. Cached
    // userdata must never replace a live general batch queue.
    const pair = try socketPair();
    defer {
        for (pair) |fd| _ = std.c.close(fd);
    }
    var file_bytes: [8]u8 = undefined;
    var vectors = [_][]u8{&file_bytes};
    _ = batch.add(.{ .file_read_streaming = .{
        .file = .{ .handle = pair[0], .flags = .{ .nonblocking = true } },
        .data = &vectors,
    } });
    try std.testing.expectError(error.Timeout, batch.awaitConcurrent(io, short));
    addReceive(&batch, receiver, &messages, &data);
    try sender.send(io, &receiver.address, "network");
    try batch.awaitConcurrent(io, long);
    const network = batch.next() orelse return error.MissingCompletion;
    try expectEqual(null, network.result.net_receive[0]);
    try std.testing.expectEqualStrings("network", messages[0].data);
    try expectEqual(4, std.c.write(pair[1], "file", 4));
    try batch.awaitConcurrent(io, long);
    const file = batch.next() orelse return error.MissingCompletion;
    try expectEqual(4, try file.result.file_read_streaming);
    try std.testing.expectEqualStrings("file", file_bytes[0..4]);
}

fn readOne(io: Io, fd: std.c.fd_t, entered: *Io.Event) anyerror!void {
    var bytes: [1]u8 = undefined;
    var vectors = [_][]u8{&bytes};
    entered.set(io);
    const result = try io.operateTimeout(.{ .net_read = .{ .socket_handle = fd, .data = &vectors } }, long);
    try expectEqual(1, (try result.net_read).data_len);
    try expectEqual('r', bytes[0]);
}

fn writeOne(io: Io, fd: std.c.fd_t, entered: *Io.Event) anyerror!void {
    entered.set(io);
    const result = try io.operateTimeout(.{ .net_write = .{ .socket_handle = fd, .data = &.{"w"} } }, long);
    try expectEqual(1, try result.net_write);
}

/// Independent read and write batches cannot share the cached entry's one
/// waiter. Cancellation of one direction must leave the other fiber alive.
fn testOppositeDirections(io: Io) !void {
    const pair = try socketPair();
    defer {
        // Timed network operations use the backend's per-socket cache, so
        // close through the same I/O instance before descriptors are reused.
        for (pair) |fd| (Io.net.Socket{ .handle = fd, .address = .{ .ip4 = .loopback(0) } }).close(io);
    }
    const fill: [4096]u8 = @splat(0);
    while (true) switch (std.c.errno(std.c.write(pair[0], &fill, fill.len))) {
        .SUCCESS, .INTR => {},
        .AGAIN => break,
        else => return error.FillFailed,
    };
    var read_entered: Io.Event = .unset;
    var write_entered: Io.Event = .unset;
    var reader = try Io.concurrent(io, readOne, .{ io, pair[0], &read_entered });
    defer reader.cancel(io) catch {};
    try read_entered.wait(io);
    try io.sleep(.fromMilliseconds(5), .awake);
    var writer = try Io.concurrent(io, writeOne, .{ io, pair[0], &write_entered });
    try write_entered.wait(io);
    try io.sleep(.fromMilliseconds(5), .awake);
    try std.testing.expectError(error.Canceled, writer.cancel(io));
    try expectEqual(1, std.c.write(pair[1], "r", 1));
    try reader.await(io);
}

fn mapWorker(io: Io, ready: *Io.Event) anyerror!void {
    try ready.wait(io);
    for (0..24) |_| {
        const socket = try bind(io);
        defer socket.close(io);
        var messages: [1]Io.net.IncomingMessage = .{.init};
        var data: [8]u8 = undefined;
        const err, const received = socket.receiveManyTimeout(io, &messages, &data, .{}, short);
        try expectEqual(error.Timeout, err.?);
        try expectEqual(0, received);
    }
}

fn testConcurrentMap(io: Io) !void {
    var ready: Io.Event = .unset;
    var futures: [12]Io.Future(anyerror!void) = undefined;
    for (&futures) |*future| future.* = try Io.concurrent(io, mapWorker, .{ io, &ready });
    ready.set(io);
    for (&futures) |*future| try future.await(io);
}

/// A single storage can be reused on another socket after completion. The
/// old tagged entry must release its lease before the new submission.
fn testReuseOneSlot(io: Io) !void {
    const first = try bind(io);
    defer first.close(io);
    const second = try bind(io);
    defer second.close(io);
    const sender = try bind(io);
    defer sender.close(io);
    var storage: [1]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(&storage);
    defer batch.cancel(io);
    var messages: [1]Io.net.IncomingMessage = undefined;
    var data: [8]u8 = undefined;
    for ([_]Io.net.Socket{ first, second, first, second }) |socket| {
        addReceive(&batch, socket, &messages, &data);
        try std.testing.expectError(error.Timeout, batch.awaitConcurrent(io, short));
        try sender.send(io, &socket.address, "reuse");
        try batch.awaitConcurrent(io, long);
        const completion = batch.next() orelse return error.MissingCompletion;
        try expectEqual(null, completion.result.net_receive[0]);
        try std.testing.expectEqualStrings("reuse", messages[0].data);
    }
}

pub fn main() !void {
    var gpa: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(gpa.deinit() == .ok);
    var evented: Io.Dispatch = undefined;
    try evented.init(gpa.allocator(), .{ .leeway = .zero });
    var deinitialized = false;
    defer if (!deinitialized) evented.deinit();
    const io = evented.io();
    try testIncrementalBatch(io);
    try testOppositeDirections(io);
    try testReuseOneSlot(io);
    try testConcurrentMap(io);
    evented.deinit();
    deinitialized = true;
    // Source cancel handlers must have released all cached allocations
    // before returning control to the allocator's owner.
    try expect(evented.live_net_entries.load(.acquire) == 0);
}

// run
// backend=llvm
// target=aarch64-macos,x86_64-macos
// link_libc=true
