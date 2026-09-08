const std = @import("std");
const Io = std.Io;
const expect = std.testing.expect;
const expectError = std.testing.expectError;
pub const std_options: std.Options = .{ .log_level = .warn };

fn sleeping(io: Io, entered: *std.atomic.Value(bool)) Io.Cancelable!void {
    entered.store(true, .release);
    try Io.sleep(io, .fromSeconds(30), .awake);
}

fn testSleepCancellation(io: Io) !void {
    var entered: std.atomic.Value(bool) = .init(false);
    var future = try Io.concurrent(io, sleeping, .{ io, &entered });
    while (!entered.load(.acquire)) try Io.sleep(io, .fromMilliseconds(1), .awake);
    try Io.sleep(io, .fromMilliseconds(5), .awake);
    try expectError(error.Canceled, future.cancel(io));
}

fn groupSleeper(io: Io, started: *std.atomic.Value(usize), canceled: *std.atomic.Value(usize)) Io.Cancelable!void {
    _ = started.fetchAdd(1, .release);
    Io.sleep(io, .fromSeconds(30), .awake) catch |err| {
        _ = canceled.fetchAdd(1, .release);
        return err;
    };
}

fn groupTask(io: Io, started: *std.atomic.Value(usize), canceled: *std.atomic.Value(usize)) Io.Cancelable!void {
    var group: Io.Group = .init;
    defer group.cancel(io);
    for (0..8) |_| group.async(io, groupSleeper, .{ io, started, canceled });
    try group.await(io);
}

fn testGroups(io: Io) !void {
    var started: std.atomic.Value(usize) = .init(0);
    var canceled: std.atomic.Value(usize) = .init(0);
    var group: Io.Group = .init;
    defer group.cancel(io);
    for (0..8) |_| try group.concurrent(io, groupSleeper, .{ io, &started, &canceled });
    while (started.load(.acquire) < 8) try Io.sleep(io, .fromMilliseconds(1), .awake);
    group.cancel(io);
    try expect(canceled.load(.acquire) == 8);
    try group.await(io);
    started.store(0, .release);
    canceled.store(0, .release);
    var future = try Io.concurrent(io, groupTask, .{ io, &started, &canceled });
    while (started.load(.acquire) < 8) try Io.sleep(io, .fromMilliseconds(1), .awake);
    try expectError(error.Canceled, future.cancel(io));
    try expect(canceled.load(.acquire) == 8);
}

fn accepting(io: Io, server: *Io.net.Server, entered: *std.atomic.Value(usize)) !void {
    _ = entered.fetchAdd(1, .release);
    const stream = try server.accept(io);
    stream.close(io);
}

fn testSharedAccept(io: Io) !void {
    var server = try (Io.net.IpAddress{ .ip4 = .loopback(0) }).listen(io, .{});
    defer server.deinit(io);
    var entered: std.atomic.Value(usize) = .init(0);
    var first = try Io.concurrent(io, accepting, .{ io, &server, &entered });
    var second = try Io.concurrent(io, accepting, .{ io, &server, &entered });
    while (entered.load(.acquire) < 2) try Io.sleep(io, .fromMilliseconds(1), .awake);
    try Io.sleep(io, .fromMilliseconds(5), .awake);
    try expectError(error.Canceled, first.cancel(io));
    const client = try server.socket.address.connect(io, .{ .mode = .stream });
    defer client.close(io);
    try second.await(io);
    for (0..64) |_| {
        var immediate = Io.async(io, accepting, .{ io, &server, &entered });
        try expectError(error.Canceled, immediate.cancel(io));
    }
}

fn protectedSleep(io: Io, entered: *std.atomic.Value(bool)) !void {
    const start = Io.Clock.awake.now(io);
    const previous = io.swapCancelProtection(.blocked);
    entered.store(true, .release);
    Io.sleep(io, .fromMilliseconds(20), .awake) catch unreachable;
    _ = io.swapCancelProtection(previous);
    try expect(start.durationTo(Io.Clock.awake.now(io)).toMilliseconds() >= 20);
    try io.checkCancel();
    return error.TestUnexpectedResult;
}

fn testProtection(io: Io) !void {
    var entered: std.atomic.Value(bool) = .init(false);
    var future = try Io.concurrent(io, protectedSleep, .{ io, &entered });
    while (!entered.load(.acquire)) try Io.sleep(io, .fromMilliseconds(1), .awake);
    try expectError(error.Canceled, future.cancel(io));
}

fn testBatchTimeouts(io: Io) !void {
    const socket = try (Io.net.IpAddress{ .ip4 = .loopback(0) }).bind(io, .{ .mode = .dgram });
    defer socket.close(io);
    var storage: [1]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(&storage);
    defer batch.cancel(io);
    var messages: [1]Io.net.IncomingMessage = .{.init};
    var data: [32]u8 = undefined;
    _ = batch.add(.{ .net_receive = .{ .socket_handle = socket.handle, .message_buffer = &messages, .data_buffer = &data, .flags = .{} } });
    for ([_]Io.Clock{ .awake, .real }) |clock| {
        for (0..8) |_| {
            try expectError(error.Timeout, batch.awaitConcurrent(io, .{ .duration = .{ .raw = .fromMilliseconds(2), .clock = clock } }));
            try expectError(error.Timeout, batch.awaitConcurrent(io, .{ .deadline = .{ .raw = .zero, .clock = clock } }));
        }
    }
}

fn testConnectAndUnix(io: Io) !void {
    var server = try (Io.net.IpAddress{ .ip4 = .loopback(0) }).listen(io, .{});
    const address = server.socket.address;
    const client = try address.connect(io, .{ .mode = .stream, .timeout = .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } } });
    defer client.close(io);
    const peer = try server.accept(io);
    defer peer.close(io);
    server.deinit(io);
    try expectError(error.ConnectionRefused, address.connect(io, .{ .mode = .stream, .timeout = .{ .duration = .{ .raw = .fromSeconds(2), .clock = .real } } }));
    var path: [100]u8 = undefined;
    const unix = try Io.net.UnixAddress.init(try std.fmt.bufPrint(&path, "/tmp/zigfork-kqueue-{d}.sock", .{std.c.getpid()}));
    Io.Dir.cwd().deleteFile(io, unix.path) catch {};
    defer Io.Dir.cwd().deleteFile(io, unix.path) catch {};
    var listener = try unix.listen(io, .{});
    defer listener.deinit(io);
    const local = try unix.connect(io);
    defer local.close(io);
    const accepted = try listener.accept(io);
    defer accepted.close(io);
    const sent = try io.operate(.{ .net_write = .{ .socket_handle = local.socket.handle, .data = &.{"unix"} } });
    try expect(try sent.net_write == 4);
    var bytes: [4]u8 = undefined;
    var data = [_][]u8{&bytes};
    try expect(try accepted.read(io, &data) == 4);
    try std.testing.expectEqualStrings("unix", &bytes);
}

fn eventWait(io: Io, event: *Io.Event) Io.Cancelable!void {
    try event.wait(io);
}

fn testFutexCancellation(io: Io) !void {
    var event: Io.Event = .unset;
    var future = try Io.concurrent(io, eventWait, .{ io, &event });
    try Io.sleep(io, .fromMilliseconds(5), .awake);
    try expectError(error.Canceled, future.cancel(io));
    var second = try Io.concurrent(io, eventWait, .{ io, &event });
    try Io.sleep(io, .fromMilliseconds(5), .awake);
    event.set(io);
    try second.await(io);
}

fn awaitLargeBatch(io: Io, batch: *Io.Batch, entered: *Io.Event) !u32 {
    entered.set(io);
    try batch.awaitConcurrent(io, .{ .duration = .{ .raw = .fromSeconds(1), .clock = .awake } });
    const completion = batch.next() orelse return error.MissingCompletion;
    try expect(completion.result.net_receive[0] == null);
    return completion.index;
}

fn testLargeBatch(io: Io) !void {
    const count = 80; // Exceeds one kevent changelist buffer.
    var sockets: [count]Io.net.Socket = undefined;
    var initialized: usize = 0;
    defer for (sockets[0..initialized]) |socket| socket.close(io);
    for (&sockets) |*socket| {
        socket.* = try (Io.net.IpAddress{ .ip4 = .loopback(0) }).bind(io, .{ .mode = .dgram });
        initialized += 1;
    }
    var storage: [count]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(&storage);
    defer batch.cancel(io);
    var messages: [count][1]Io.net.IncomingMessage = @splat(.{.init});
    var data: [count][8]u8 = undefined;
    for (sockets, &messages, &data) |socket, *message, *bytes|
        _ = batch.add(.{ .net_receive = .{ .socket_handle = socket.handle, .message_buffer = message, .data_buffer = bytes, .flags = .{} } });
    var entered: Io.Event = .unset;
    var future = try Io.concurrent(io, awaitLargeBatch, .{ io, &batch, &entered });
    try entered.wait(io);
    try Io.sleep(io, .fromMilliseconds(5), .awake);
    const sender = try (Io.net.IpAddress{ .ip4 = .loopback(0) }).bind(io, .{ .mode = .dgram });
    defer sender.close(io);
    try sender.send(io, &sockets[count - 1].address, "last");
    try expect(try future.await(io) == count - 1);
}

fn sharedReceive(io: Io, socket: Io.net.Socket, batched: bool, entered: *Io.Event, done: *std.atomic.Value(bool)) !void {
    entered.set(io);
    var data: [32]u8 = undefined;
    if (batched) {
        var storage: [1]Io.Operation.Storage = undefined;
        var batch: Io.Batch = .init(&storage);
        defer batch.cancel(io);
        var messages: [1]Io.net.IncomingMessage = .{.init};
        _ = batch.add(.{ .net_receive = .{ .socket_handle = socket.handle, .message_buffer = &messages, .data_buffer = &data, .flags = .{} } });
        try batch.awaitAsync(io);
        const completion = batch.next() orelse return error.MissingCompletion;
        try expect(completion.result.net_receive[0] == null);
        try expect(completion.result.net_receive[1] == 1);
        try std.testing.expectEqualStrings("shared", messages[0].data);
    } else {
        const message = try socket.receive(io, &data);
        try std.testing.expectEqualStrings("shared", message.data);
    }
    done.store(true, .release);
}

fn testSharedBatchCancellation(io: Io) !void {
    // A single worker makes all waits share the same kernel descriptor/filter.
    // Cover two batches and both direct/batch registration orders.
    for ([_][2]bool{ .{ true, true }, .{ false, true }, .{ true, false } }) |batched| {
        const socket = try (Io.net.IpAddress{ .ip4 = .loopback(0) }).bind(io, .{ .mode = .dgram });
        defer socket.close(io);
        var first_entered: Io.Event = .unset;
        var second_entered: Io.Event = .unset;
        var first_done: std.atomic.Value(bool) = .init(false);
        var second_done: std.atomic.Value(bool) = .init(false);
        var first = try Io.concurrent(io, sharedReceive, .{ io, socket, batched[0], &first_entered, &first_done });
        defer _ = first.cancel(io) catch {};
        try first_entered.wait(io);
        try io.sleep(.fromMilliseconds(5), .awake);
        var second = try Io.concurrent(io, sharedReceive, .{ io, socket, batched[1], &second_entered, &second_done });
        try second_entered.wait(io);
        try io.sleep(.fromMilliseconds(5), .awake);
        try expectError(error.Canceled, second.cancel(io));
        const sender = try (Io.net.IpAddress{ .ip4 = .loopback(0) }).bind(io, .{ .mode = .dgram });
        defer sender.close(io);
        try sender.send(io, &socket.address, "shared");
        const deadline = Io.Clock.awake.now(io).addDuration(.fromSeconds(1));
        while (!first_done.load(.acquire) and Io.Clock.awake.now(io).nanoseconds < deadline.nanoseconds)
            try io.sleep(.fromMilliseconds(1), .awake);
        try expect(first_done.load(.acquire));
        try first.await(io);
    }
}

fn testBatchRegistrationAllocationFailure() !void {
    var failing: std.testing.FailingAllocator = .init(std.heap.page_allocator, .{});
    var kqueue: Io.Kqueue = undefined;
    try kqueue.init(failing.allocator(), .{ .n_threads = 1 });
    defer kqueue.deinit();
    const io = kqueue.io();
    const socket = try (Io.net.IpAddress{ .ip4 = .loopback(0) }).bind(io, .{ .mode = .dgram });
    defer socket.close(io);
    const sender = try (Io.net.IpAddress{ .ip4 = .loopback(0) }).bind(io, .{ .mode = .dgram });
    defer sender.close(io);
    const count = 65; // Larger than the inline registration buffer.
    var storage: [count]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(&storage);
    defer batch.cancel(io);
    var messages: [count][1]Io.net.IncomingMessage = @splat(.{.init});
    var data: [count][8]u8 = undefined;
    for (&messages, &data) |*message, *bytes|
        _ = batch.add(.{ .net_receive = .{ .socket_handle = socket.handle, .message_buffer = message, .data_buffer = bytes, .flags = .{} } });
    try sender.send(io, &socket.address, "oom");
    failing.fail_index = failing.alloc_index;
    try expectError(error.ConcurrencyUnavailable, batch.awaitConcurrent(io, .none));
    // Async may fall back to one operation, and must not turn OOM into a trap.
    try batch.awaitAsync(io);
    const completion = batch.next() orelse return error.MissingCompletion;
    try expect(completion.index == 0);
    try expect(completion.result.net_receive[0] == null);
    try expect(completion.result.net_receive[1] == 1);
    try std.testing.expectEqualStrings("oom", messages[0][0].data);
}

fn futureValue(n: u32) u32 {
    return n;
}

fn testFutureCompletion(io: Io) !void {
    // An awaiter may recycle immediately after completion is published.
    // The returning fiber must have left its stack before that happens.
    for (0..20000) |i| {
        const expected: u32 = @intCast(i);
        var future = try Io.concurrent(io, futureValue, .{expected});
        try std.testing.expectEqual(expected, future.await(io));
    }
}

fn nestedFutureRounds(io: Io, worker: u32) anyerror!void {
    for (0..1000) |round| {
        var futures: [8]Io.Future(u32) = undefined;
        for (&futures, 0..) |*future, i| {
            const value = worker * 100000 + @as(u32, @intCast(round * futures.len + i));
            future.* = try Io.concurrent(io, futureValue, .{value});
        }
        for (&futures, 0..) |*future, i| {
            const expected = worker * 100000 + @as(u32, @intCast(round * futures.len + i));
            try std.testing.expectEqual(expected, future.await(io));
        }
    }
}

fn testConcurrentFiberReuse(io: Io) !void {
    // Several parents allocate and recycle children concurrently. A plain
    // pointer CAS pool can reinsert a live child after its head cycles back
    // to the same address, even though no allocation has been freed.
    var parents: [4]Io.Future(anyerror!void) = undefined;
    for (&parents, 0..) |*parent, i|
        parent.* = try Io.concurrent(io, nestedFutureRounds, .{ io, @as(u32, @intCast(i)) });
    for (&parents) |*parent| try parent.await(io);
}

pub fn main() !void {
    try testBatchRegistrationAllocationFailure();
    {
        var single: Io.Kqueue = undefined;
        try single.init(std.heap.page_allocator, .{ .n_threads = 1 });
        defer single.deinit();
        try testSharedBatchCancellation(single.io());
    }
    var kqueue: Io.Kqueue = undefined;
    try kqueue.init(std.heap.page_allocator, .{ .n_threads = 4 });
    defer kqueue.deinit();
    const io = kqueue.io();
    try testFutureCompletion(io);
    try testConcurrentFiberReuse(io);
    try testSleepCancellation(io);
    try testGroups(io);
    try testSharedAccept(io);
    try testProtection(io);
    try testBatchTimeouts(io);
    try testConnectAndUnix(io);
    try testFutexCancellation(io);
    try testLargeBatch(io);
}

// run
// backend=llvm
// target=aarch64-macos,x86_64-macos,aarch64-freebsd,x86_64-freebsd,aarch64-openbsd,x86_64-openbsd
// link_libc=true
