const std = @import("std");
const Io = std.Io;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;
const Mode = enum { direct, async, timed };

fn operate(io: Io, mode: Mode, operation: Io.Operation) !Io.Operation.Result {
    if (mode == .direct) return io.operate(operation);
    var storage: [1]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(&storage);
    defer batch.cancel(io);
    _ = batch.add(operation);
    switch (mode) {
        .direct => unreachable,
        .async => try batch.awaitAsync(io),
        .timed => try batch.awaitConcurrent(io, .{
            .duration = .{ .raw = .fromSeconds(2), .clock = .awake },
        }),
    }
    return (batch.next() orelse return error.MissingCompletion).result;
}

fn write(io: Io, mode: Mode, fd: std.posix.fd_t, splat: usize) !usize {
    return (try operate(io, mode, .{ .net_write = .{
        .socket_handle = fd,
        .header = "H",
        .data = &.{ "ab", "xy" },
        .splat = splat,
    } })).net_write;
}

fn read(io: Io, mode: Mode, fd: std.posix.fd_t, expected: []const u8) !void {
    var first: [2]u8 = undefined;
    var middle: [3]u8 = undefined;
    var last: [4]u8 = undefined;
    var vectors = [_][]u8{ &.{}, &first, &middle, &last };
    const n = try (try operate(io, mode, .{ .net_read = .{
        .socket_handle = fd,
        .data = &vectors,
    } })).net_read;
    try expectEqual(expected.len, n);
    var joined: [9]u8 = undefined;
    var copied: usize = 0;
    for (vectors) |vector| {
        const count = @min(vector.len, n - copied);
        @memcpy(joined[copied..][0..count], vector[0..count]);
        copied += count;
    }
    try expectEqualStrings(expected, joined[0..n]);
}

fn delayedWrite(io: Io, fd: std.posix.fd_t) !void {
    try Io.sleep(io, .fromMilliseconds(10), .awake);
    try expectEqual(9, try write(io, .direct, fd, 3));
}

fn testPartialSendError(io: Io) !void {
    const receiver = try (Io.net.IpAddress{ .ip4 = .loopback(0) }).bind(io, .{ .mode = .dgram });
    defer receiver.close(io);
    const sender = try (Io.net.IpAddress{ .ip4 = .loopback(0) }).bind(io, .{ .mode = .dgram });
    defer sender.close(io);
    var oversized: [65536]u8 = @splat(0);
    for (std.enums.values(Mode)) |mode| {
        var messages = [_]Io.net.OutgoingMessage{
            .{ .address = &receiver.address, .data_ptr = "sent", .data_len = 4 },
            .{ .address = &receiver.address, .data_ptr = &oversized, .data_len = oversized.len },
        };
        const result = try operate(io, mode, .{ .net_send = .{
            .socket_handle = sender.handle,
            .messages = &messages,
            .flags = .{ .dont_route = true },
        } });
        const err, const sent = result.net_send;
        try expectEqual(error.MessageOversize, err.?);
        try expectEqual(1, sent);
        var bytes: [8]u8 = undefined;
        const received = try receiver.receive(io, &bytes);
        try expectEqualStrings("sent", received.data);
    }
}

pub fn main() !void {
    var kqueue: Io.Kqueue = undefined;
    try Io.Kqueue.init(&kqueue, std.heap.page_allocator, .{ .n_threads = 1 });
    defer kqueue.deinit();
    const io = kqueue.io();
    try testPartialSendError(io);
    var pair: [2]std.posix.fd_t = undefined;
    if (std.c.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &pair) != 0)
        return error.SocketPairFailed;
    defer {
        for (pair) |fd| _ = std.c.close(fd);
    }
    for (pair) |fd| {
        const old = std.c.fcntl(fd, std.posix.F.GETFL);
        if (old == -1 or std.c.fcntl(fd, std.posix.F.SETFL, old | (1 << @bitOffsetOf(std.posix.O, "NONBLOCK"))) == -1)
            return error.SetFlagsFailed;
    }
    for (std.enums.values(Mode)) |mode| {
        try expectEqual(9, try write(io, mode, pair[0], 3));
        try read(io, mode, pair[1], "Habxyxyxy");
        try expectEqual(3, try write(io, mode, pair[0], 0));
        try read(io, mode, pair[1], "Hab");

        var empty_vectors: [0][]u8 = .{};
        try expectEqual(0, try (try operate(io, mode, .{ .net_read = .{
            .socket_handle = pair[1],
            .data = &empty_vectors,
        } })).net_read);
        try expectEqual(0, try (try operate(io, mode, .{ .net_write = .{
            .socket_handle = pair[0],
            .data = &.{""},
            .splat = 0,
        } })).net_write);

        try expectEqual(0, try (try operate(io, mode, .{ .net_write = .{
            .socket_handle = pair[0],
            .data = &.{},
        } })).net_write);

        var writer = Io.async(io, delayedWrite, .{ io, pair[0] });
        try read(io, mode, pair[1], "Habxyxyxy");
        try writer.await(io);
    }
}

// run
// backend=llvm
// target=aarch64-macos,x86_64-macos,aarch64-freebsd,x86_64-freebsd,aarch64-openbsd,x86_64-openbsd
// link_libc=true
