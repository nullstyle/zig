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
    const read_result = try (try operate(io, mode, .{ .net_read = .{
        .socket_handle = fd,
        .data = &vectors,
    } })).net_read;
    const n = read_result.data_len;
    try expectEqual(@as(usize, 0), read_result.control_len);
    try expectEqual(false, read_result.control_truncated);
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

fn receiveCapacity(
    io: Io,
    mode: Mode,
    receiver: *const Io.net.Socket,
    messages: []Io.net.IncomingMessage,
    data: []u8,
) !Io.Operation.Result {
    for (messages) |*message| message.* = .init;
    return operate(io, mode, .{ .net_receive = .{
        .socket_handle = receiver.handle,
        .message_buffer = messages,
        .data_buffer = data,
        .flags = .{},
    } });
}

/// Exhausted data space ends a receive batch. Both a queued drain and a
/// readiness wake must leave the following datagram available to the caller.
fn testReceiveDataCapacity(io: Io) !void {
    const address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    const receiver = try address.bind(io, .{ .mode = .dgram });
    defer receiver.close(io);
    const sender = try address.bind(io, .{ .mode = .dgram });
    defer sender.close(io);
    for (std.enums.values(Mode)) |mode| {
        for ([_]usize{ 4, 0 }) |capacity| {
            for ([_]bool{ false, true }) |wake| {
                var messages: [2]Io.net.IncomingMessage = undefined;
                var data: [4]u8 = undefined;
                var outgoing: [2]Io.net.OutgoingMessage = .{
                    .{ .address = &receiver.address, .data_ptr = "full", .data_len = 4 },
                    .{ .address = &receiver.address, .data_ptr = "next", .data_len = 4 },
                };
                const result = if (wake) result: {
                    var future = Io.async(io, receiveCapacity, .{ io, mode, &receiver, &messages, data[0..capacity] });
                    defer _ = future.cancel(io) catch {};
                    // The single worker parks the receiver before queuing
                    // both datagrams without yielding in sendMany.
                    try Io.sleep(io, .fromMilliseconds(10), .awake);
                    try sender.sendMany(io, &outgoing, .{});
                    break :result try future.await(io);
                } else result: {
                    try sender.sendMany(io, &outgoing, .{});
                    break :result try receiveCapacity(io, mode, &receiver, &messages, data[0..capacity]);
                };
                const opt_err, const count = result.net_receive;
                if (opt_err) |err| return err;
                try expectEqual(@as(usize, 1), count);
                try expectEqualStrings("full"[0..capacity], messages[0].data);
                // Darwin does not report MSG_TRUNC for an empty iovec.
                if (capacity != 0) try expectEqual(false, messages[0].flags.trunc);
                var tail: [4]u8 = undefined;
                const next = try receiver.receiveTimeout(io, &tail, .{
                    .duration = .{ .raw = .fromSeconds(2), .clock = .awake },
                });
                // Some kernels treat a zero-length receive as a no-op. In
                // either case, it must not consume the following datagram.
                if (capacity == 0 and std.mem.eql(u8, next.data, "full")) {
                    const after = try receiver.receiveTimeout(io, &tail, .{
                        .duration = .{ .raw = .fromSeconds(2), .clock = .awake },
                    });
                    try expectEqualStrings("next", after.data);
                } else try expectEqualStrings("next", next.data);
            }
        }
    }
}

/// The ReadResult metadata and both ancillary slices survive all three paths.
/// A complete SCM_RIGHTS message is used so no truncation can hide an fd.
fn testControl(io: Io, mode: Mode, pair: [2]std.posix.fd_t) !void {
    const Fd = std.posix.fd_t;
    var outgoing: [Io.net.cmsg.space(@sizeOf(Fd))]u8 align(Io.net.cmsg_align) = @splat(0);
    const header: *align(Io.net.cmsg_align) std.posix.cmsghdr = @ptrCast(&outgoing);
    header.* = .{ .len = Io.net.cmsg.len(@sizeOf(Fd)), .level = std.posix.SOL.SOCKET, .type = std.posix.SCM.RIGHTS };
    @memcpy(Io.net.cmsg.data(header)[0..@sizeOf(Fd)], std.mem.asBytes(&pair[0]));
    const sent = try (try operate(io, mode, .{ .net_write = .{
        .socket_handle = pair[0],
        .data = &.{"C"},
        .control = &outgoing,
    } })).net_write;
    try expectEqual(1, sent);
    var incoming: [Io.net.cmsg.space(@sizeOf(Fd))]u8 align(Io.net.cmsg_align) = @splat(0);
    var byte: [1]u8 = undefined;
    var data: [1][]u8 = .{&byte};
    const received = try (try operate(io, mode, .{ .net_read = .{
        .socket_handle = pair[1],
        .data = &data,
        .control = &incoming,
    } })).net_read;
    try expectEqual(1, received.data_len);
    try expectEqualStrings("C", &byte);
    try std.testing.expect(received.control_len >= Io.net.cmsg.len(@sizeOf(Fd)));
    try expectEqual(false, received.control_truncated);
    var iterator: Io.net.cmsg.Iterator = .{ .control = @alignCast(incoming[0..received.control_len]) };
    const message = iterator.next() orelse return error.MissingControl;
    try expectEqual(std.posix.SOL.SOCKET, message.header.level);
    try expectEqual(std.posix.SCM.RIGHTS, message.header.type);
    try expectEqual(@sizeOf(Fd), message.data.len);
    const received_fd = std.mem.bytesToValue(Fd, message.data[0..@sizeOf(Fd)]);
    defer _ = std.c.close(received_fd);
    try std.testing.expect(received_fd >= 0);
    try std.testing.expect(std.c.fcntl(received_fd, std.posix.F.GETFD) != -1);
}

pub fn main() !void {
    var kqueue: Io.Kqueue = undefined;
    try Io.Kqueue.init(&kqueue, std.heap.page_allocator, .{ .n_threads = 1 });
    defer kqueue.deinit();
    const io = kqueue.io();
    try testPartialSendError(io);
    try testReceiveDataCapacity(io);
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
        try testControl(io, mode, pair);
        try expectEqual(9, try write(io, mode, pair[0], 3));
        try read(io, mode, pair[1], "Habxyxyxy");
        try expectEqual(3, try write(io, mode, pair[0], 0));
        try read(io, mode, pair[1], "Hab");

        var empty_vectors: [0][]u8 = .{};
        try expectEqual(0, (try (try operate(io, mode, .{ .net_read = .{
            .socket_handle = pair[1],
            .data = &empty_vectors,
        } })).net_read).data_len);
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
