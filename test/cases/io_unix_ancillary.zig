const std = @import("std");
const builtin = @import("builtin");
const Io = std.Io;
const posix = std.posix;
const cmsg = Io.net.cmsg;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const Mode = enum { direct, async, timed };

fn operate(io: Io, mode: Mode, op: Io.Operation) !Io.Operation.Result {
    if (mode == .direct) return io.operate(op);
    var storage: [1]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(&storage);
    defer batch.cancel(io);
    _ = batch.add(op);
    if (mode == .async) try batch.awaitAsync(io) else try batch.awaitConcurrent(io, .{
        .duration = .{ .raw = .fromSeconds(2), .clock = .awake },
    });
    return (batch.next() orelse return error.MissingCompletion).result;
}

fn countFds() usize {
    var n: usize = 0;
    for (0..4096) |fd| {
        if (std.c.fcntl(@intCast(fd), posix.F.GETFD) != -1) n += 1;
    }
    return n;
}

fn rights(buf: []align(Io.net.cmsg_align) u8, fds: []const posix.fd_t) []const u8 {
    const bytes = std.mem.sliceAsBytes(fds);
    const size = cmsg.space(bytes.len);
    @memset(buf[0..size], 0);
    const hdr: *align(Io.net.cmsg_align) posix.cmsghdr = @ptrCast(buf.ptr);
    hdr.* = .{ .len = @intCast(cmsg.len(@intCast(bytes.len))), .level = posix.SOL.SOCKET, .type = posix.SCM.RIGHTS };
    @memcpy(cmsg.data(hdr), bytes);
    return buf[0..size];
}

fn testReceive(io: Io, mode: Mode, capacity: usize, n_fds: usize, cloexec: bool, datagram: bool) !void {
    var pair: [2]posix.fd_t = undefined;
    try expect(std.c.socketpair(posix.AF.UNIX, if (datagram) posix.SOCK.DGRAM else posix.SOCK.STREAM, 0, &pair) == 0);
    const sockets: [2]Io.net.Socket = .{
        .{ .handle = pair[0], .address = .{ .ip4 = .loopback(0) } },
        .{ .handle = pair[1], .address = .{ .ip4 = .loopback(0) } },
    };
    defer Io.net.Socket.closeMany(io, &sockets);
    for (pair) |fd| {
        const flags = std.c.fcntl(fd, posix.F.GETFL);
        try expect(std.c.fcntl(fd, posix.F.SETFL, flags | (1 << @bitOffsetOf(posix.O, "NONBLOCK"))) == 0);
    }
    // Warm scheduler resources before taking the descriptor baseline.
    try expectEqual(1, std.c.write(pair[0], "W", 1));
    var warm_byte: [1]u8 = undefined;
    var warm_vectors: [1][]u8 = .{&warm_byte};
    if (datagram) {
        var warm_messages: [1]Io.net.IncomingMessage = .{.init};
        const err, const n = (try operate(io, mode, .{ .net_receive = .{
            .socket_handle = pair[1],
            .message_buffer = &warm_messages,
            .data_buffer = &warm_byte,
            .flags = .{},
        } })).net_receive;
        if (err) |e| return e;
        try expectEqual(1, n);
    } else _ = try (try operate(io, mode, .{ .net_read = .{
        .socket_handle = pair[1],
        .data = &warm_vectors,
    } })).net_read;
    try Io.sleep(io, .fromMilliseconds(10), .awake);
    const before = countFds();
    var fds: [254]posix.fd_t = @splat(pair[0]);
    var outgoing: [cmsg.space(@sizeOf(@TypeOf(fds)))]u8 align(Io.net.cmsg_align) = undefined;
    var iov: posix.iovec_const = .{ .base = "Z", .len = 1 };
    const ctl = rights(&outgoing, fds[0..n_fds]);
    const msg: posix.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = (&iov)[0..1],
        .iovlen = 1,
        .control = ctl.ptr,
        .controllen = @intCast(ctl.len),
        .flags = 0,
    };
    try expectEqual(1, std.c.sendmsg(pair[0], &msg, 0));
    var data: [1]u8 = undefined;
    var vectors: [1][]u8 = .{&data};
    var incoming: [2048]u8 align(Io.net.cmsg_align) = @splat(0);
    const result: Io.net.Stream.ReadResult = if (datagram) result: {
        var messages: [1]Io.net.IncomingMessage = .{.init};
        messages[0].control = incoming[0..capacity];
        const err, const n = (try operate(io, mode, .{ .net_receive = .{
            .socket_handle = pair[1],
            .message_buffer = &messages,
            .data_buffer = &data,
            .flags = .{},
        } })).net_receive;
        if (err) |e| return e;
        try expectEqual(1, n);
        break :result .{ .data_len = messages[0].data.len, .control_len = messages[0].control.len, .control_truncated = messages[0].flags.ctrunc };
    } else try (try operate(io, mode, .{ .net_read = .{
        .socket_handle = pair[1],
        .data = &vectors,
        .control = incoming[0..capacity],
    } })).net_read;
    try expectEqual(1, result.data_len);
    try expect(result.control_len <= capacity);
    var it: cmsg.Iterator = .{ .control = incoming[0..result.control_len] };
    var delivered: usize = 0;
    var inherited = false;
    while (it.next()) |m| {
        try expectEqual(posix.SOL.SOCKET, m.header.level);
        try expectEqual(posix.SCM.RIGHTS, m.header.type);
        try expectEqual(0, m.data.len % @sizeOf(posix.fd_t));
        for (std.mem.bytesAsSlice(posix.fd_t, m.data)) |fd| {
            const descriptor_flags = std.c.fcntl(fd, posix.F.GETFD);
            try expect(descriptor_flags != -1);
            inherited = inherited or (descriptor_flags & posix.FD_CLOEXEC == 0);
            _ = std.c.close(fd);
            delivered += 1;
        }
    }
    std.debug.print("{t} {t}: capacity={d} sent={d} delivered={d} truncated={} fd_delta={d}\n", .{
        builtin.os.tag,                                                  mode, capacity, n_fds, delivered, result.control_truncated,
        @as(isize, @intCast(countFds())) - @as(isize, @intCast(before)),
    });
    try expectEqual(before, countFds());
    if (cloexec) try expect(!inherited);
    if (capacity >= ctl.len) try expectEqual(n_fds, delivered) else try expect(result.control_truncated);
}

fn canceledReceive(io: Io, fd: posix.fd_t, data: []u8, control: []u8) !void {
    var messages: [1]Io.net.IncomingMessage = .{.init};
    messages[0].control = control;
    const err, const count = (try io.operate(.{ .net_receive = .{
        .socket_handle = fd,
        .message_buffer = &messages,
        .data_buffer = data,
        .flags = .{},
    } })).net_receive;
    if (err) |e| return e;
    _ = count;
    return error.UnexpectedReceive;
}

fn testCancellation(io: Io) !void {
    var pair: [2]posix.fd_t = undefined;
    try expect(std.c.socketpair(posix.AF.UNIX, posix.SOCK.DGRAM, 0, &pair) == 0);
    const sockets: [2]Io.net.Socket = .{
        .{ .handle = pair[0], .address = .{ .ip4 = .loopback(0) } },
        .{ .handle = pair[1], .address = .{ .ip4 = .loopback(0) } },
    };
    defer Io.net.Socket.closeMany(io, &sockets);
    for (pair) |fd| {
        const flags = std.c.fcntl(fd, posix.F.GETFL);
        try expect(std.c.fcntl(fd, posix.F.SETFL, flags | (1 << @bitOffsetOf(posix.O, "NONBLOCK"))) == 0);
    }
    var data: [1]u8 = undefined;
    var control: [cmsg.space(@sizeOf(posix.fd_t))]u8 align(Io.net.cmsg_align) = undefined;
    for (0..16) |_| {
        var future = Io.async(io, canceledReceive, .{ io, pair[1], &data, &control });
        try Io.sleep(io, .fromMilliseconds(2), .awake);
        try std.testing.expectError(error.Canceled, future.cancel(io));
        @memset(&control, 0xaa);
        const before = countFds();
        const ctl = rights(&control, &.{pair[0]});
        var iov: posix.iovec_const = .{ .base = "C", .len = 1 };
        const msg: posix.msghdr_const = .{
            .name = null,
            .namelen = 0,
            .iov = (&iov)[0..1],
            .iovlen = 1,
            .control = ctl.ptr,
            .controllen = @intCast(ctl.len),
            .flags = 0,
        };
        try expectEqual(1, std.c.sendmsg(pair[0], &msg, 0));
        var incoming: [1]Io.net.IncomingMessage = .{.init};
        incoming[0].control = &control;
        const err, const n = (try operate(io, .timed, .{ .net_receive = .{
            .socket_handle = pair[1],
            .message_buffer = &incoming,
            .data_buffer = &data,
            .flags = .{},
        } })).net_receive;
        if (err) |e| return e;
        try expectEqual(1, n);
        var it: cmsg.Iterator = .{ .control = @alignCast(incoming[0].control) };
        const message = it.next() orelse return error.MissingControl;
        try expectEqual(@sizeOf(posix.fd_t), message.data.len);
        const fd = std.mem.bytesToValue(posix.fd_t, message.data);
        const descriptor_flags = std.c.fcntl(fd, posix.F.GETFD);
        try expect(descriptor_flags != -1 and descriptor_flags & posix.FD_CLOEXEC != 0);
        _ = std.c.close(fd);
        try expectEqual(before, countFds());
    }
}

fn testSendErrors(io: Io) !void {
    var pair: [2]posix.fd_t = undefined;
    try expect(std.c.socketpair(posix.AF.UNIX, posix.SOCK.STREAM, 0, &pair) == 0);
    const sockets: [2]Io.net.Socket = .{
        .{ .handle = pair[0], .address = .{ .ip4 = .loopback(0) } },
        .{ .handle = pair[1], .address = .{ .ip4 = .loopback(0) } },
    };
    defer Io.net.Socket.closeMany(io, &sockets);
    var fds: [255]posix.fd_t = @splat(pair[0]);
    var control: [cmsg.space(@sizeOf(@TypeOf(fds)))]u8 align(Io.net.cmsg_align) = undefined;
    for (std.enums.values(Mode)) |mode| {
        const bad = rights(&control, &.{-1});
        try std.testing.expectError(error.InvalidControlMessage, (try operate(io, mode, .{ .net_write = .{
            .socket_handle = pair[0],
            .data = &.{"X"},
            .control = bad,
        } })).net_write);
        const too_many = rights(&control, fds[0..if (builtin.os.tag == .linux) 254 else 255]);
        try std.testing.expectError(error.InvalidControlMessage, (try operate(io, mode, .{ .net_write = .{
            .socket_handle = pair[0],
            .data = &.{"X"},
            .control = too_many,
        } })).net_write);
        // The invalid length must be checked before the pointer is dereferenced
        // or narrowed to the native controllen type.
        const huge: []const u8 = @as([*]const u8, @ptrFromInt(1))[0 .. @as(usize, std.math.maxInt(i32)) + 1];
        try std.testing.expectError(error.InvalidControlMessage, (try operate(io, mode, .{ .net_write = .{
            .socket_handle = pair[0],
            .data = &.{"X"},
            .control = huge,
        } })).net_write);
    }
}

fn run(io: Io, only_truncation: bool, datagram_only: bool) !void {
    for ([_]bool{ false, true }) |datagram| {
        if (datagram_only and !datagram) continue;
        for (std.enums.values(Mode)) |mode| {
            if (only_truncation) {
                try testReceive(io, mode, cmsg.space(@sizeOf(posix.fd_t)), 3, false, datagram);
            } else {
                for ([_]usize{ 0, 1, @sizeOf(posix.cmsghdr) - 1, cmsg.space(@sizeOf(posix.fd_t)), 2048 }) |capacity|
                    try testReceive(io, mode, capacity, 3, true, datagram);
                try testReceive(io, mode, 2048, if (builtin.os.tag == .linux) 253 else 254, true, datagram);
            }
        }
    }
    if (!only_truncation and !datagram_only) try testSendErrors(io);
}

pub fn main(init: std.process.Init) !void {
    const args = try init.minimal.args.toSlice(init.arena.allocator());
    const backend = if (args.len > 1) args[1] else "threaded";
    const truncation = args.len > 2 and std.mem.eql(u8, args[2], "truncation");
    if (std.mem.eql(u8, backend, "threaded")) {
        try run(init.io, truncation, false);
    } else if (builtin.os.tag == .macos and std.mem.eql(u8, backend, "dispatch")) {
        var ev: Io.Evented = undefined;
        try ev.init(std.heap.page_allocator, .{ .leeway = .zero });
        defer ev.deinit();
        try run(ev.io(), truncation, false);
        try testCancellation(ev.io());
    } else if (builtin.os.tag == .macos and std.mem.eql(u8, backend, "kqueue")) {
        var ev: Io.Kqueue = undefined;
        try ev.init(std.heap.page_allocator, .{ .n_threads = 1 });
        defer ev.deinit();
        try run(ev.io(), truncation, false);
        try testCancellation(ev.io());
    } else if (builtin.os.tag == .linux and std.mem.eql(u8, backend, "uring")) {
        var ev: Io.Evented = undefined;
        try ev.init(std.heap.page_allocator, .{});
        defer ev.deinit();
        try run(ev.io(), truncation, true);
        try testCancellation(ev.io());
    } else return error.UnknownBackend;
}

// run
// backend=llvm
// target=aarch64-macos,x86_64-linux,aarch64-linux
// link_libc=true
