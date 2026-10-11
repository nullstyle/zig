const std = @import("std");
const Io = std.Io;
const expect = std.testing.expect;

fn receiveControl(io: Io, stream: Io.net.Stream) !void {
    var data: [1]u8 = undefined;
    var vectors: [1][]u8 = .{&data};
    var control: [128]u8 align(Io.net.cmsg_align) = undefined;
    var storage: [1]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(&storage);
    defer batch.cancel(io);
    _ = batch.add(.{ .net_read = .{ .socket_handle = stream.socket.handle, .data = &vectors, .control = &control } });
    try batch.awaitConcurrent(io, .{ .duration = .{ .raw = .fromSeconds(2), .clock = .awake } });
    const result = try batch.next().?.result.net_read;
    try expect(result.data_len == 1 and data[0] == 'x');
    try expect(result.control_len >= @sizeOf(std.posix.cmsghdr) + @sizeOf(std.c.fd_t));
    try expect(!result.control_truncated);
    // Inspect one well-formed SCM_RIGHTS message directly and close its fd.
    const header: *const std.posix.cmsghdr = @ptrCast(&control);
    try expect(header.level == std.posix.SOL.SOCKET and header.type == std.posix.SCM.RIGHTS);
    const fd: *align(1) const std.c.fd_t = @ptrCast(control[std.mem.alignForward(usize, @sizeOf(std.posix.cmsghdr), Io.net.cmsg_align)..].ptr);
    defer _ = std.c.close(fd.*);
    try expect(std.c.fcntl(fd.*, std.posix.F.GETFD) >= 0);
}

fn checkContract(io: Io) !void {
    var pair: [2]std.c.fd_t = undefined;
    try expect(std.c.socketpair(std.posix.AF.UNIX, std.posix.SOCK.STREAM, 0, &pair) == 0);
    const left: Io.net.Stream = .{ .socket = .{ .handle = pair[0], .address = .{ .ip4 = .loopback(0) } } };
    defer left.close(io);
    const right: Io.net.Stream = .{ .socket = .{ .handle = pair[1], .address = .{ .ip4 = .loopback(0) } } };
    defer right.close(io);
    for (pair) |fd| {
        const flags = std.c.fcntl(fd, std.posix.F.GETFL);
        try expect(std.c.fcntl(fd, std.posix.F.SETFL, @as(usize, @intCast(flags)) | (1 << @bitOffsetOf(std.posix.O, "NONBLOCK"))) == 0);
    }
    var receiver = Io.async(io, receiveControl, .{ io, right });
    defer _ = receiver.cancel(io) catch {};
    try io.sleep(.fromMilliseconds(10), .awake);
    var control: [64]u8 align(Io.net.cmsg_align) = @splat(0);
    const header: *std.posix.cmsghdr = @ptrCast(&control);
    const offset = std.mem.alignForward(usize, @sizeOf(std.posix.cmsghdr), Io.net.cmsg_align);
    header.* = .{ .len = @intCast(offset + @sizeOf(std.c.fd_t)), .level = std.posix.SOL.SOCKET, .type = std.posix.SCM.RIGHTS };
    const fd: *align(1) std.c.fd_t = @ptrCast(control[offset..].ptr);
    fd.* = pair[0];
    const written = try (try io.operate(.{ .net_write = .{ .socket_handle = left.socket.handle, .data = &.{"x"}, .control = control[0..header.len] } })).net_write;
    try expect(written == 1);
    try receiver.await(io);

    // SpawnOptions.exe resolves independently from argv[0].
    const bin = try Io.Dir.cwd().openDir(io, "/bin", .{});
    defer bin.close(io);
    var child = try std.process.spawn(io, .{ .exe = .{ .explicit = .{ .dir = bin, .path = "sh" } }, .argv = &.{ "not-a-real-executable", "-c", "exit 7" } });
    defer child.kill(io);
    try expect((try child.wait(io)).exited == 7);
    const duplicate = std.c.dup(bin.handle);
    try expect(duplicate >= 0);
    const parent = try io.vtable.inheritParentDir(io.userdata, duplicate);
    defer parent.close(io);
    try expect((std.c.fcntl(parent.handle, std.posix.F.GETFD) & std.posix.FD_CLOEXEC) != 0);
}

pub fn main() !void {
    var allocator: std.heap.DebugAllocator(.{}) = .init;
    defer std.debug.assert(allocator.deinit() == .ok);
    var evented: Io.Dispatch = undefined;
    try evented.init(allocator.allocator(), .{ .leeway = .fromMilliseconds(0) });
    defer evented.deinit();
    try checkContract(evented.io());
}

// run
// target=aarch64-macos,x86_64-macos
// backend=llvm
// link_libc=true
