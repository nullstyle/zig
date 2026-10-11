const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const net = std.Io.net;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;

fn roundTrip(path: []const u8) !void {
    const address = try net.UnixAddress.init(path);
    var storage: std.Io.Threaded.UnixAddress = undefined;
    const length = std.Io.Threaded.addressUnixToPosix(&address, &storage);
    const abstract = builtin.os.tag == .linux and address.isAbstract();
    try expectEqual(@offsetOf(posix.sockaddr.un, "path") + path.len + @intFromBool(!abstract), length);
    try std.testing.expectEqualSlices(u8, path, storage.un.path[0..path.len]);
    const fd = std.c.socket(posix.AF.UNIX, posix.SOCK.DGRAM, 0);
    try expect(fd != -1);
    defer _ = std.c.close(fd);
    if (!abstract) {
        defer _ = std.c.unlink(@ptrCast(&storage.un.path));
        try expect(std.c.bind(fd, &storage.any, length) == 0);
    } else try expect(std.c.bind(fd, &storage.any, length) == 0);
    var actual: [256]u8 align(@alignOf(posix.sockaddr.un)) = undefined;
    var actual_length: posix.socklen_t = actual.len;
    try expect(std.c.getsockname(fd, @ptrCast(&actual), &actual_length) == 0);
    try expectEqual(length, actual_length);
    try std.testing.expectEqualSlices(u8, path, actual[@offsetOf(posix.sockaddr.un, "path")..][0..path.len]);
}

pub fn main() !void {
    const capacity = @sizeOf(@FieldType(posix.sockaddr.un, "path"));
    try expect(net.UnixAddress.max_len <= capacity);
    var path: [capacity + 1]u8 = @splat('x');
    const prefix = try std.fmt.bufPrint(&path, "/tmp/zig-unix-{d}-", .{std.c.getpid()});
    @memset(path[prefix.len..], 'x');
    try std.testing.expectError(error.NameTooLong, net.UnixAddress.init(&path));
    try std.testing.expectError(error.NameTooLong, net.UnixAddress.init(path[0..capacity]));
    try roundTrip(path[0 .. capacity - 1]);
    if (builtin.os.tag == .linux) {
        // Abstract addresses are byte strings; both embedded and trailing NUL
        // bytes are part of the name, and no extra terminator may be appended.
        path[0] = 0;
        path[3] = 0;
        path[capacity - 1] = 0;
        try roundTrip(path[0..capacity]);
        path[8] = 0;
        try roundTrip(path[0..9]);
    }
}

// run
// backend=llvm
// target=aarch64-macos,x86_64-linux,aarch64-linux
// link_libc=true
