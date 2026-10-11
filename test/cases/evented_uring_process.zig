const std = @import("std");
const Io = std.Io;
const expect = std.testing.expect;
const expectError = std.testing.expectError;

fn testCanceledWait(io: Io, bin: Io.Dir, before_registration: bool) !void {
    var child = try std.process.spawn(io, .{
        .exe = .{ .explicit = .{ .dir = bin, .path = "sh" } },
        .argv = &.{ "separate-argv-zero", "-c", "exec sleep 5" },
        .stdin = .pipe,
        .stdout = .pipe,
        .stderr = .pipe,
    });
    defer child.kill(io);
    const id = child.id.?;
    const stdin = child.stdin.?.handle;
    const stdout = child.stdout.?.handle;
    const stderr = child.stderr.?.handle;
    var waiting = Io.async(io, std.process.Child.wait, .{ &child, io });
    defer _ = waiting.cancel(io) catch {};
    if (!before_registration) try io.sleep(.fromMilliseconds(10), .awake);
    const start = Io.Timestamp.now(io, .awake);
    try expectError(error.Canceled, waiting.cancel(io));
    try expect(start.durationTo(.now(io, .awake)).toMilliseconds() < 2000);
    try expect(child.id.? == id);
    try expect(child.stdin.?.handle == stdin);
    try expect(child.stdout.?.handle == stdout);
    try expect(child.stderr.?.handle == stderr);
    for ([_]std.c.fd_t{ stdin, stdout, stderr }) |fd| {
        try expect(std.c.fcntl(fd, std.posix.F.GETFD) >= 0);
    }
    const kill_start = Io.Timestamp.now(io, .awake);
    child.kill(io);
    try expect(kill_start.durationTo(.now(io, .awake)).toMilliseconds() < 2000);
    try expect(child.id == null and child.stdin == null and child.stdout == null and child.stderr == null);
}

fn testImmediateExit(io: Io, bin: Io.Dir) !void {
    for (0..32) |round| {
        var child = try std.process.spawn(io, .{
            .exe = .{ .explicit = .{ .dir = bin, .path = "sh" } },
            .argv = &.{ "separate-argv-zero", "-c", "exit 7" },
            .stdout = .pipe,
            .request_resource_usage_statistics = round % 2 == 0,
        });
        defer child.kill(io);
        try expect((try child.wait(io)).exited == 7);
        try expect(child.id == null and child.stdout == null);
    }
}

fn testForcedTermination(io: Io, bin: Io.Dir) !void {
    var child = try std.process.spawn(io, .{
        .exe = .{ .explicit = .{ .dir = bin, .path = "sh" } },
        .argv = &.{ "separate-argv-zero", "-c", "trap '' TERM; printf ready; exec sleep 5" },
        .stdout = .pipe,
    });
    defer child.kill(io);
    var ready: [5]u8 = undefined;
    var count: usize = 0;
    while (count != ready.len) {
        const n = try child.stdout.?.readStreaming(io, &.{ready[count..]});
        try expect(n != 0);
        count += n;
    }
    try std.testing.expectEqualStrings("ready", &ready);
    const start = Io.Timestamp.now(io, .awake);
    child.kill(io);
    try expect(start.durationTo(.now(io, .awake)).toMilliseconds() < 2000);
    try expect(child.id == null and child.stdout == null);
}

pub fn main() !void {
    for ([_]usize{ 0, 3 }) |thread_limit| {
        var allocator: std.heap.DebugAllocator(.{}) = .init;
        defer std.debug.assert(allocator.deinit() == .ok);
        var evented: Io.Uring = undefined;
        try evented.init(allocator.allocator(), .{ .thread_limit = thread_limit });
        defer evented.deinit();
        const io = evented.io();
        const bin = try Io.Dir.cwd().openDir(io, "/bin", .{});
        defer bin.close(io);
        for (0..8) |_| {
            try testCanceledWait(io, bin, true);
            try testCanceledWait(io, bin, false);
        }
        try testImmediateExit(io, bin);
        try testForcedTermination(io, bin);
    }
}

// run
// target=aarch64-linux,x86_64-linux
// backend=llvm
// link_libc=true
