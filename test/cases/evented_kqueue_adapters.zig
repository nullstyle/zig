const std = @import("std");
const Io = std.Io;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectEqualStrings = std.testing.expectEqualStrings;
const expectError = std.testing.expectError;

fn testFiles(io: Io, dir: Io.Dir) !void {
    io.recancel();
    try expectError(error.Canceled, dir.stat(io));
    _ = try dir.stat(io);
    const initial = try dir.createFile(io, "payload", .{ .read = true });
    defer initial.close(io);
    try initial.writePositionalAll(io, "initial", 0);
    try expectEqual(7, (try initial.stat(io)).size);
    try expectEqual(7, try initial.length(io));
    var buffer: [32]u8 = undefined;
    const n = try initial.readPositionalAll(io, &buffer, 0);
    try expectEqualStrings("initial", buffer[0..n]);

    const streaming = try dir.createFile(io, "streaming", .{ .read = true });
    defer streaming.close(io);
    try expectEqual(6, try streaming.writeStreaming(io, "H", &.{ "ab", "z" }, 3));
    const stream_n = try streaming.readPositionalAll(io, &buffer, 0);
    try expectEqualStrings("Habzzz", buffer[0..stream_n]);

    // Full atomic lifecycle: create a nested path, materialize without
    // replacement, reject a duplicate, replace, and clean an abandoned temp.
    {
        var atomic = try dir.createFileAtomic(io, "nested/value", .{ .make_path = true });
        defer atomic.deinit(io);
        try atomic.file.writePositionalAll(io, "first", 0);
        try atomic.link(io);
    }
    try expectEqualStrings("first", try dir.readFile(io, "nested/value", &buffer));
    {
        var duplicate = try dir.createFileAtomic(io, "nested/value", .{});
        defer duplicate.deinit(io);
        try duplicate.file.writeStreamingAll(io, "duplicate");
        try expectError(error.PathAlreadyExists, duplicate.link(io));
    }
    try expectEqualStrings("first", try dir.readFile(io, "nested/value", &buffer));
    {
        var atomic = try dir.createFileAtomic(io, "nested/value", .{ .replace = true });
        defer atomic.deinit(io);
        try atomic.file.writeStreamingAll(io, "replacement");
        try atomic.replace(io);
    }
    {
        var abandoned = try dir.createFileAtomic(io, "nested/value", .{ .replace = true });
        abandoned.deinit(io);
    }
    try expectEqualStrings("replacement", try dir.readFile(io, "nested/value", &buffer));
    const nested = try dir.openDir(io, "nested", .{ .iterate = true });
    defer nested.close(io);
    _ = try nested.stat(io);
    try expectEqual(11, (try nested.statFile(io, "value", .{})).size);
    var iterator = nested.iterate();
    const entry = (try iterator.next(io)) orelse return error.TestUnexpectedResult;
    try expectEqualStrings("value", entry.name);
    try expect(try iterator.next(io) == null);
}

fn expectExit(child: *std.process.Child, io: Io, expected: u8) !void {
    const term = try child.wait(io);
    try expectEqual(std.process.Child.Term{ .exited = expected }, term);
    try expect(child.id == null);
    child.kill(io); // Cleanup is idempotent after wait.
}

fn testProcesses(io: Io, dir: Io.Dir) !void {
    var child = try std.process.spawn(io, .{ .argv = &.{ "/bin/sh", "-c", "exit 17" } });
    defer child.kill(io);
    try expectExit(&child, io, 17);
    try expectError(error.FileNotFound, std.process.spawn(io, .{
        .argv = &.{"/this-path-does-not-exist/zig-kqueue-regression"},
    }));

    // The stateful null-file cache, explicit environment, and pipe cleanup
    // all use the fallback's state rather than interpreting Kqueue memory.
    var environ: std.process.Environ.Map = .init(std.heap.page_allocator);
    defer environ.deinit();
    try environ.put("KQUEUE_ADAPTER_VALUE", "ok");
    var piped = try std.process.spawn(io, .{
        .argv = &.{ "/bin/sh", "-c", "test \"$KQUEUE_ADAPTER_VALUE\" = ok && printf result" },
        .environ_map = &environ,
        .stdin = .ignore,
        .stdout = .pipe,
        .stderr = .ignore,
    });
    defer piped.kill(io);
    var output: [6]u8 = undefined;
    var read: usize = 0;
    while (read != output.len) {
        const n = try piped.stdout.?.readStreaming(io, &.{output[read..]});
        try expect(n != 0);
        read += n;
    }
    try expectEqualStrings("result", &output);
    try expectExit(&piped, io, 0);
    try expect(piped.stdout == null);

    var cwd_before: [std.fs.max_path_bytes]u8 = undefined;
    const before = try std.process.currentPath(io, &cwd_before);
    try dir.symLink(io, "/bin/sh", "adapter-shell", .{});
    var relative = try std.process.spawnPath(io, dir, .{
        .argv = &.{ "adapter-shell", "-c", "test -f payload" },
    });
    defer relative.kill(io);
    try expectExit(&relative, io, 0);
    var cwd_after: [std.fs.max_path_bytes]u8 = undefined;
    const after = try std.process.currentPath(io, &cwd_after);
    try expectEqualStrings(cwd_before[0..before], cwd_after[0..after]);
    // A basename must resolve against dir, even if PATH contains that name.
    try expectError(error.FileNotFound, std.process.spawnPath(io, dir, .{ .argv = &.{"sh"} }));

    var sleeper = try std.process.spawn(io, .{ .argv = &.{ "/bin/sleep", "30" } });
    sleeper.kill(io);
    try expect(sleeper.id == null);
    sleeper.kill(io);

    // Test replacement in a forked child so the case can verify its exit.
    const pid = std.c.fork();
    try expect(pid >= 0);
    if (pid == 0) {
        const err = std.process.replacePath(io, dir, .{
            .argv = &.{ "adapter-shell", "-c", "test -f payload && exit 23" },
        });
        std.c._exit(if (err == error.FileNotFound) 98 else 99);
    }
    var replaced: std.process.Child = .{
        .id = pid,
        .thread_handle = {},
        .stdin = null,
        .stdout = null,
        .stderr = null,
        .request_resource_usage_statistics = false,
    };
    defer replaced.kill(io);
    try expectExit(&replaced, io, 23);
}

fn lookupWithBackpressure(io: Io, queue: *Io.Queue(Io.net.HostName.LookupResult), canonical: *[Io.net.HostName.max_len]u8) !void {
    try (Io.net.HostName{ .bytes = "localhost" }).lookup(io, queue, .{
        .port = 8123,
        .family = .ip4,
        .canonical_name_buffer = canonical,
    });
}

fn testResolver(io: Io) !void {
    const loopback = try Io.net.Interface.Name.fromSlice("lo0");
    const interface = try loopback.resolve(io);
    const name = try interface.name(io);
    try expectEqualStrings("lo0", name.toSlice());
    const missing = try Io.net.Interface.Name.fromSlice("no-such-if-xyz");
    try expectError(error.InterfaceNotFound, missing.resolve(io));
    try expectError(error.InterfaceNotFound, (Io.net.Interface{ .index = std.math.maxInt(u32) }).name(io));

    for ([_][]const u8{ "127.0.0.1", "localhost" }) |host| {
        var results: [16]Io.net.HostName.LookupResult = undefined;
        var queue: Io.Queue(Io.net.HostName.LookupResult) = .init(&results);
        var canonical: [Io.net.HostName.max_len]u8 = undefined;
        try (Io.net.HostName{ .bytes = host }).lookup(io, &queue, .{
            .port = 8123,
            .family = .ip4,
            .canonical_name_buffer = &canonical,
        });
        var count: usize = 0;
        while (true) switch (queue.getOne(io) catch |err| switch (err) {
            error.Closed => break,
            else => return err,
        }) {
            .address => |address| {
                try expectEqual(Io.net.IpAddress{ .ip4 = .loopback(8123) }, address);
                count += 1;
            },
            .canonical_name => |canon| try expect(canon.bytes.len != 0),
        };
        try expect(count != 0 and count <= 15);
    }
    {
        // One worker and one queue slot exercise both consumer wakeup and
        // producer backpressure while the resolver emits its canonical name.
        var results: [1]Io.net.HostName.LookupResult = undefined;
        var queue: Io.Queue(Io.net.HostName.LookupResult) = .init(&results);
        var canonical: [Io.net.HostName.max_len]u8 = undefined;
        var lookup = Io.async(io, lookupWithBackpressure, .{ io, &queue, &canonical });
        defer lookup.cancel(io) catch {};
        var addresses: usize = 0;
        var names: usize = 0;
        while (true) switch (queue.getOne(io) catch |err| switch (err) {
            error.Closed => break,
            else => return err,
        }) {
            .address => addresses += 1,
            .canonical_name => names += 1,
        };
        try lookup.await(io);
        try expect(addresses != 0);
        try expectEqual(1, names);
    }
    var results: [16]Io.net.HostName.LookupResult = undefined;
    var queue: Io.Queue(Io.net.HostName.LookupResult) = .init(&results);
    io.recancel();
    try expectError(error.Canceled, (Io.net.HostName{ .bytes = "localhost" }).lookup(io, &queue, .{ .port = 0 }));
    try expectError(error.Closed, queue.getOne(io));
}

pub fn main() !void {
    var kqueue: Io.Kqueue = undefined;
    try Io.Kqueue.init(&kqueue, std.heap.page_allocator, .{ .n_threads = 1 });
    defer kqueue.deinit();
    const io = kqueue.io();
    var random: u64 = undefined;
    io.random(std.mem.asBytes(&random));
    var path_buffer: [96]u8 = undefined;
    const path = try std.fmt.bufPrint(&path_buffer, "/tmp/zig-kqueue-adapters-{x}", .{random});
    const dir = try Io.Dir.cwd().createDirPathOpen(io, path, .{ .open_options = .{ .iterate = true } });
    defer Io.Dir.cwd().deleteTree(io, path) catch {};
    defer dir.close(io);
    try testFiles(io, dir);
    try testProcesses(io, dir);
    try testResolver(io);
}

// run
// backend=llvm
// target=aarch64-macos,x86_64-macos,aarch64-freebsd,x86_64-freebsd,aarch64-openbsd,x86_64-openbsd
// link_libc=true
