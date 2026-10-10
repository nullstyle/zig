const std = @import("std");
const Io = std.Io;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;

// Count allocations without eagerly retaining the 60 MiB fiber stacks in
// a debug allocator's metadata. The backend must release every allocation
// before its owner tears this state down.
const TrackingAllocator = struct {
    live: std.atomic.Value(usize) = .init(0),

    fn allocator(self: *TrackingAllocator) std.mem.Allocator {
        return .{ .ptr = self, .vtable = &.{ .alloc = alloc, .resize = resize, .remap = remap, .free = free } };
    }

    fn alloc(ptr: *anyopaque, len: usize, alignment: std.mem.Alignment, ret_addr: usize) ?[*]u8 {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ptr));
        const result = std.heap.page_allocator.rawAlloc(len, alignment, ret_addr) orelse return null;
        _ = self.live.fetchAdd(1, .monotonic);
        return result;
    }

    fn resize(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret_addr: usize) bool {
        return std.heap.page_allocator.rawResize(memory, alignment, len, ret_addr);
    }

    fn remap(_: *anyopaque, memory: []u8, alignment: std.mem.Alignment, len: usize, ret_addr: usize) ?[*]u8 {
        return std.heap.page_allocator.rawRemap(memory, alignment, len, ret_addr);
    }

    fn free(ptr: *anyopaque, memory: []u8, alignment: std.mem.Alignment, ret_addr: usize) void {
        const self: *TrackingAllocator = @ptrCast(@alignCast(ptr));
        std.heap.page_allocator.rawFree(memory, alignment, ret_addr);
        std.debug.assert(self.live.fetchSub(1, .release) != 0);
    }
};

fn value(n: u32) u32 {
    return n;
}

fn immediateCompletion(io: Io) !void {
    // The result can become visible before its caller reaches await. The
    // finished fiber must already be parked when its stack is recycled.
    for (0..20_000) |i| {
        const n: u32 = @intCast(i);
        var future = try Io.concurrent(io, value, .{n});
        try expectEqual(n, future.await(io));
    }
}

fn nestedRounds(io: Io, worker: u32) anyerror!void {
    for (0..1000) |round| {
        var futures: [8]Io.Future(u32) = undefined;
        for (&futures, 0..) |*future, i| {
            const n = worker * 100_000 + @as(u32, @intCast(round * futures.len + i));
            future.* = try Io.concurrent(io, value, .{n});
        }
        for (&futures, 0..) |*future, i| {
            const n = worker * 100_000 + @as(u32, @intCast(round * futures.len + i));
            try expectEqual(n, future.await(io));
        }
    }
}

fn concurrentReuse(io: Io) !void {
    var parents: [4]Io.Future(anyerror!void) = undefined;
    for (&parents, 0..) |*parent, i|
        parent.* = try Io.concurrent(io, nestedRounds, .{ io, @as(u32, @intCast(i)) });
    for (&parents) |*parent| try parent.await(io);
}

fn groupMember(done: *std.atomic.Value(usize)) void {
    _ = done.fetchAdd(1, .release);
}

fn groupRounds(io: Io, rounds: usize) !void {
    var done: std.atomic.Value(usize) = .init(0);
    var group: Io.Group = .init;
    defer group.cancel(io);
    for (0..rounds) |round| {
        for (0..8) |_| try group.concurrent(io, groupMember, .{&done});
        try group.await(io);
        try expectEqual((round + 1) * 8, done.load(.acquire));
    }
}

fn run(thread_limit: usize, stress: bool) !void {
    const original_thread = std.Thread.getCurrentId();
    var tracking: TrackingAllocator = .{};
    var evented: Io.Uring = undefined;
    try evented.init(tracking.allocator(), .{ .thread_limit = thread_limit });
    var deinitialized = false;
    defer if (!deinitialized) evented.deinit();
    const io = evented.io();
    if (stress) {
        try immediateCompletion(io);
        try concurrentReuse(io);
    }
    try groupRounds(io, if (stress) 1000 else 32);
    // Group completion and the subsequent deinit race the final member's
    // return. Also cover a main fiber migrated by a worker's future wake.
    evented.deinit();
    deinitialized = true;
    try expectEqual(original_thread, std.Thread.getCurrentId());
    try expect(tracking.live.load(.acquire) == 0);
}

pub fn main() !void {
    try run(0, true);
    try run(3, true);
    for (0..8) |_| try run(3, false);
}

// run
// target=aarch64-linux,x86_64-linux
// backend=llvm
// link_libc=true
