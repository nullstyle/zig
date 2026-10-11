const std = @import("std");
const Io = std.Io;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;

fn requireCanceled(result: anyerror!void, missing: anyerror) !void {
    result catch |err| {
        if (err == error.Canceled) return;
        return err;
    };
    return missing;
}

fn waitRegistered(io: Io, group: *Io.Group) !void {
    while (@atomicLoad(usize, &group.state, .acquire) >> 2 == 0)
        try io.sleep(.fromMicroseconds(10), .awake);
}

fn gatedMember(io: Io, gate: *Io.Event, done: *std.atomic.Value(usize)) Io.Cancelable!void {
    defer _ = done.fetchAdd(1, .release);
    try gate.wait(io);
}

fn releaseRegistered(io: Io, group: *Io.Group, gate: *Io.Event) !void {
    try waitRegistered(io, group);
    gate.set(io);
}

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
        try expectEqual(@as(usize, 0), group.state);
        try group.await(io);
        group.cancel(io);
    }
}

fn groupAlreadyFinished(io: Io) !void {
    var done: std.atomic.Value(usize) = .init(0);
    var group: Io.Group = .init;
    defer group.cancel(io);
    for (0..32) |_| {
        try group.concurrent(io, groupMember, .{&done});
        // Force a context switch after the member can complete. The member
        // must retain the group's resources until await releases them.
        var barrier = try Io.concurrent(io, value, .{@as(u32, 7)});
        try expectEqual(@as(u32, 7), barrier.await(io));
        try group.await(io);
        try expectEqual(@as(usize, 0), group.state);
    }
    try expectEqual(@as(usize, 32), done.load(.acquire));
}

fn cancellableMember(io: Io, done: *std.atomic.Value(usize)) Io.Cancelable!void {
    defer _ = done.fetchAdd(1, .release);
    try Io.Timeout.sleep(.none, io);
}

fn canceledGroupOwner(io: Io, protected: bool) anyerror!void {
    // This cancellation precedes group registration. Re-arm it to exercise
    // both protected completion and propagation through Group.await.
    try requireCanceled(Io.Timeout.sleep(.none, io), error.PreCancelSleepReturnedNormally);
    io.recancel();
    const previous = io.swapCancelProtection(if (protected) .blocked else .unblocked);
    defer _ = io.swapCancelProtection(previous);
    var done: std.atomic.Value(usize) = .init(0);
    var group: Io.Group = .init;
    defer group.cancel(io);
    if (protected) {
        var gate: Io.Event = .unset;
        try group.concurrent(io, gatedMember, .{ io, &gate, &done });
        var releaser = try Io.concurrent(io, releaseRegistered, .{ io, &group, &gate });
        try group.await(io);
        try releaser.await(io);
        _ = io.swapCancelProtection(.unblocked);
        try requireCanceled(io.checkCancel(), error.ProtectedGroupDidNotRetainCancellation);
    } else {
        try group.concurrent(io, cancellableMember, .{ io, &done });
        try requireCanceled(group.await(io), error.PreCanceledGroupReturnedNormally);
    }
    try expectEqual(@as(usize, 1), done.load(.acquire));
    try expectEqual(@as(usize, 0), group.state);
    try expectEqual(null, group.token.load(.acquire));
    // The canceled owner can reuse the same group after acknowledging it.
    try group.concurrent(io, groupMember, .{&done});
    try group.await(io);
    try expectEqual(@as(usize, 2), done.load(.acquire));
    try expectEqual(@as(usize, 0), group.state);
}

fn cancellationBeforeGroup(io: Io) !void {
    for (0..32) |_| {
        for ([_]bool{ false, true }) |protected| {
            var owner = try Io.concurrent(io, canceledGroupOwner, .{ io, protected });
            try owner.cancel(io);
        }
    }
}

const OwnerState = struct {
    ready: std.atomic.Value(u32) = .init(0),
    done: std.atomic.Value(usize) = .init(0),
};

fn waitingGroupOwner(io: Io, state: *OwnerState) anyerror!void {
    var group: Io.Group = .init;
    defer group.cancel(io);
    for (0..4) |_| try group.concurrent(io, cancellableMember, .{ io, &state.done });
    state.ready.store(1, .release);
    io.futexWake(u32, &state.ready.raw, std.math.maxInt(u32));
    const result = group.await(io);
    try expectEqual(@as(usize, 4), state.done.load(.acquire));
    try expectEqual(null, group.token.load(.acquire));
    try expectEqual(@as(usize, 0), group.state);
    return result;
}

fn cancellationDuringGroup(io: Io) !void {
    for (0..64) |_| {
        var state: OwnerState = .{};
        var owner = try Io.concurrent(io, waitingGroupOwner, .{ io, &state });
        while (state.ready.load(.acquire) == 0)
            try io.futexWait(u32, &state.ready.raw, 0);
        try std.testing.expectError(error.Canceled, owner.cancel(io));
    }
}

fn cancelGroupReuse(io: Io) !void {
    var done: std.atomic.Value(usize) = .init(0);
    var group: Io.Group = .init;
    defer group.cancel(io);
    for (0..32) |round| {
        for (0..4) |_| try group.concurrent(io, cancellableMember, .{ io, &done });
        group.cancel(io);
        group.cancel(io);
        try group.await(io);
        try expectEqual((round + 1) * 4, done.load(.acquire));
        try expectEqual(@as(usize, 0), group.state);
    }
}

fn heldMember(io: Io, gate: *std.atomic.Value(u32), done: *std.atomic.Value(usize)) Io.Cancelable!void {
    while (gate.load(.acquire) == 0) try io.futexWait(u32, &gate.raw, 0);
    groupMember(done);
}

fn addGroupMembers(io: Io, group: *Io.Group, gate: *std.atomic.Value(u32), done: *std.atomic.Value(usize)) anyerror!void {
    for (0..8) |_| try group.concurrent(io, groupMember, .{done});
    // The held member prevents completion until every concurrent add returns,
    // as required by Io.Group's concurrent-add lifetime contract.
    gate.store(1, .release);
    io.futexWake(u32, &gate.raw, std.math.maxInt(u32));
}

fn addWhileAwaited(io: Io) !void {
    var group: Io.Group = .init;
    defer group.cancel(io);
    for (0..64) |_| {
        var gate: std.atomic.Value(u32) = .init(0);
        var done: std.atomic.Value(usize) = .init(0);
        try group.concurrent(io, heldMember, .{ io, &gate, &done });
        var producer = try Io.concurrent(io, addGroupMembers, .{ io, &group, &gate, &done });
        try group.await(io);
        try producer.await(io);
        try expectEqual(@as(usize, 9), done.load(.acquire));
        try expectEqual(@as(usize, 0), group.state);
    }
}

fn longSleep(io: Io, ready: *Io.Event) Io.Cancelable!void {
    ready.set(io);
    try io.sleep(.fromMilliseconds(5000), .awake);
}

fn submittedSleepCancel(io: Io) !void {
    try io.sleep(.fromMilliseconds(1), .awake);
    for (0..16) |_| {
        var ready: Io.Event = .unset;
        var sleeper = try Io.concurrent(io, longSleep, .{ io, &ready });
        try ready.wait(io);
        // In the single-thread run this context switch guarantees the sleeper
        // has submitted its timeout, rather than taking the pre-request path.
        try io.sleep(.fromMilliseconds(1), .awake);
        const MaybeFiber = @FieldType(@typeInfo(@FieldType(@FieldType(Io.Uring, "threads"), "allocated")).pointer.child, "free_queue");
        const fiber: @typeInfo(MaybeFiber).optional.child = @ptrCast(@alignCast(sleeper.any_future.?));
        try expect(@atomicLoad(@TypeOf(fiber.cancel_status), &fiber.cancel_status, .acquire).awaiting != .nothing);
        try requireCanceled(sleeper.cancel(io), error.SubmittedSleepDidNotCancel);
    }
}

fn protectedMidOwner(io: Io, group: *Io.Group) anyerror!void {
    const old = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(old);
    try group.await(io);
    _ = io.swapCancelProtection(.unblocked);
    try io.checkCancel();
    return error.ProtectedGroupLostCancellation;
}

fn directCancelOwner(io: Io, group: *Io.Group) anyerror!void {
    group.cancel(io);
    try io.checkCancel();
    return error.DirectGroupCancelLostCancellation;
}

fn protectedMember(io: Io, ready: *Io.Event, gate: *Io.Event, done: *std.atomic.Value(usize)) Io.Cancelable!void {
    const old = io.swapCancelProtection(.blocked);
    defer _ = io.swapCancelProtection(old);
    ready.set(io);
    try gatedMember(io, gate, done);
}

fn releaseRequested(io: Io, group: *Io.Group, owner: *Io.Future(anyerror!void), gate: *Io.Event, done: *std.atomic.Value(usize)) !void {
    const MaybeFiber = @FieldType(@typeInfo(@FieldType(@FieldType(Io.Uring, "threads"), "allocated")).pointer.child, "free_queue");
    const fiber: @typeInfo(MaybeFiber).optional.child = @ptrCast(@alignCast(owner.any_future.?));
    try waitRegistered(io, group);
    while (!@atomicLoad(@TypeOf(fiber.cancel_status), &fiber.cancel_status, .acquire).requested)
        try io.sleep(.fromMicroseconds(10), .awake);
    try expect(@atomicLoad(usize, &group.state, .acquire) >> 2 != 0);
    try expectEqual(@as(usize, 0), done.load(.acquire));
    // The owner fiber can be released once the gate opens: do not access it
    // after this point.
    gate.set(io);
}

fn protectedAndDirectCancel(io: Io) !void {
    for (0..32) |_| {
        for (0..2) |mode| {
            var group: Io.Group = .init;
            defer group.cancel(io);
            var gate: Io.Event = .unset;
            var ready: Io.Event = .unset;
            var done: std.atomic.Value(usize) = .init(0);
            if (mode == 0) {
                try group.concurrent(io, gatedMember, .{ io, &gate, &done });
            } else {
                try group.concurrent(io, protectedMember, .{ io, &ready, &gate, &done });
                try ready.wait(io);
            }
            var owner = if (mode == 0)
                try Io.concurrent(io, protectedMidOwner, .{ io, &group })
            else
                try Io.concurrent(io, directCancelOwner, .{ io, &group });
            try waitRegistered(io, &group);
            var releaser = try Io.concurrent(io, releaseRequested, .{ io, &group, &owner, &gate, &done });
            try requireCanceled(owner.cancel(io), error.RegisteredGroupOwnerDidNotCancel);
            try releaser.await(io);
            try expectEqual(@as(usize, 1), done.load(.acquire));
            try expectEqual(null, group.token.load(.acquire));
            try expectEqual(@as(usize, 0), group.state);
        }
    }
}

fn nestedGroupRounds(io: Io) anyerror!void {
    try groupRounds(io, 250);
    try groupAlreadyFinished(io);
    try cancellationBeforeGroup(io);
    try cancellationDuringGroup(io);
    try cancelGroupReuse(io);
    try addWhileAwaited(io);
    try protectedAndDirectCancel(io);
}

fn concurrentGroups(io: Io) !void {
    var owners: [4]Io.Future(anyerror!void) = undefined;
    for (&owners) |*owner| owner.* = try Io.concurrent(io, nestedGroupRounds, .{io});
    for (&owners) |*owner| try owner.await(io);
}

fn run(thread_limit: usize, stress: bool) !void {
    const original_thread = std.Thread.getCurrentId();
    var tracking: TrackingAllocator = .{};
    var evented: Io.Uring = undefined;
    try evented.init(tracking.allocator(), .{ .thread_limit = thread_limit });
    var deinitialized = false;
    defer if (!deinitialized) evented.deinit();
    const io = evented.io();
    try submittedSleepCancel(io);
    if (stress) {
        try immediateCompletion(io);
        try concurrentReuse(io);
    }
    try groupRounds(io, if (stress) 1000 else 32);
    try groupAlreadyFinished(io);
    try cancellationBeforeGroup(io);
    try cancellationDuringGroup(io);
    try cancelGroupReuse(io);
    try addWhileAwaited(io);
    try protectedAndDirectCancel(io);
    if (stress) try concurrentGroups(io);
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
