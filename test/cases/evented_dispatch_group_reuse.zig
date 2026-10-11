const std = @import("std");
const Io = std.Io;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;

fn count(counter: *std.atomic.Value(usize)) Io.Cancelable!void {
    _ = counter.fetchAdd(1, .release);
}

fn gated(io: Io, gate: *Io.Event, counter: *std.atomic.Value(usize)) Io.Cancelable!void {
    defer _ = counter.fetchAdd(1, .release);
    try gate.wait(io);
}

fn gatedCompletion(io: Io, gate: *Io.Event, counter: *std.atomic.Value(usize)) Io.Cancelable!void {
    try gate.wait(io);
    _ = counter.fetchAdd(1, .release);
}

// This is a backend regression: observe its actual registered awaiter rather
// than infer registration from a sleep or from the member having started.
fn waitRegistered(io: Io, group: *Io.Group) Io.Cancelable!void {
    while (@atomicLoad(usize, &group.state, .acquire) >> 2 == 0)
        try Io.sleep(io, .fromMicroseconds(10), .awake);
}

fn releaseRegistered(io: Io, group: *Io.Group, gate: *Io.Event) anyerror!void {
    try waitRegistered(io, group);
    gate.set(io);
}

fn expectReleased(group: *Io.Group) !void {
    try expect(group.token.load(.acquire) == null);
    try expectEqual(@as(usize, 0), @atomicLoad(usize, &group.state, .acquire));
}

fn repeatedAwait(io: Io) !void {
    var group: Io.Group = .init;
    defer group.cancel(io);
    var completed: std.atomic.Value(usize) = .init(0);
    for (0..1000) |round| {
        var gate: Io.Event = .unset;
        try group.concurrent(io, gated, .{ io, &gate, &completed });
        var releaser = try Io.concurrent(io, releaseRegistered, .{ io, &group, &gate });
        try group.await(io);
        try releaser.await(io);
        try expectReleased(&group);
        try expectEqual(round + 1, completed.load(.acquire));
    }
}

fn completionBeforeAwait(io: Io) !void {
    var completed: std.atomic.Value(usize) = .init(0);
    for (0..100) |round| {
        const group = try std.heap.page_allocator.create(Io.Group);
        group.* = .init;
        try group.concurrent(io, count, .{&completed});
        // all_ones identifies completed resources which still need owner
        // cleanup. Null here would allow freeing storage before last unlock.
        while (true) {
            const token = group.token.load(.acquire) orelse return error.ResourcesReleasedBeforeAwait;
            if (@intFromPtr(token) >> 2 == std.math.maxInt(usize) >> 2) break;
            try Io.sleep(io, .fromMicroseconds(10), .awake);
        }
        try group.await(io);
        try expectReleased(group);
        try expectEqual(round + 1, completed.load(.acquire));
        std.heap.page_allocator.destroy(group);
    }
}

fn cancelAndReuse(io: Io) !void {
    var group: Io.Group = .init;
    defer group.cancel(io);
    var completed: std.atomic.Value(usize) = .init(0);
    for (0..200) |round| {
        var gate: Io.Event = .unset;
        try group.concurrent(io, gated, .{ io, &gate, &completed });
        group.cancel(io);
        try expectReleased(&group);
        try group.concurrent(io, count, .{&completed});
        try group.await(io);
        try expectReleased(&group);
        try expectEqual((round + 1) * 2, completed.load(.acquire));
    }
}

fn awaitGroup(io: Io, group: *Io.Group) Io.Cancelable!void {
    try group.await(io);
}

fn ownerCancellation(io: Io) !void {
    var group: Io.Group = .init;
    defer group.cancel(io);
    var completed: std.atomic.Value(usize) = .init(0);
    for (0..200) |round| {
        var gate: Io.Event = .unset;
        try group.concurrent(io, gated, .{ io, &gate, &completed });
        var owner = try Io.concurrent(io, awaitGroup, .{ io, &group });
        try waitRegistered(io, &group);
        try std.testing.expectError(error.Canceled, owner.cancel(io));
        try expectReleased(&group);
        try group.concurrent(io, count, .{&completed});
        try group.await(io);
        try expectReleased(&group);
        try expectEqual((round + 1) * 2, completed.load(.acquire));
    }
}

fn completionCancellationRace(io: Io) !void {
    var group: Io.Group = .init;
    defer group.cancel(io);
    var completed: std.atomic.Value(usize) = .init(0);
    for (0..500) |round| {
        var gate: Io.Event = .unset;
        try group.concurrent(io, gated, .{ io, &gate, &completed });
        var owner = try Io.concurrent(io, awaitGroup, .{ io, &group });
        try waitRegistered(io, &group);
        gate.set(io);
        owner.cancel(io) catch |err| switch (err) {
            error.Canceled => {},
        };
        try expectReleased(&group);
        try expectEqual(round + 1, completed.load(.acquire));
    }
}

fn protectedOwner(io: Io, group: *Io.Group, ready: *Io.Event) anyerror!void {
    var enter: Io.Event = .unset;
    ready.set(io);
    enter.wait(io) catch |err| switch (err) {
        error.Canceled => {
            // Re-arm a known request before registering the protected await.
            // No cancellation callback can own this future group registration.
            Io.recancel(io);
            const old = Io.swapCancelProtection(io, .blocked);
            defer _ = Io.swapCancelProtection(io, old);
            try group.await(io);
            _ = Io.swapCancelProtection(io, .unblocked);
            try Io.checkCancel(io);
            return error.MissingCancellation;
        },
    };
    return error.UnexpectedWake;
}

fn protectedPreCanceled(io: Io) !void {
    var completed: std.atomic.Value(usize) = .init(0);
    for (0..32) |_| {
        var group: Io.Group = .init;
        defer group.cancel(io);
        var gate: Io.Event = .unset;
        try group.concurrent(io, gatedCompletion, .{ io, &gate, &completed });
        var ready: Io.Event = .unset;
        var owner = try Io.concurrent(io, protectedOwner, .{ io, &group, &ready });
        try ready.wait(io);
        var releaser = try Io.concurrent(io, releaseRegistered, .{ io, &group, &gate });
        try std.testing.expectError(error.Canceled, owner.cancel(io));
        try releaser.await(io);
        try expectReleased(&group);
    }
    try expectEqual(@as(usize, 32), completed.load(.acquire));
}

fn protectedMidOwner(io: Io, group: *Io.Group) anyerror!void {
    const old = Io.swapCancelProtection(io, .blocked);
    defer _ = Io.swapCancelProtection(io, old);
    try group.await(io);
    _ = Io.swapCancelProtection(io, .unblocked);
    try Io.checkCancel(io);
    return error.MissingCancellation;
}

fn directCancelOwner(io: Io, group: *Io.Group) anyerror!void {
    group.cancel(io);
    try Io.checkCancel(io);
    return error.MissingCancellation;
}

fn protectedMember(io: Io, ready: *Io.Event, gate: *Io.Event, counter: *std.atomic.Value(usize)) Io.Cancelable!void {
    const old = Io.swapCancelProtection(io, .blocked);
    defer _ = Io.swapCancelProtection(io, old);
    ready.set(io);
    try gate.wait(io);
    _ = counter.fetchAdd(1, .release);
}

fn releaseRequested(
    io: Io,
    group: *Io.Group,
    owner: *Io.Future(anyerror!void),
    gate: *Io.Event,
    completed: *std.atomic.Value(usize),
    before: usize,
) anyerror!void {
    // Use the backend's actual fiber type without duplicating its layout.
    // The owner cannot finish or be destroyed until this releases its member.
    const Fiber = @TypeOf(@as(Io.Evented, undefined).main_fiber);
    const fiber: *Fiber = @ptrCast(@alignCast(owner.any_future.?));
    try waitRegistered(io, group);
    while (!@atomicLoad(@TypeOf(fiber.cancel_status), &fiber.cancel_status, .acquire).requested)
        try Io.sleep(io, .fromMicroseconds(10), .awake);
    try expect(@atomicLoad(usize, &group.state, .acquire) >> 2 != 0);
    try expectEqual(before, completed.load(.acquire));
    gate.set(io);
}

fn protectedAndDirectCancel(io: Io) !void {
    var completed: std.atomic.Value(usize) = .init(0);
    var before: usize = 0;
    for (0..64) |_| {
        for (0..2) |mode| {
            var group: Io.Group = .init;
            defer group.cancel(io);
            var gate: Io.Event = .unset;
            var ready: Io.Event = .unset;
            if (mode == 0) {
                try group.concurrent(io, gatedCompletion, .{ io, &gate, &completed });
            } else {
                // Group.cancel requests this member's cancellation, but its
                // protected wait retains the member until the gate opens.
                try group.concurrent(io, protectedMember, .{ io, &ready, &gate, &completed });
                try ready.wait(io);
            }
            var owner = if (mode == 0)
                try Io.concurrent(io, protectedMidOwner, .{ io, &group })
            else
                try Io.concurrent(io, directCancelOwner, .{ io, &group });
            try waitRegistered(io, &group);
            var releaser = try Io.concurrent(io, releaseRequested, .{ io, &group, &owner, &gate, &completed, before });
            try std.testing.expectError(error.Canceled, owner.cancel(io));
            try releaser.await(io);
            try expectReleased(&group);
            before += 1;
            try expectEqual(before, completed.load(.acquire));
        }
    }
}

pub fn main() !void {
    var evented: Io.Evented = undefined;
    try evented.init(std.heap.page_allocator, .{});
    defer evented.deinit();
    const io = evented.io();
    try repeatedAwait(io);
    try completionBeforeAwait(io);
    try cancelAndReuse(io);
    try ownerCancellation(io);
    try completionCancellationRace(io);
    try protectedPreCanceled(io);
    try protectedAndDirectCancel(io);
}

// run
// backend=llvm
// target=aarch64-macos,x86_64-macos
// link_libc=true
