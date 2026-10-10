const std = @import("std");
const Io = std.Io;
const Probe = struct {
    clock_reads: usize = 0,
    waits: usize = 0,
    deadlines: [2]i96 = undefined,
    canceled: bool = false,

    fn now(userdata: ?*anyopaque, _: Io.Clock) Io.Timestamp {
        const self: *Probe = @ptrCast(@alignCast(userdata));
        self.clock_reads += 1;
        return .{ .nanoseconds = @intCast(self.clock_reads * std.time.ns_per_ms) };
    }
    fn wait(userdata: ?*anyopaque, batch: *Io.Batch, timeout: Io.Timeout) Io.Batch.AwaitConcurrentError!void {
        const self: *Probe = @ptrCast(@alignCast(userdata));
        // Backend relinquishes an interrupted operation without a result.
        self.deadlines[self.waits] = timeout.deadline.raw.nanoseconds;
        self.waits += 1;
        if (self.waits == 2) return error.Timeout;
        batch.* = .init(batch.storage);
    }
    fn cancel(userdata: ?*anyopaque, batch: *Io.Batch) void {
        const self: *Probe = @ptrCast(@alignCast(userdata));
        self.canceled = true;
        batch.submitted = .empty;
        batch.pending = .empty;
    }
};
pub fn main() !void {
    var probe: Probe = .{};
    var threaded: Io.Threaded = .init_single_threaded;
    var vtable = threaded.io().vtable.*;
    vtable.now = Probe.now;
    vtable.batchAwaitConcurrent = Probe.wait;
    vtable.batchCancel = Probe.cancel;
    const io: Io = .{ .userdata = &probe, .vtable = &vtable };
    var messages: [1]Io.net.IncomingMessage = .{.init};
    var data: [1]u8 = undefined;
    try std.testing.expectError(error.Timeout, io.operateTimeout(.{ .net_receive = .{
        .socket_handle = 0,
        .message_buffer = &messages,
        .data_buffer = &data,
        .flags = .{},
    } }, .{ .duration = .{ .raw = .fromMilliseconds(2), .clock = .awake } }));
    try std.testing.expect(probe.canceled);
    try std.testing.expectEqual(@as(usize, 2), probe.waits);
    try std.testing.expectEqual(probe.deadlines[0], probe.deadlines[1]);
    try std.testing.expectEqual(@as(usize, 1), probe.clock_reads);
}

// run
// backend=llvm
// link_libc=true
// target=aarch64-macos,x86_64-linux,aarch64-linux
