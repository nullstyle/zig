const std = @import("std");
const Io = std.Io;

/// A batched `net_receive` that completes at submit time (its datagram was
/// already queued) must leave the `submitted` list when it moves to
/// `completed`. The bug it regressions: the slot stayed linked, so a second
/// wait on the same batch walked it as a submission after its union had
/// become `completion` — a union safety panic in safe builds, a garbage
/// operation in ReleaseFast — and the poll pairing of the remaining
/// submitted operations shifted by one.
fn testBatchSubmitTimeCompletion(io: Io) !void {
    const bind_address: Io.net.IpAddress = .{ .ip4 = .loopback(0) };
    var busy = try Io.net.IpAddress.bind(&bind_address, io, .{ .mode = .dgram });
    defer busy.close(io);
    var idle = try Io.net.IpAddress.bind(&bind_address, io, .{ .mode = .dgram });
    defer idle.close(io);
    var sender = try Io.net.IpAddress.bind(&bind_address, io, .{ .mode = .dgram });
    defer sender.close(io);
    try sender.send(io, &busy.address, "queued");

    const short: Io.Timeout = .{ .duration = .{ .raw = .fromMilliseconds(20), .clock = .awake } };

    var busy_messages: [1]Io.net.IncomingMessage = .{.init};
    var busy_buf: [64]u8 = undefined;
    var idle_messages: [1]Io.net.IncomingMessage = .{.init};
    var idle_buf: [64]u8 = undefined;
    var storage: [2]Io.Operation.Storage = undefined;
    var batch: Io.Batch = .init(&storage);
    defer batch.cancel(io);

    // Slot 0 completes at submit time (the datagram is queued); slot 1 arms
    // a readiness wait on the empty socket.
    batch.addAt(0, .{ .net_receive = .{
        .socket_handle = busy.handle,
        .message_buffer = &busy_messages,
        .data_buffer = &busy_buf,
        .flags = .{},
    } });
    batch.addAt(1, .{ .net_receive = .{
        .socket_handle = idle.handle,
        .message_buffer = &idle_messages,
        .data_buffer = &idle_buf,
        .flags = .{},
    } });

    // The first wait returns with the submit-time completion; the receive on
    // the idle socket is still pending.
    try batch.awaitConcurrent(io, short);
    const completion = batch.next() orelse return error.TestUnexpectedResult;
    if (completion.index != 0) return error.TestUnexpectedResult;
    const opt_err, const count = completion.result.net_receive;
    if (opt_err != null or count != 1) return error.TestUnexpectedResult;
    try std.testing.expectEqualStrings("queued", busy_messages[0].data);

    // The second wait must see only the still-pending receive: a short
    // timeout, then the batch is cancelled.
    try std.testing.expectError(error.Timeout, batch.awaitConcurrent(io, short));
    if (batch.next() != null) return error.TestUnexpectedResult;
}

pub fn main() !void {
    var threaded: Io.Threaded = .init(std.heap.page_allocator, .{});
    defer threaded.deinit();
    try testBatchSubmitTimeCompletion(threaded.io());
}

// run
// target=aarch64-linux,x86_64-linux,aarch64-macos,x86_64-macos
