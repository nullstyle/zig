const std = @import("std");
const Io = std.Io;
const c = std.c;
const HostName = Io.net.HostName;
const expect = std.testing.expect;
const expectEqual = std.testing.expectEqual;
const expectError = std.testing.expectError;
const expectEqualStrings = std.testing.expectEqualStrings;

// Override the DNS-SD entry points with a deterministic resolver daemon.
// Replies travel through real pipes and the backend's real Dispatch waits.
const DNSServiceRef = *opaque {};
const Reply = *const fn (DNSServiceRef, u32, u32, i32, ?[*:0]const u8, ?*const c.sockaddr, u32, ?*anyopaque) callconv(.c) void;
const Mode = enum { answers, many, delay_ip6, no_ip6, missing, timeout, stalled, fail_start, fail_process, bad_canon };
var mode: Mode = .answers;
var calls: std.atomic.Value(usize) = .init(0);
var live_refs: std.atomic.Value(usize) = .init(0);
var processed_ip4: std.atomic.Value(bool) = .init(false);
var pending_ip6: std.atomic.Value(?*MockRef) = .init(null);
var lookup_done: std.atomic.Value(bool) = .init(false);

const MockRef = struct {
    pipe: [2]c.fd_t,
    reply: Reply,
    context: ?*anyopaque,
    protocol: u32,
    mode: Mode,
};

export fn DNSServiceGetAddrInfo(out: *DNSServiceRef, flags: u32, interface_index: u32, protocol: u32, hostname: [*:0]const u8, reply: Reply, context: ?*anyopaque) callconv(.c) i32 {
    std.debug.assert(flags == 0x10000 and interface_index == 0);
    std.debug.assert(protocol == 1 or protocol == 2);
    std.debug.assert(std.mem.eql(u8, std.mem.sliceTo(hostname, 0), "mock.example"));
    _ = calls.fetchAdd(1, .monotonic);
    if (mode == .fail_start) return -65539;
    const ref = std.heap.page_allocator.create(MockRef) catch return -65539;
    if (c.pipe(&ref.pipe) != 0) {
        std.heap.page_allocator.destroy(ref);
        return -65539;
    }
    ref.reply = reply;
    ref.context = context;
    ref.protocol = protocol;
    ref.mode = mode;
    _ = live_refs.fetchAdd(1, .monotonic);
    out.* = @ptrCast(ref);
    if (mode == .delay_ip6 and protocol == 2) {
        pending_ip6.store(ref, .release);
    } else if (mode != .stalled) {
        std.debug.assert(c.write(ref.pipe[1], "x", 1) == 1);
    }
    return 0;
}

export fn DNSServiceRefSockFD(service: DNSServiceRef) callconv(.c) c_int {
    const ref: *MockRef = @ptrCast(@alignCast(service));
    return ref.pipe[0];
}

export fn DNSServiceProcessResult(service: DNSServiceRef) callconv(.c) i32 {
    const ref: *MockRef = @ptrCast(@alignCast(service));
    var byte: [1]u8 = undefined;
    std.debug.assert(c.read(ref.pipe[0], &byte, 1) == 1);
    if (ref.mode == .fail_process) return -65563;
    if (ref.mode == .missing or ref.mode == .timeout or (ref.mode == .no_ip6 and ref.protocol == 2)) {
        // DNS-SD says all parameters except the error are undefined on error.
        // MoreComing is deliberately set to catch a wait for a nonexistent reply.
        ref.reply(service, 0xffffffff, 0, if (ref.mode == .timeout) -65568 else -65554, null, null, 0, ref.context);
        return 0;
    }
    var ip4: c.sockaddr.in = .{ .port = 0, .addr = @bitCast([4]u8{ 192, 0, 2, 1 }) };
    var ip6: c.sockaddr.in6 = .{ .port = 0, .flowinfo = 0, .scope_id = 0, .addr = .{ 0x20, 1, 0xd, 0xb8, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 1 } };
    const address: *const c.sockaddr = if (ref.protocol == 1) @ptrCast(&ip4) else @ptrCast(&ip6);
    const count: usize = if (ref.mode == .many) 32 else 1;
    for (0..count) |i| {
        ref.reply(service, 2 | @as(u32, if (i + 1 != count) 1 else 0), 0, 0, if (ref.mode == .bad_canon) "bad name" else "canonical.example.", address, 60, ref.context);
    }
    if (ref.protocol == 1) processed_ip4.store(true, .release);
    return 0;
}

export fn DNSServiceRefDeallocate(service: DNSServiceRef) callconv(.c) void {
    const ref: *MockRef = @ptrCast(@alignCast(service));
    _ = c.close(ref.pipe[0]);
    _ = c.close(ref.pipe[1]);
    std.heap.page_allocator.destroy(ref);
    _ = live_refs.fetchSub(1, .release);
}

fn reset(new_mode: Mode) !void {
    try expectEqual(0, live_refs.load(.acquire));
    mode = new_mode;
    calls.store(0, .monotonic);
    processed_ip4.store(false, .monotonic);
    pending_ip6.store(null, .monotonic);
    lookup_done.store(false, .monotonic);
}

fn performLookup(io: Io, queue: *Io.Queue(HostName.LookupResult), canon: *[HostName.max_len]u8, family: ?Io.net.IpAddress.Family) HostName.LookupError!void {
    defer lookup_done.store(true, .release);
    return (HostName{ .bytes = "mock.example" }).lookup(io, queue, .{ .port = 8443, .family = family, .canonical_name_buffer = canon });
}

fn checkResults(io: Io, queue: *Io.Queue(HostName.LookupResult), expected: usize, ip4: bool, ip6: bool, canonical: []const u8) !void {
    var addresses: usize = 0;
    var names: usize = 0;
    var found_ip4 = false;
    var found_ip6 = false;
    while (queue.getOne(io)) |result| switch (result) {
        .address => |address| {
            addresses += 1;
            try expectEqual(8443, address.getPort());
            switch (address) {
                .ip4 => found_ip4 = true,
                .ip6 => found_ip6 = true,
            }
        },
        .canonical_name => |name| {
            names += 1;
            try expectEqualStrings(canonical, name.bytes);
        },
    } else |err| switch (err) {
        error.Closed => {},
        else => return err,
    }
    try expectEqual(expected, addresses);
    try expectEqual(1, names);
    try expectEqual(ip4, found_ip4);
    try expectEqual(ip6, found_ip6);
}

fn testFastPaths(io: Io) !void {
    try reset(.answers);
    for ([_][]const u8{ "localhost", "LOCALHOST.", "child.localhost", "child.LoCaLhOsT." }) |name| {
        var buffer: [16]HostName.LookupResult = undefined;
        var queue: Io.Queue(HostName.LookupResult) = .init(&buffer);
        var canon: [HostName.max_len]u8 = undefined;
        try (try HostName.init(name)).lookup(io, &queue, .{ .port = 8443, .canonical_name_buffer = &canon });
        try checkResults(io, &queue, 2, true, true, "localhost");
    }
    var buffer: [16]HostName.LookupResult = undefined;
    var queue: Io.Queue(HostName.LookupResult) = .init(&buffer);
    var canon: [HostName.max_len]u8 = undefined;
    try (try HostName.init("127.0.0.1")).lookup(io, &queue, .{ .port = 8443, .canonical_name_buffer = &canon });
    try checkResults(io, &queue, 1, true, false, "127.0.0.1");
    queue = .init(&buffer);
    try expectError(error.UnknownHostName, (try HostName.init("127.0.0.1")).lookup(io, &queue, .{ .port = 8443, .family = .ip6 }));
    try expectError(error.Closed, queue.getOne(io));
    try expectEqual(0, calls.load(.monotonic));
}

fn testAnswers(io: Io) !void {
    for ([_]?Io.net.IpAddress.Family{ .ip4, .ip6, null }) |family| {
        try reset(.answers);
        var buffer: [16]HostName.LookupResult = undefined;
        var queue: Io.Queue(HostName.LookupResult) = .init(&buffer);
        var canon: [HostName.max_len]u8 = undefined;
        try performLookup(io, &queue, &canon, family);
        try checkResults(io, &queue, if (family == null) 2 else 1, family != .ip6, family != .ip4, "canonical.example.");
        try expectEqual(if (family == null) @as(usize, 2) else 1, calls.load(.monotonic));
    }
}

fn testDelayedFamily(io: Io) !void {
    try reset(.delay_ip6);
    var buffer: [16]HostName.LookupResult = undefined;
    var queue: Io.Queue(HostName.LookupResult) = .init(&buffer);
    var canon: [HostName.max_len]u8 = undefined;
    var future = Io.async(io, performLookup, .{ io, &queue, &canon, null });
    defer future.cancel(io) catch {};
    while (pending_ip6.load(.acquire) == null or !processed_ip4.load(.acquire)) {
        try Io.sleep(io, .fromMilliseconds(1), .awake);
    }
    try expect(!lookup_done.load(.acquire));
    const ref = pending_ip6.load(.acquire).?;
    try expectEqual(1, c.write(ref.pipe[1], "x", 1));
    try future.await(io);
    try checkResults(io, &queue, 2, true, true, "canonical.example.");
}

fn testMissingFamily(io: Io) !void {
    try reset(.no_ip6);
    var buffer: [16]HostName.LookupResult = undefined;
    var queue: Io.Queue(HostName.LookupResult) = .init(&buffer);
    var canon: [HostName.max_len]u8 = undefined;
    try performLookup(io, &queue, &canon, null);
    try checkResults(io, &queue, 1, true, false, "canonical.example.");
}

fn testBoundedResults(io: Io) !void {
    try reset(.many);
    var buffer: [16]HostName.LookupResult = undefined;
    var queue: Io.Queue(HostName.LookupResult) = .init(&buffer);
    var canon: [HostName.max_len]u8 = undefined;
    // A capacity-16 queue must not block even when the daemon supplies more.
    try performLookup(io, &queue, &canon, .ip4);
    try checkResults(io, &queue, 15, true, false, "canonical.example.");
}

fn testQueueBackpressure(io: Io) !void {
    try reset(.many);
    var buffer: [1]HostName.LookupResult = undefined;
    var queue: Io.Queue(HostName.LookupResult) = .init(&buffer);
    var canon: [HostName.max_len]u8 = undefined;
    var future = Io.async(io, performLookup, .{ io, &queue, &canon, .ip4 });
    defer future.cancel(io) catch {};
    // Reply delivery must remain cancellable when a smaller queue requires
    // the resolver fiber and consumer to take turns.
    try checkResults(io, &queue, 15, true, false, "canonical.example.");
    try future.await(io);
}

fn testErrors(io: Io) !void {
    const modes = [_]Mode{ .missing, .timeout, .fail_start, .fail_process, .bad_canon };
    const errors = [_]anyerror{ error.UnknownHostName, error.NameServerFailure, error.SystemResources, error.NameServerFailure, error.InvalidDnsCnameRecord };
    for (modes, errors) |test_mode, expected_error| {
        try reset(test_mode);
        var buffer: [16]HostName.LookupResult = undefined;
        var queue: Io.Queue(HostName.LookupResult) = .init(&buffer);
        var canon: [HostName.max_len]u8 = undefined;
        try expectError(expected_error, performLookup(io, &queue, &canon, .ip4));
        try expectError(error.Closed, queue.getOne(io));
    }
}

fn testCancellation(io: Io) !void {
    for (0..32) |_| {
        try reset(.stalled);
        var buffer: [16]HostName.LookupResult = undefined;
        var queue: Io.Queue(HostName.LookupResult) = .init(&buffer);
        var canon: [HostName.max_len]u8 = undefined;
        var future = Io.async(io, performLookup, .{ io, &queue, &canon, null });
        while (live_refs.load(.acquire) != 2) {
            try Io.sleep(io, .fromMilliseconds(1), .awake);
        }
        try expectError(error.Canceled, future.cancel(io));
        try expect(lookup_done.load(.acquire));
        try expectEqual(0, live_refs.load(.acquire));
        try expectError(error.Closed, queue.getOne(io));
    }
}

fn testCancellationAfterFirstFamily(io: Io) !void {
    try reset(.delay_ip6);
    var buffer: [16]HostName.LookupResult = undefined;
    var queue: Io.Queue(HostName.LookupResult) = .init(&buffer);
    var canon: [HostName.max_len]u8 = undefined;
    var future = Io.async(io, performLookup, .{ io, &queue, &canon, null });
    while (pending_ip6.load(.acquire) == null or !processed_ip4.load(.acquire)) {
        try Io.sleep(io, .fromMilliseconds(1), .awake);
    }
    try expectError(error.Canceled, future.cancel(io));
    try expectEqual(0, live_refs.load(.acquire));
    const result = try queue.getOne(io);
    try expect(result == .address);
    try expectError(error.Closed, queue.getOne(io));
}

pub fn main() !void {
    var evented: Io.Evented = undefined;
    try evented.init(std.heap.page_allocator, .{});
    defer evented.deinit();
    const io = evented.io();
    try testFastPaths(io);
    try testAnswers(io);
    try testDelayedFamily(io);
    try testMissingFamily(io);
    try testBoundedResults(io);
    try testQueueBackpressure(io);
    try testErrors(io);
    try testCancellation(io);
    try testCancellationAfterFirstFamily(io);
    try expectEqual(0, live_refs.load(.acquire));
}

// run
// backend=llvm
// target=aarch64-macos,x86_64-macos
// link_libc=true
