//! Ancillary receive ownership shared by the POSIX I/O backends.
const std = @import("std");
const builtin = @import("builtin");
const posix = std.posix;
const net = std.Io.net;

pub fn checkLength(length: usize) error{InvalidControlMessage}!void {
    // In addition to socklen_t, native kernels use signed lengths internally.
    if (length > std.math.maxInt(i32)) return error.InvalidControlMessage;
}

pub const Receive = struct {
    // XNU externalizes the entire rights message before copyout_control truncates
    // it. sockargs limits MT_CONTROL to one 2048-byte mbuf cluster (including
    // expansion of descriptors to pointers). Capture it before truncating for
    // the caller, so every installed descriptor has an observable owner.
    scratch: [if (builtin.os.tag.isDarwin()) 2048 else 0]u8 align(net.cmsg_align) = undefined,

    pub fn buffer(r: *Receive, destination: []u8) error{InvalidControlMessage}![]u8 {
        try checkLength(destination.len);
        if (destination.len < r.scratch.len) return &r.scratch;
        return destination;
    }

    pub const Result = struct { len: usize, truncated: bool };

    pub fn finish(destination: []u8, received: []u8, length: usize, flags: u32) error{Unexpected}!Result {
        const bytes = received[0..@min(length, received.len)];
        if (!@hasDecl(posix.MSG, "CMSG_CLOEXEC")) {
            setCloseOnExec(bytes) catch {
                closeRights(bytes);
                return error.Unexpected;
            };
        }
        const copied = @min(destination.len, bytes.len);
        var truncated = flags & posix.MSG.CTRUNC != 0 or length > copied;
        var end: usize = 0;
        var offset: usize = 0;
        const header_size = net.cmsg.len(0);
        while (bytes.len - offset >= header_size) {
            const header = std.mem.bytesToValue(posix.cmsghdr, bytes[offset..][0..@sizeOf(posix.cmsghdr)]);
            if (header.len < header_size or header.len > bytes.len - offset) {
                truncated = true;
                break;
            }
            const message_end = offset + header.len;
            const deliver = message_end <= copied;
            if (header.level == posix.SOL.SOCKET and header.type == posix.SCM.RIGHTS) {
                const data = bytes[offset + header_size .. message_end];
                var i: usize = 0;
                while (data.len - i >= @sizeOf(posix.fd_t)) : (i += @sizeOf(posix.fd_t)) {
                    const fd = std.mem.bytesToValue(posix.fd_t, data[i..][0..@sizeOf(posix.fd_t)]);
                    if (!deliver) {
                        _ = posix.system.close(fd);
                    }
                }
            }
            if (deliver) end = @min(copied, std.mem.alignForward(usize, message_end, net.cmsg_align));
            const step = std.mem.alignForward(usize, header.len, net.cmsg_align);
            if (step > bytes.len - offset) break;
            offset += step;
        }
        if (received.ptr != destination.ptr) @memcpy(destination[0..end], bytes[0..end]);
        return .{ .len = end, .truncated = truncated };
    }

    fn setCloseOnExec(bytes: []const u8) error{Unexpected}!void {
        var offset: usize = 0;
        const header_size = net.cmsg.len(0);
        while (bytes.len - offset >= header_size) {
            const header = std.mem.bytesToValue(posix.cmsghdr, bytes[offset..][0..@sizeOf(posix.cmsghdr)]);
            if (header.len < header_size or header.len > bytes.len - offset) break;
            if (header.level == posix.SOL.SOCKET and header.type == posix.SCM.RIGHTS) {
                const data = bytes[offset + header_size ..][0 .. header.len - header_size];
                var i: usize = 0;
                while (data.len - i >= @sizeOf(posix.fd_t)) : (i += @sizeOf(posix.fd_t)) {
                    const fd = std.mem.bytesToValue(posix.fd_t, data[i..][0..@sizeOf(posix.fd_t)]);
                    while (true) {
                        const rc = posix.system.fcntl(fd, posix.F.SETFD, @as(c_int, posix.FD_CLOEXEC));
                        switch (posix.errno(rc)) {
                            .SUCCESS => break,
                            .INTR => continue,
                            else => return error.Unexpected,
                        }
                    }
                }
            }
            const step = std.mem.alignForward(usize, header.len, net.cmsg_align);
            if (step > bytes.len - offset) break;
            offset += step;
        }
    }

    fn closeRights(bytes: []const u8) void {
        var offset: usize = 0;
        const header_size = net.cmsg.len(0);
        while (bytes.len - offset >= header_size) {
            const header = std.mem.bytesToValue(posix.cmsghdr, bytes[offset..][0..@sizeOf(posix.cmsghdr)]);
            if (header.len < header_size or header.len > bytes.len - offset) break;
            if (header.level == posix.SOL.SOCKET and header.type == posix.SCM.RIGHTS) {
                const data = bytes[offset + header_size ..][0 .. header.len - header_size];
                var i: usize = 0;
                while (data.len - i >= @sizeOf(posix.fd_t)) : (i += @sizeOf(posix.fd_t)) {
                    const fd = std.mem.bytesToValue(posix.fd_t, data[i..][0..@sizeOf(posix.fd_t)]);
                    _ = posix.system.close(fd);
                }
            }
            const step = std.mem.alignForward(usize, header.len, net.cmsg_align);
            if (step > bytes.len - offset) break;
            offset += step;
        }
    }
};
