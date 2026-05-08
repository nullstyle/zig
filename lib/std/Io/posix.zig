//! Internal POSIX adapter layer shared by `Io.Threaded` and `Io.Dispatch`.
//!
//! Items here are decoupled from any specific backend's runtime model:
//! - sockaddr ↔ `Io.net.IpAddress` / `Io.net.UnixAddress` conversions
//! - errno mapping and bug-class helpers
//! - clock / timestamp / stat / wait-status conversions
//! - per-instance state shared between backends (CSPRNG, environ snapshot,
//!   sendfile/fcopyfile feature flags, argv[0] override)
//!
//! Both backends import what they need from here. Items genuinely tied to a
//! specific backend's runtime model (such as `Io.Threaded.Syscall` or
//! `Io.Dispatch.WaitReadyWaiter`) stay in their respective files.

const std = @import("../std.zig");
const builtin = @import("builtin");

const native_os = builtin.os.tag;
const is_windows = native_os == .windows;
const is_darwin = native_os.isDarwin();
const is_debug = builtin.mode == .Debug;

const Io = std.Io;
const net = Io.net;
const HostName = net.HostName;
const IpAddress = net.IpAddress;
const File = Io.File;
const Dir = Io.Dir;
const process = std.process;
const Allocator = std.mem.Allocator;
const posix = std.posix;
const windows = std.os.windows;
const assert = std.debug.assert;

// =============================================================================
// I/O sizing constants
// =============================================================================

pub const max_iovecs_len = 8;
pub const splat_buffer_size = 64;
pub const default_PATH = "/usr/local/bin:/bin/:/usr/bin";

// =============================================================================
// CSPRNG and random seeding
// =============================================================================

pub const Csprng = struct {
    rng: std.Random.DefaultCsprng,

    pub const uninitialized: Csprng = .{ .rng = .{
        .state = undefined,
        .offset = std.math.maxInt(usize),
    } };

    pub const seed_len = std.Random.DefaultCsprng.secret_seed_length;

    pub fn isInitialized(c: *const Csprng) bool {
        return c.rng.offset != std.math.maxInt(usize);
    }
};

pub fn fallbackSeed(aslr_addr: ?*anyopaque, seed: *[Csprng.seed_len]u8) void {
    @memset(seed, 0);
    std.mem.writeInt(usize, seed[seed.len - @sizeOf(usize) ..][0..@sizeOf(usize)], @intFromPtr(aslr_addr), .native);
    const fallbackSeedImpl = switch (native_os) {
        .windows => fallbackSeedWindows,
        .wasi => if (builtin.link_libc) fallbackSeedPosix else fallbackSeedWasi,
        else => fallbackSeedPosix,
    };
    fallbackSeedImpl(seed);
}

fn fallbackSeedPosix(seed: *[Csprng.seed_len]u8) void {
    std.mem.writeInt(posix.pid_t, seed[0..@sizeOf(posix.pid_t)], posix.system.getpid(), .native);
    const i_1 = @sizeOf(posix.pid_t);

    var ts: posix.timespec = undefined;
    const Sec = @TypeOf(ts.sec);
    const Nsec = @TypeOf(ts.nsec);
    const i_2 = i_1 + @sizeOf(Sec);
    switch (posix.errno(posix.system.clock_gettime(.REALTIME, &ts))) {
        .SUCCESS => {
            std.mem.writeInt(Sec, seed[i_1..][0..@sizeOf(Sec)], ts.sec, .native);
            std.mem.writeInt(Nsec, seed[i_2..][0..@sizeOf(Nsec)], ts.nsec, .native);
        },
        else => {},
    }
}

fn fallbackSeedWindows(seed: *[Csprng.seed_len]u8) void {
    var pc: windows.LARGE_INTEGER = undefined;
    _ = windows.ntdll.RtlQueryPerformanceCounter(&pc);
    std.mem.writeInt(windows.LARGE_INTEGER, seed[0..@sizeOf(windows.LARGE_INTEGER)], pc, .native);
}

fn fallbackSeedWasi(seed: *[Csprng.seed_len]u8) void {
    var ts: std.os.wasi.timestamp_t = undefined;
    if (std.os.wasi.clock_time_get(.REALTIME, 1, &ts) == .SUCCESS) {
        std.mem.writeInt(std.os.wasi.timestamp_t, seed[0..@sizeOf(std.os.wasi.timestamp_t)], ts, .native);
    }
}

// =============================================================================
// Process spawn and environment
// =============================================================================

pub const Argv0 = switch (native_os) {
    .openbsd, .haiku => struct {
        value: ?[*:0]const u8,

        pub const empty: Argv0 = .{ .value = null };

        pub fn init(args: process.Args) Argv0 {
            return .{ .value = args.vector[0] };
        }
    },
    else => struct {
        pub const empty: Argv0 = .{};

        pub fn init(args: process.Args) Argv0 {
            _ = args;
            return .{};
        }
    },
};

pub const Environ = struct {
    /// Unmodified data directly from the OS.
    process_environ: process.Environ,
    /// Protected by `mutex`. Memoized based on `process_environ`. Tracks whether the
    /// environment variables are present, ignoring their value.
    exist: Exist = .{},
    /// Protected by `mutex`. Memoized based on `process_environ`.
    string: String = .{},
    /// ZIG_PROGRESS
    zig_progress_file: std.Progress.ParentFileError!File = error.EnvironmentVariableMissing,
    /// Protected by `mutex`. Tracks the problem, if any, that occurred when
    /// trying to scan environment variables.
    ///
    /// Errors are only possible on WASI.
    err: ?Error = null,

    pub const empty: Environ = .{ .process_environ = .empty };

    pub const Error = Allocator.Error || Io.UnexpectedError;

    pub const Exist = struct {
        NO_COLOR: bool = false,
        CLICOLOR_FORCE: bool = false,
    };

    pub const String = switch (native_os) {
        .windows, .wasi => struct {},
        else => struct {
            PATH: ?[:0]const u8 = null,
            DEBUGINFOD_CACHE_PATH: ?[:0]const u8 = null,
            XDG_CACHE_HOME: ?[:0]const u8 = null,
            HOME: ?[:0]const u8 = null,
        },
    };

    pub fn scan(environ: *Environ, allocator: Allocator) void {
        if (is_windows) {
            // This value expires with any call that modifies the environment,
            // which is outside of this Io implementation's control, so references
            // must be short-lived.
            const peb = windows.peb();
            assert(windows.ntdll.RtlEnterCriticalSection(peb.FastPebLock) == .SUCCESS);
            defer assert(windows.ntdll.RtlLeaveCriticalSection(peb.FastPebLock) == .SUCCESS);
            const ptr = peb.ProcessParameters.Environment;

            var i: usize = 0;
            while (ptr[i] != 0) {
                // There are some special environment variables that start with =,
                // so we need a special case to not treat = as a key/value separator
                // if it's the first character.
                // https://devblogs.microsoft.com/oldnewthing/20100506-00/?p=14133
                const key_start = i;
                if (ptr[i] == '=') i += 1;
                while (ptr[i] != 0 and ptr[i] != '=') : (i += 1) {}
                const key_w = ptr[key_start..i];

                const value_start = i + 1;
                while (ptr[i] != 0) : (i += 1) {} // skip over '=' and value
                const value_w = ptr[value_start..i];
                i += 1; // skip over null byte

                if (windows.eqlIgnoreCaseWtf16(key_w, &.{ 'N', 'O', '_', 'C', 'O', 'L', 'O', 'R' })) {
                    environ.exist.NO_COLOR = true;
                } else if (windows.eqlIgnoreCaseWtf16(key_w, &.{ 'C', 'L', 'I', 'C', 'O', 'L', 'O', 'R', '_', 'F', 'O', 'R', 'C', 'E' })) {
                    environ.exist.CLICOLOR_FORCE = true;
                } else if (windows.eqlIgnoreCaseWtf16(key_w, &.{ 'Z', 'I', 'G', '_', 'P', 'R', 'O', 'G', 'R', 'E', 'S', 'S' })) {
                    environ.zig_progress_file = file: {
                        var value_buf: [std.fmt.count("{d}", .{std.math.maxInt(usize)})]u8 = undefined;
                        const len = std.unicode.calcWtf8Len(value_w);
                        if (len > value_buf.len) break :file error.UnrecognizedFormat;
                        assert(std.unicode.wtf16LeToWtf8(&value_buf, value_w) == len);
                        break :file .{
                            .handle = @ptrFromInt(std.fmt.parseInt(usize, value_buf[0..len], 10) catch
                                break :file error.UnrecognizedFormat),
                            .flags = .{ .nonblocking = true },
                        };
                    };
                }
                comptime assert(@sizeOf(String) == 0);
            }
        } else if (native_os == .wasi and !builtin.link_libc) {
            var environ_size: usize = undefined;
            var environ_buf_size: usize = undefined;

            switch (std.os.wasi.environ_sizes_get(&environ_size, &environ_buf_size)) {
                .SUCCESS => {},
                else => |err| {
                    environ.err = posix.unexpectedErrno(err);
                    return;
                },
            }
            if (environ_size == 0) return;

            const wasi_environ = allocator.alloc([*:0]u8, environ_size) catch |err| {
                environ.err = err;
                return;
            };
            defer allocator.free(wasi_environ);
            const wasi_environ_buf = allocator.alloc(u8, environ_buf_size) catch |err| {
                environ.err = err;
                return;
            };
            defer allocator.free(wasi_environ_buf);

            switch (std.os.wasi.environ_get(wasi_environ.ptr, wasi_environ_buf.ptr)) {
                .SUCCESS => {},
                else => |err| {
                    environ.err = posix.unexpectedErrno(err);
                    return;
                },
            }

            for (wasi_environ) |env| {
                const pair = std.mem.sliceTo(env, 0);
                var parts = std.mem.splitScalar(u8, pair, '=');
                const key = parts.first();
                if (std.mem.eql(u8, key, "NO_COLOR")) {
                    environ.exist.NO_COLOR = true;
                } else if (std.mem.eql(u8, key, "CLICOLOR_FORCE")) {
                    environ.exist.CLICOLOR_FORCE = true;
                }
                comptime assert(@sizeOf(String) == 0);
            }
        } else {
            for (environ.process_environ.block.slice) |opt_entry| {
                const entry = opt_entry.?;
                var entry_i: usize = 0;
                while (entry[entry_i] != 0 and entry[entry_i] != '=') : (entry_i += 1) {}
                const key = entry[0..entry_i];

                var end_i: usize = entry_i;
                while (entry[end_i] != 0) : (end_i += 1) {}
                const value = entry[entry_i + 1 .. end_i :0];

                if (std.mem.eql(u8, key, "NO_COLOR")) {
                    environ.exist.NO_COLOR = true;
                } else if (std.mem.eql(u8, key, "CLICOLOR_FORCE")) {
                    environ.exist.CLICOLOR_FORCE = true;
                } else if (std.mem.eql(u8, key, "ZIG_PROGRESS")) {
                    environ.zig_progress_file = file: {
                        break :file .{
                            .handle = std.fmt.parseInt(u31, value, 10) catch
                                break :file error.UnrecognizedFormat,
                            .flags = .{ .nonblocking = true },
                        };
                    };
                } else inline for (@typeInfo(String).@"struct".fields) |field| {
                    if (std.mem.eql(u8, key, field.name)) @field(environ.string, field.name) = value;
                }
            }
        }
    }
};

// =============================================================================
// Sendfile / fcopyfile feature flags
// =============================================================================

const have_sendfile = if (builtin.link_libc) @TypeOf(std.c.sendfile) != void else native_os == .linux;
const have_fcopyfile = is_darwin;

pub const UseSendfile = if (have_sendfile) enum {
    enabled,
    disabled,
    pub const default: UseSendfile = .enabled;
} else enum {
    disabled,
    pub const default: UseSendfile = .disabled;
};

pub const UseFcopyfile = if (have_fcopyfile) enum {
    enabled,
    disabled,
    pub const default: UseFcopyfile = .enabled;
} else enum {
    disabled,
    pub const default: UseFcopyfile = .disabled;
};

// =============================================================================
// Errno helpers
// =============================================================================

pub fn errnoBug(err: posix.E) Io.UnexpectedError {
    if (is_debug) std.debug.panic("programmer bug caused syscall error: {t}", .{err});
    return error.Unexpected;
}

pub fn recoverableOsBugDetected() void {
    if (is_debug) unreachable;
}

// =============================================================================
// File-descriptor helpers
// =============================================================================

pub fn closeFd(fd: posix.fd_t) void {
    if (native_os == .wasi and !builtin.link_libc) {
        switch (std.os.wasi.fd_close(fd)) {
            .SUCCESS, .INTR => {},
            .BADF => recoverableOsBugDetected(), // use after free
            else => recoverableOsBugDetected(), // unexpected failure
        }
    } else switch (posix.errno(posix.system.close(fd))) {
        .SUCCESS, .INTR => {}, // INTR still a success, see https://github.com/ziglang/zig/issues/2425
        .BADF => recoverableOsBugDetected(), // use after free
        else => recoverableOsBugDetected(), // unexpected failure
    }
}

// =============================================================================
// Path helpers
// =============================================================================

pub fn pathToPosix(file_path: []const u8, buffer: *[posix.PATH_MAX]u8) Dir.PathNameError![:0]u8 {
    if (std.mem.containsAtLeastScalar2(u8, file_path, 0, 1)) return error.BadPathName;
    // >= rather than > to make room for the null byte
    if (file_path.len >= buffer.len) return error.NameTooLong;
    @memcpy(buffer[0..file_path.len], file_path);
    buffer[file_path.len] = 0;
    return buffer[0..file_path.len :0];
}

pub const ChdirError = error{
    AccessDenied,
    FileSystem,
    SymLinkLoop,
    NameTooLong,
    FileNotFound,
    SystemResources,
    NotDir,
    BadPathName,
} || Io.Cancelable || Io.UnexpectedError;

// =============================================================================
// Clock / timestamp / stat conversions
// =============================================================================

pub fn clockToPosix(clock: Io.Clock) posix.clockid_t {
    return switch (clock) {
        .real => posix.CLOCK.REALTIME,
        .awake => switch (native_os) {
            .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => posix.CLOCK.UPTIME_RAW,
            else => posix.CLOCK.MONOTONIC,
        },
        .boot => switch (native_os) {
            .driverkit, .ios, .maccatalyst, .macos, .tvos, .visionos, .watchos => posix.CLOCK.MONOTONIC_RAW,
            // On freebsd derivatives, use MONOTONIC_FAST as currently there's
            // no precision tradeoff.
            .freebsd, .dragonfly => posix.CLOCK.MONOTONIC_FAST,
            // On linux, use BOOTTIME instead of MONOTONIC as it ticks while
            // suspended.
            .linux => posix.CLOCK.BOOTTIME,
            // On other posix systems, MONOTONIC is generally the fastest and
            // ticks while suspended.
            else => posix.CLOCK.MONOTONIC,
        },
        .cpu_process => posix.CLOCK.PROCESS_CPUTIME_ID,
        .cpu_thread => posix.CLOCK.THREAD_CPUTIME_ID,
    };
}

pub fn timestampFromPosix(timespec: *const posix.timespec) Io.Timestamp {
    return .{ .nanoseconds = nanosecondsFromPosix(timespec) };
}

pub fn nanosecondsFromPosix(timespec: *const posix.timespec) i96 {
    return @intCast(@as(i128, timespec.sec) * std.time.ns_per_s + timespec.nsec);
}

pub fn setTimestampToPosix(set_ts: File.SetTimestamp) posix.timespec {
    return switch (set_ts) {
        .unchanged => posix.UTIME.OMIT,
        .now => posix.UTIME.NOW,
        .new => |t| timestampToPosix(t.nanoseconds),
    };
}

pub fn timestampToPosix(nanoseconds: i96) posix.timespec {
    if (builtin.zig_backend == .stage2_wasm) {
        // Workaround for https://codeberg.org/ziglang/zig/issues/30575
        return .{
            .sec = @intCast(@divTrunc(nanoseconds, std.time.ns_per_s)),
            .nsec = @intCast(@rem(nanoseconds, std.time.ns_per_s)),
        };
    }
    return .{
        .sec = @intCast(@divFloor(nanoseconds, std.time.ns_per_s)),
        .nsec = @intCast(@mod(nanoseconds, std.time.ns_per_s)),
    };
}

pub fn statFromPosix(st: *const posix.Stat) File.Stat {
    const atime = st.atime();
    const mtime = st.mtime();
    const ctime = st.ctime();
    return .{
        .inode = st.ino,
        .nlink = st.nlink,
        .size = @bitCast(st.size),
        .permissions = .fromMode(st.mode),
        .kind = k: {
            const m = st.mode & posix.S.IFMT;
            switch (m) {
                posix.S.IFBLK => break :k .block_device,
                posix.S.IFCHR => break :k .character_device,
                posix.S.IFDIR => break :k .directory,
                posix.S.IFIFO => break :k .named_pipe,
                posix.S.IFLNK => break :k .sym_link,
                posix.S.IFREG => break :k .file,
                posix.S.IFSOCK => break :k .unix_domain_socket,
                else => {},
            }
            if (native_os == .illumos) switch (m) {
                posix.S.IFDOOR => break :k .door,
                posix.S.IFPORT => break :k .event_port,
                else => {},
            };

            break :k .unknown;
        },
        .atime = timestampFromPosix(&atime),
        .mtime = timestampFromPosix(&mtime),
        .ctime = timestampFromPosix(&ctime),
        .block_size = @intCast(st.blksize),
    };
}

pub fn statusToTerm(status: u32) process.Child.Term {
    return if (posix.W.IFEXITED(status))
        .{ .exited = posix.W.EXITSTATUS(status) }
    else if (posix.W.IFSIGNALED(status))
        .{ .signal = posix.W.TERMSIG(status) }
    else if (posix.W.IFSTOPPED(status))
        .{ .stopped = posix.W.STOPSIG(status) }
    else
        .{ .unknown = status };
}

// =============================================================================
// Socket address conversions
// =============================================================================

pub const PosixAddress = extern union {
    any: posix.sockaddr,
    in: posix.sockaddr.in,
    in6: posix.sockaddr.in6,
};

pub const UnixAddress = extern union {
    any: posix.sockaddr,
    un: posix.sockaddr.un,
};

pub fn posixAddressFamily(a: *const IpAddress) posix.sa_family_t {
    return switch (a.*) {
        .ip4 => posix.AF.INET,
        .ip6 => posix.AF.INET6,
    };
}

pub fn addressFromPosix(posix_address: *const PosixAddress) IpAddress {
    return switch (posix_address.any.family) {
        posix.AF.INET => .{ .ip4 = address4FromPosix(&posix_address.in) },
        posix.AF.INET6 => .{ .ip6 = address6FromPosix(&posix_address.in6) },
        else => .{ .ip4 = .loopback(0) },
    };
}

pub fn addressToPosix(a: *const IpAddress, storage: *PosixAddress) posix.socklen_t {
    return switch (a.*) {
        .ip4 => |ip4| {
            storage.in = address4ToPosix(ip4);
            return @sizeOf(posix.sockaddr.in);
        },
        .ip6 => |*ip6| {
            storage.in6 = address6ToPosix(ip6);
            return @sizeOf(posix.sockaddr.in6);
        },
    };
}

pub fn addressUnixToPosix(a: *const net.UnixAddress, storage: *UnixAddress) posix.socklen_t {
    storage.un.family = posix.AF.UNIX;
    var path_len = switch (native_os) {
        .windows => @min(a.path.len, storage.un.path.len),
        else => a.path.len,
    };
    // With the AFD API, `sockaddr.un` is purely informational, so
    // use a suffix which is usually the most relevant part of a path.
    @memcpy(storage.un.path[0..path_len], a.path[a.path.len - path_len ..]);
    if (storage.un.path.len - path_len > 0) {
        @branchHint(.likely);
        storage.un.path[path_len] = 0;
        path_len += 1;
    }
    switch (native_os) {
        .windows => {
            if (storage.un.path[0] == 0) @memset(storage.un.path[path_len..], 0);
            return @sizeOf(posix.sockaddr.un);
        },
        else => return @intCast(@offsetOf(posix.sockaddr.un, "path") + path_len),
    }
}

fn address4FromPosix(in: *const posix.sockaddr.in) net.Ip4Address {
    return .{
        .port = std.mem.bigToNative(u16, in.port),
        .bytes = @bitCast(in.addr),
    };
}

fn address6FromPosix(in6: *const posix.sockaddr.in6) net.Ip6Address {
    return .{
        .port = std.mem.bigToNative(u16, in6.port),
        .bytes = in6.addr,
        .flow = in6.flowinfo,
        .interface = .{ .index = in6.scope_id },
    };
}

fn address4ToPosix(a: net.Ip4Address) posix.sockaddr.in {
    return .{
        .port = std.mem.nativeToBig(u16, a.port),
        .addr = @bitCast(a.bytes),
    };
}

fn address6ToPosix(a: *const net.Ip6Address) posix.sockaddr.in6 {
    return .{
        .port = std.mem.nativeToBig(u16, a.port),
        .flowinfo = a.flow,
        .addr = a.bytes,
        .scope_id = a.interface.index,
    };
}

pub fn posixSocketModeProtocol(family: posix.sa_family_t, mode: net.Socket.Mode, protocol: ?net.Protocol) !struct { u32, u32 } {
    return .{
        switch (mode) {
            .stream => posix.SOCK.STREAM,
            .dgram => posix.SOCK.DGRAM,
            .seqpacket => posix.SOCK.SEQPACKET,
            .raw => posix.SOCK.RAW,
            .rdm => if (@hasDecl(posix.SOCK, "RDM")) posix.SOCK.RDM else return error.OptionUnsupported,
        },
        if (protocol) |p| @intFromEnum(p) else if (is_windows) switch (family) {
            posix.AF.UNIX => switch (mode) {
                .stream => 0,
                else => return error.ProtocolUnsupportedByAddressFamily,
            },
            posix.AF.INET, posix.AF.INET6 => @intFromEnum(@as(net.Protocol, switch (mode) {
                .stream => .tcp,
                .dgram => .udp,
                else => return error.ProtocolUnsupportedByAddressFamily,
            })),
            else => return error.ProtocolUnsupportedByAddressFamily,
        } else 0,
    };
}

// =============================================================================
// DNS lookup helpers
// =============================================================================

pub fn copyCanon(canonical_name_buffer: ?*[HostName.max_len]u8, name: []const u8) ?HostName {
    const buf = canonical_name_buffer orelse return null;
    const dest = buf[0..name.len];
    @memcpy(dest, name);
    return .{ .bytes = dest };
}
