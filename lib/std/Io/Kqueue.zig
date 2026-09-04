const Kqueue = @This();
const builtin = @import("builtin");

const std = @import("../std.zig");
const Io = std.Io;
const Dir = std.Io.Dir;
const File = std.Io.File;
const net = std.Io.net;
const assert = std.debug.assert;
const Allocator = std.mem.Allocator;
const Alignment = std.mem.Alignment;
const IpAddress = std.Io.net.IpAddress;
const errnoBug = std.Io.Threaded.errnoBug;
const closeFd = std.Io.Threaded.closeFd;
const posix = std.posix;
const posixSocketModeProtocol = Io.Threaded.posixSocketModeProtocol;

/// Must be a thread-safe allocator.
gpa: Allocator,
mutex: Io.Mutex,
main_fiber_buffer: [@sizeOf(Fiber) + Fiber.max_result_size]u8 align(@alignOf(Fiber)),
threads: Thread.List,
/// Finished fibers, kept for reuse. Fibers are pooled rather than freed
/// because kevents that fired before their wait was satisfied (or
/// cancelled) can still be delivered later, and their udata points at the
/// fiber or its batch waiter: freed memory would turn that late delivery
/// into use-after-free. A pooled fiber's waiter is reinitialized on reuse,
/// so a late event at worst causes a spurious wake the waiter's state
/// machine absorbs.
fiber_pool: std.atomic.Value(?*Fiber),

/// Empirically saw >128KB being used by the self-hosted backend to panic.
const idle_stack_size = 256 * 1024;

const max_idle_search = 4;
const max_steal_ready_search = 4;
const max_iovecs_len = 8;

const changes_buffer_len = 64;

const Thread = struct {
    thread: std.Thread,
    idle_context: Io.fiber.Context,
    current_context: *Io.fiber.Context,
    ready_queue: ?*Fiber,
    kq_fd: posix.fd_t,
    idle_search_index: u32,
    steal_ready_search_index: u32,
    /// For ensuring multiple fibers waiting on the same file descriptor and
    /// filter use the same kevent.
    wait_queues: std.array_hash_map.Auto(WaitQueueKey, *Fiber),

    const WaitQueueKey = struct {
        ident: usize,
        filter: i32,
    };

    const canceling: ?*Thread = @ptrFromInt(@alignOf(Thread));

    threadlocal var self: *Thread = undefined;

    fn current() *Thread {
        return self;
    }

    fn currentFiber(thread: *Thread) *Fiber {
        return @fieldParentPtr("context", thread.current_context);
    }

    const List = struct {
        allocated: []Thread,
        reserved: u32,
        active: u32,
    };

    fn deinit(thread: *Thread, gpa: Allocator) void {
        closeFd(thread.kq_fd);
        assert(thread.wait_queues.count() == 0);
        thread.wait_queues.deinit(gpa);
        thread.* = undefined;
    }
};

const Fiber = struct {
    required_align: void align(4),
    context: Io.fiber.Context,
    awaiter: ?*Fiber,
    queue_next: ?*Fiber,
    cancel_thread: ?*Thread,
    awaiting_completions: std.bit_set.Static(3),
    /// The fiber's side of a batched wait; lives here so events that arrive
    /// late (after the wait completed or was cancelled) reference memory
    /// that is stable for the fiber's whole life.
    batch_waiter: BatchWaiter,

    const finished: ?*Fiber = @ptrFromInt(@alignOf(Thread));

    const max_result_align: Alignment = .@"16";
    const max_result_size = max_result_align.forward(64);
    /// This includes any stack realignments that need to happen, and also the
    /// initial frame return address slot and argument frame, depending on target.
    const min_stack_size = 4 * 1024 * 1024;
    const max_context_align: Alignment = .@"16";
    const max_context_size = max_context_align.forward(1024);
    const max_closure_size: usize = @sizeOf(AsyncClosure);
    const max_closure_align: Alignment = .of(AsyncClosure);
    const allocation_size = std.mem.alignForward(
        usize,
        max_closure_align.max(max_context_align).forward(
            max_result_align.forward(@sizeOf(Fiber)) + max_result_size + min_stack_size,
        ) + max_closure_size + max_context_size,
        std.heap.page_size_max,
    );

    fn allocate(k: *Kqueue) error{OutOfMemory}!*Fiber {
        var head = k.fiber_pool.load(.acquire);
        while (head) |fiber| {
            const next = fiber.queue_next;
            if (k.fiber_pool.cmpxchgWeak(head, next, .acquire, .acquire)) |actual| {
                head = actual;
                continue;
            }
            fiber.queue_next = null;
            return fiber;
        }
        return @ptrCast(try k.gpa.alignedAlloc(u8, .of(Fiber), allocation_size));
    }

    fn allocatedSlice(f: *Fiber) []align(@alignOf(Fiber)) u8 {
        return @as([*]align(@alignOf(Fiber)) u8, @ptrCast(f))[0..allocation_size];
    }

    fn allocatedEnd(f: *Fiber) [*]u8 {
        const allocated_slice = f.allocatedSlice();
        return allocated_slice[allocated_slice.len..].ptr;
    }

    fn resultPointer(f: *Fiber, comptime Result: type) *Result {
        return @ptrCast(@alignCast(f.resultBytes(.of(Result))));
    }

    fn resultBytes(f: *Fiber, alignment: Alignment) [*]u8 {
        return @ptrFromInt(alignment.forward(@intFromPtr(f) + @sizeOf(Fiber)));
    }

    fn enterCancelRegion(fiber: *Fiber, thread: *Thread) error{Canceled}!void {
        if (@cmpxchgStrong(
            ?*Thread,
            &fiber.cancel_thread,
            null,
            thread,
            .acq_rel,
            .acquire,
        )) |cancel_thread| {
            assert(cancel_thread == Thread.canceling);
            return error.Canceled;
        }
    }

    fn exitCancelRegion(fiber: *Fiber, thread: *Thread) void {
        if (@cmpxchgStrong(
            ?*Thread,
            &fiber.cancel_thread,
            thread,
            null,
            .acq_rel,
            .acquire,
        )) |cancel_thread| assert(cancel_thread == Thread.canceling);
    }

    const Queue = struct { head: *Fiber, tail: *Fiber };
};

fn recycle(k: *Kqueue, fiber: *Fiber) void {
    std.log.debug("recyling {*}", .{fiber});
    assert(fiber.queue_next == null);
    // The `.recycle` switch task runs on the destination thread, so the
    // pool is a lock-free stack.
    var head = k.fiber_pool.load(.monotonic);
    while (true) {
        fiber.queue_next = head;
        if (k.fiber_pool.cmpxchgWeak(head, fiber, .release, .monotonic) == null) return;
        head = k.fiber_pool.load(.monotonic);
    }
}

pub const InitOptions = struct {
    n_threads: ?usize = null,
};

pub const InitError = Allocator.Error || CreateFileDescriptorError;

pub fn init(k: *Kqueue, gpa: Allocator, options: InitOptions) !void {
    assert(options.n_threads != 0);

    const n_threads = @max(1, options.n_threads orelse std.Thread.getCpuCount() catch 1);
    const threads_size = n_threads * @sizeOf(Thread);
    const idle_stack_end_offset = std.mem.alignForward(usize, threads_size + idle_stack_size, std.heap.page_size_max);
    const allocated_slice = try gpa.alignedAlloc(u8, .of(Thread), idle_stack_end_offset);
    errdefer gpa.free(allocated_slice);
    k.* = .{
        .gpa = gpa,
        .mutex = .init,
        .main_fiber_buffer = undefined,
        .fiber_pool = .init(null),
        .threads = .{
            .allocated = @ptrCast(allocated_slice[0..threads_size]),
            .reserved = 1,
            .active = 1,
        },
    };
    const main_fiber: *Fiber = @ptrCast(&k.main_fiber_buffer);
    main_fiber.* = .{
        .required_align = {},
        .context = undefined,
        .awaiter = null,
        .queue_next = null,
        .cancel_thread = null,
        .awaiting_completions = .empty, .batch_waiter = .{},
    };
    const main_thread = &k.threads.allocated[0];
    Thread.self = main_thread;
    const idle_stack_end: [*]align(16) usize = @ptrCast(@alignCast(allocated_slice[idle_stack_end_offset..].ptr));
    (idle_stack_end - 1)[0..1].* = .{@intFromPtr(k)};
    main_thread.* = .{
        .thread = undefined,
        .idle_context = switch (builtin.cpu.arch) {
            .aarch64 => .{
                .sp = @intFromPtr(idle_stack_end),
                .fp = 0,
                .pc = @intFromPtr(&mainIdleEntry),
            },
            .x86_64 => .{
                .rsp = @intFromPtr(idle_stack_end - 1),
                .rbp = 0,
                .rip = @intFromPtr(&mainIdleEntry),
            },
            else => @compileError("unimplemented architecture"),
        },
        .current_context = &main_fiber.context,
        .ready_queue = null,
        .kq_fd = try createFileDescriptor(),
        .idle_search_index = 1,
        .steal_ready_search_index = 1,
        .wait_queues = .empty,
    };
    errdefer closeFd(main_thread.kq_fd);
    registerWakeupEvent(main_thread.kq_fd);
    std.log.debug("created main idle {*}", .{&main_thread.idle_context});
    std.log.debug("created main {*}", .{main_fiber});
}

pub fn deinit(k: *Kqueue) void {
    const active_threads = @atomicLoad(u32, &k.threads.active, .acquire);
    for (k.threads.allocated[0..active_threads]) |*thread| {
        const ready_fiber = @atomicLoad(?*Fiber, &thread.ready_queue, .monotonic);
        assert(ready_fiber == null or ready_fiber == Fiber.finished); // pending async
    }
    // A worker erases its own `Thread` struct as it exits (`threadEntry`
    // calls `Thread.deinit`), so the join handles are snapshotted before
    // the exit signal; afterwards the structs may already be garbage.
    const gpa = k.gpa;
    const join_handles = gpa.alloc(std.Thread, active_threads) catch @panic("OOM joining kqueue threads");
    defer gpa.free(join_handles);
    for (k.threads.allocated[0..active_threads], join_handles) |*thread, *handle| handle.* = thread.thread;
    k.yield(null, .exit);
    const main_thread = &k.threads.allocated[0];
    while (k.fiber_pool.load(.acquire)) |fiber| {
        k.fiber_pool.store(fiber.queue_next, .monotonic);
        gpa.free(fiber.allocatedSlice());
    }
    main_thread.deinit(gpa);
    const allocated_ptr: [*]align(@alignOf(Thread)) u8 = @ptrCast(@alignCast(k.threads.allocated.ptr));
    const idle_stack_end_offset = std.mem.alignForward(usize, k.threads.allocated.len * @sizeOf(Thread) + idle_stack_size, std.heap.page_size_max);
    for (join_handles[1..active_threads]) |handle| handle.join();
    gpa.free(allocated_ptr[0..idle_stack_end_offset]);
    k.* = undefined;
}

pub const CreateFileDescriptorError = error{
    /// The per-process limit on the number of open file descriptors has been reached.
    ProcessFdQuotaExceeded,
    /// The system-wide limit on the total number of open files has been reached.
    SystemFdQuotaExceeded,
} || Io.UnexpectedError;

pub fn createFileDescriptor() CreateFileDescriptorError!posix.fd_t {
    const rc = posix.system.kqueue();
    switch (posix.errno(rc)) {
        .SUCCESS => return @intCast(rc),
        .MFILE => return error.ProcessFdQuotaExceeded,
        .NFILE => return error.SystemFdQuotaExceeded,
        else => |err| return posix.unexpectedErrno(err),
    }
}

/// Registers the thread's persistent EVFILT_USER event. FreeBSD 15 does
/// not deliver an event whose EV_ADD and NOTE_TRIGGER arrive in the same
/// kevent call, so the wakeup and exit paths register the knote once here
/// and submit trigger-only changes afterwards.
fn registerWakeupEvent(kq_fd: posix.fd_t) void {
    const changes = [_]posix.Kevent{
        .{
            .ident = 0,
            .filter = std.c.EVFILT.USER,
            .flags = std.c.EV.ADD,
            .fflags = 0,
            .data = 0,
            .udata = @backingInt(Completion.UserData.wakeup),
        },
    };
    assert(0 == (kevent(kq_fd, &changes, &.{}, null) catch |err| {
        // TODO handle EINTR for cancellation purposes
        @panic(@errorName(err)); // TODO
    }));
}

/// Submits a trigger-only change for the thread's persistent EVFILT_USER
/// event. The change's udata replaces the registered knote's.
fn triggerWakeupEvent(kq_fd: posix.fd_t, udata: usize) void {
    const changes = [_]posix.Kevent{
        .{
            .ident = 0,
            .filter = std.c.EVFILT.USER,
            .flags = 0,
            .fflags = std.c.NOTE.TRIGGER,
            .data = 0,
            .udata = udata,
        },
    };
    _ = kevent(kq_fd, &changes, &.{}, null) catch {};
}

fn findReadyFiber(k: *Kqueue, thread: *Thread) ?*Fiber {
    if (@atomicRmw(?*Fiber, &thread.ready_queue, .Xchg, Fiber.finished, .acquire)) |ready_fiber| {
        @atomicStore(?*Fiber, &thread.ready_queue, ready_fiber.queue_next, .release);
        ready_fiber.queue_next = null;
        return ready_fiber;
    }
    const active_threads = @atomicLoad(u32, &k.threads.active, .acquire);
    for (0..@min(max_steal_ready_search, active_threads)) |_| {
        defer thread.steal_ready_search_index += 1;
        if (thread.steal_ready_search_index == active_threads) thread.steal_ready_search_index = 0;
        const steal_ready_search_thread = &k.threads.allocated[0..active_threads][thread.steal_ready_search_index];
        if (steal_ready_search_thread == thread) continue;
        const ready_fiber = @atomicLoad(?*Fiber, &steal_ready_search_thread.ready_queue, .acquire) orelse continue;
        if (ready_fiber == Fiber.finished) continue;
        if (@cmpxchgWeak(
            ?*Fiber,
            &steal_ready_search_thread.ready_queue,
            ready_fiber,
            null,
            .acquire,
            .monotonic,
        )) |_| continue;
        @atomicStore(?*Fiber, &thread.ready_queue, ready_fiber.queue_next, .release);
        ready_fiber.queue_next = null;
        return ready_fiber;
    }
    // couldn't find anything to do, so we are now open for business
    @atomicStore(?*Fiber, &thread.ready_queue, null, .monotonic);
    return null;
}

fn yield(k: *Kqueue, maybe_ready_fiber: ?*Fiber, pending_task: SwitchMessage.PendingTask) void {
    const thread: *Thread = .current();
    const ready_context = if (maybe_ready_fiber orelse k.findReadyFiber(thread)) |ready_fiber|
        &ready_fiber.context
    else
        &thread.idle_context;
    const message: SwitchMessage = .{
        .contexts = .{
            .old = thread.current_context,
            .new = ready_context,
        },
        .pending_task = pending_task,
    };
    std.log.debug("switching from {*} to {*}", .{ message.contexts.old, message.contexts.new });
    contextSwitch(&message).handle(k);
}

fn schedule(k: *Kqueue, thread: *Thread, ready_queue: Fiber.Queue) void {
    {
        var fiber = ready_queue.head;
        while (true) {
            std.log.debug("scheduling {*}", .{fiber});
            fiber = fiber.queue_next orelse break;
        }
        assert(fiber == ready_queue.tail);
    }
    // shared fields of previous `Thread` must be initialized before later ones are marked as active
    const new_thread_index = @atomicLoad(u32, &k.threads.active, .acquire);
    for (0..@min(max_idle_search, new_thread_index)) |_| {
        defer thread.idle_search_index += 1;
        if (thread.idle_search_index == new_thread_index) thread.idle_search_index = 0;
        const idle_search_thread = &k.threads.allocated[0..new_thread_index][thread.idle_search_index];
        if (idle_search_thread == thread) continue;
        if (@cmpxchgWeak(
            ?*Fiber,
            &idle_search_thread.ready_queue,
            null,
            ready_queue.head,
            .release,
            .monotonic,
        )) |_| continue;
        // If an error occurs it only pessimises scheduling.
        triggerWakeupEvent(idle_search_thread.kq_fd, @backingInt(Completion.UserData.wakeup));
        return;
    }
    spawn_thread: {
        // previous failed reservations must have completed before retrying
        if (new_thread_index == k.threads.allocated.len or @cmpxchgWeak(
            u32,
            &k.threads.reserved,
            new_thread_index,
            new_thread_index + 1,
            .acquire,
            .monotonic,
        ) != null) break :spawn_thread;
        const new_thread = &k.threads.allocated[new_thread_index];
        const next_thread_index = new_thread_index + 1;
        new_thread.* = .{
            .thread = undefined,
            .idle_context = undefined,
            .current_context = &new_thread.idle_context,
            .ready_queue = ready_queue.head,
            .kq_fd = createFileDescriptor() catch |err| {
                @atomicStore(u32, &k.threads.reserved, new_thread_index, .release);
                // no more access to `thread` after giving up reservation
                std.log.warn("unable to create worker thread due to kqueue init failure: {t}", .{err});
                break :spawn_thread;
            },
            .idle_search_index = 0,
            .steal_ready_search_index = 0,
            .wait_queues = .empty,
        };
        registerWakeupEvent(new_thread.kq_fd);
        new_thread.thread = std.Thread.spawn(.{
            .stack_size = idle_stack_size,
            .allocator = k.gpa,
        }, threadEntry, .{ k, new_thread_index }) catch |err| {
            closeFd(new_thread.kq_fd);
            @atomicStore(u32, &k.threads.reserved, new_thread_index, .release);
            // no more access to `thread` after giving up reservation
            std.log.warn("unable to create worker thread due spawn failure: {s}", .{@errorName(err)});
            break :spawn_thread;
        };
        // shared fields of `Thread` must be initialized before being marked active
        @atomicStore(u32, &k.threads.active, next_thread_index, .release);
        return;
    }
    // nobody wanted it, so just queue it on ourselves
    while (@cmpxchgWeak(
        ?*Fiber,
        &thread.ready_queue,
        ready_queue.tail.queue_next,
        ready_queue.head,
        .acq_rel,
        .acquire,
    )) |old_head| ready_queue.tail.queue_next = old_head;
}

fn mainIdle(k: *Kqueue, message: *const SwitchMessage) callconv(.withStackAlign(.c, @max(@alignOf(Thread), @alignOf(Io.fiber.Context)))) noreturn {
    message.handle(k);
    k.idle(&k.threads.allocated[0]);
    k.yield(@ptrCast(&k.main_fiber_buffer), .nothing);
    unreachable; // switched to dead fiber
}

fn threadEntry(k: *Kqueue, index: u32) void {
    const thread: *Thread = &k.threads.allocated[index];
    Thread.self = thread;
    std.log.debug("created thread idle {*}", .{&thread.idle_context});
    k.idle(thread);
    thread.deinit(k.gpa);
}

/// Backend state for an `Io.Group`. The awaiter owns this memory: member
/// fibers only read it, and the last one swaps the `finished` sentinel
/// into `awaiter` (the same handshake `Future.await` uses), so a group
/// await that is registering concurrently either finds the sentinel in
/// its own switch task or is woken by the exiting member. `await`
/// destroys the state after it is woken; `cancel` runs the same path.
///
/// There is no per-fiber cancellation on this backend yet, so `cancel`
/// waits for the members to run to completion, like the Evented network
/// waits elsewhere (no cancellation points).
const GroupState = struct {
    token: *Io.Group,
    members: std.atomic.Value(usize),
    awaiter: ?*Fiber,

    const finished: ?*Fiber = Fiber.finished;
};

/// Tag bit in kevent `udata` distinguishing a batch waiter from a fiber
/// pointer (both at least 4-aligned).
const batch_userdata_tag: usize = 1;

/// The waiting fiber's side of a batched await, using the same
/// register_awaiter handshake `Future.await` uses, which makes stale
/// events harmless: an event swaps `parked` to the `finished` sentinel
/// and schedules the fiber only when it was actually parked, and the
/// fiber parks by swapping itself into the slot from the switch task
/// (scheduling itself immediately when a wake already landed). A late
/// event for an earlier wait of the same fiber therefore causes at most
/// a spurious wake — the fiber re-drains its submitted operations
/// nonblocking and re-parks — and can never schedule a running fiber.
///
/// `kq_fd` records where this wait's kevents were registered; after a
/// work-stealing migration the fiber's deletes must target that kq, not
/// its current one.
///
/// The wake side swaps the `finished` sentinel into `parked` and
/// schedules the fiber only when it took the fiber from the slot; the
/// park side claims the slot from empty with a CAS in the switch task,
/// scheduling itself when a wake already landed. Several events can
/// therefore arrive for one wait (a readiness plus the timer plus stale
/// registrations from before a migration) and exactly one wake happens.
///
/// REMAINING KNOWN CRASH, deeper than this waiter: with one server and
/// two client fibers on a shared instance (`--loops 1 --clients 2`), the
/// process faults in `findReadyFiber` reading `ready_fiber.queue_next`
/// with the queue's own lock sentinel (8) as the head — the ready-queue
/// lock state leaks between operations somewhere in the scheduler. Both
/// this waiter and its predecessor (armed/fired CAS) reproduce it, at
/// the same site, while `ev-thread` (one instance per thread) does not;
/// the single-fiber and suite shapes pass. Until the scheduler bug is
/// found, prefer one Kqueue instance per thread.
const BatchWaiter = struct {
    /// null while the fiber runs; the fiber while it is parked; the
    /// `finished` sentinel once an event has claimed the wake.
    parked: ?*Fiber = null,
    /// Absolute deadline on the awake clock, when the wait is timed.
    /// Nanoseconds; i64 keeps `Fiber`'s alignment unchanged.
    when_ns: ?i64 = null,
    /// The kqueue descriptor this wait's kevents were registered on.
    kq_fd: posix.fd_t = -1,
};

const Completion = struct {
    const UserData = enum(usize) {
        unused,
        wakeup,
        cleanup,
        exit,
        /// *Fiber
        _,
    };
    /// Corresponds to Kevent field.
    flags: u16,
    /// Corresponds to Kevent field.
    fflags: u32,
    /// Corresponds to Kevent field.
    data: isize,
};

fn idle(k: *Kqueue, thread: *Thread) void {
    var events_buffer: [changes_buffer_len]posix.Kevent = undefined;
    var maybe_ready_fiber: ?*Fiber = null;
    while (true) {
        while (maybe_ready_fiber orelse k.findReadyFiber(thread)) |ready_fiber| {
            k.yield(ready_fiber, .nothing);
            maybe_ready_fiber = null;
        }
        const n = kevent(thread.kq_fd, &.{}, &events_buffer, null) catch |err| {
            // TODO handle EINTR for cancellation purposes
            @panic(@errorName(err)); // TODO
        };
        var maybe_ready_queue: ?Fiber.Queue = null;
        for (events_buffer[0..n]) |event| switch (@as(Completion.UserData, @fromBackingInt(@intCast(event.udata)))) {
            .unused => unreachable, // bad submission queued?
            .wakeup => {},
            .cleanup => @panic("failed to notify other threads that we are exiting"),
            .exit => {
                assert(maybe_ready_fiber == null and maybe_ready_queue == null); // pending async
                return;
            },
            _ => {
                if (event.udata & batch_userdata_tag != 0) {
                    // A batched operation (or its timer) fired. The fiber
                    // performs all bookkeeping after it wakes; here it is
                    // only woken, and only if it was parked.
                    const waiter: *BatchWaiter = @ptrFromInt(event.udata & ~batch_userdata_tag);
                    const parked = @atomicRmw(?*Fiber, &waiter.parked, .Xchg, Fiber.finished, .acq_rel);
                    if (parked) |ready_fiber| {
                        if (ready_fiber == Fiber.finished) continue;
                        if (maybe_ready_fiber == null) {
                            maybe_ready_fiber = ready_fiber;
                        } else if (maybe_ready_queue) |*ready_queue| {
                            ready_queue.tail.queue_next = ready_fiber;
                            ready_queue.tail = ready_fiber;
                        } else {
                            maybe_ready_queue = .{ .head = ready_fiber, .tail = ready_fiber };
                        }
                    }
                    continue;
                }
                const event_head_fiber: *Fiber = @ptrFromInt(event.udata);
                const event_tail_fiber = thread.wait_queues.fetchSwapRemove(.{
                    .ident = event.ident,
                    .filter = event.filter,
                }).?.value;
                assert(event_tail_fiber.queue_next == null);

                // TODO reevaluate this logic
                event_head_fiber.resultPointer(Completion).* = .{
                    .flags = event.flags,
                    .fflags = event.fflags,
                    .data = event.data,
                };

                queue_ready: {
                    const head: *Fiber = if (maybe_ready_fiber == null) f: {
                        maybe_ready_fiber = event_head_fiber;
                        const next = event_head_fiber.queue_next orelse break :queue_ready;
                        event_head_fiber.queue_next = null;
                        break :f next;
                    } else event_head_fiber;

                    if (maybe_ready_queue) |*ready_queue| {
                        ready_queue.tail.queue_next = head;
                        ready_queue.tail = event_tail_fiber;
                    } else {
                        maybe_ready_queue = .{ .head = head, .tail = event_tail_fiber };
                    }
                }
            },
        };
        if (maybe_ready_queue) |ready_queue| k.schedule(thread, ready_queue);
    }
}

const SwitchMessage = struct {
    contexts: Io.fiber.Switch,
    pending_task: PendingTask,

    const PendingTask = union(enum) {
        nothing,
        reschedule,
        recycle: *Fiber,
        register_awaiter: *?*Fiber,
        /// Parks the switching fiber in a batch waiter slot. Unlike
        /// `register_awaiter`, this only claims the slot when it is empty:
        /// a wake that landed between arming the kevents and the switch
        /// has already left the `finished` sentinel, and the fiber must
        /// schedule itself — without overwriting the sentinel, which later
        /// stale events must keep seeing.
        register_batch_waiter: *?*Fiber,
        exit,
    };

    fn handle(message: *const SwitchMessage, k: *Kqueue) void {
        const thread: *Thread = .current();
        thread.current_context = message.contexts.new;
        switch (message.pending_task) {
            .nothing => {},
            .reschedule => if (message.contexts.old != &thread.idle_context) {
                const prev_fiber: *Fiber = @alignCast(@fieldParentPtr("context", message.contexts.old));
                assert(prev_fiber.queue_next == null);
                k.schedule(thread, .{ .head = prev_fiber, .tail = prev_fiber });
            },
            .recycle => |fiber| {
                k.recycle(fiber);
            },
            .register_awaiter => |awaiter| {
                const prev_fiber: *Fiber = @alignCast(@fieldParentPtr("context", message.contexts.old));
                assert(prev_fiber.queue_next == null);
                if (@atomicRmw(?*Fiber, awaiter, .Xchg, prev_fiber, .acq_rel) == Fiber.finished)
                    k.schedule(thread, .{ .head = prev_fiber, .tail = prev_fiber });
            },
            .register_batch_waiter => |slot| {
                const prev_fiber: *Fiber = @alignCast(@fieldParentPtr("context", message.contexts.old));
                assert(prev_fiber.queue_next == null);
                if (@cmpxchgStrong(
                    ?*Fiber,
                    slot,
                    null,
                    prev_fiber,
                    .acq_rel,
                    .acquire,
                ) != null) {
                    // A wake already landed; the sentinel stays so stale
                    // events keep no-op'ing.
                    k.schedule(thread, .{ .head = prev_fiber, .tail = prev_fiber });
                }
            },
            .exit => {
                for (k.threads.allocated[0..@atomicLoad(u32, &k.threads.active, .acquire)]) |*each_thread| {
                    triggerWakeupEvent(each_thread.kq_fd, @backingInt(Completion.UserData.exit));
                }
            },
        }
    }
};

inline fn contextSwitch(message: *const SwitchMessage) *const SwitchMessage {
    return @fieldParentPtr("contexts", Io.fiber.contextSwitch(&message.contexts));
}

fn mainIdleEntry() callconv(.naked) void {
    switch (builtin.cpu.arch) {
        .x86_64 => asm volatile (
            \\ movq (%%rsp), %%rdi
            \\ jmp %[mainIdle:P]
            :
            : [mainIdle] "X" (&mainIdle),
        ),
        .aarch64 => asm volatile (
            \\ ldr x0, [sp, #-8]
            \\ b %[mainIdle]
            :
            : [mainIdle] "X" (&mainIdle),
        ),
        else => |arch| @compileError("unimplemented architecture: " ++ @tagName(arch)),
    }
}

fn fiberEntry() callconv(.naked) void {
    switch (builtin.cpu.arch) {
        .x86_64 => asm volatile (
            \\ leaq 8(%%rsp), %%rdi
            \\ jmp %[AsyncClosure_call:P]
            :
            : [AsyncClosure_call] "X" (&AsyncClosure.call),
        ),
        .aarch64 => asm volatile (
            \\ mov x0, sp
            \\ b %[AsyncClosure_call]
            :
            : [AsyncClosure_call] "X" (&AsyncClosure.call),
        ),
        else => |arch| @compileError("unimplemented architecture: " ++ @tagName(arch)),
    }
}

const AsyncClosure = struct {
    kqueue: *Kqueue,
    fiber: *Fiber,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
    result_align: Alignment,
    already_awaited: bool,
    /// When set, this fiber is an `Io.Group` member rather than a future:
    /// `group_start` runs instead of `start`, and the group teardown runs
    /// instead of the awaiter handshake.
    group: ?*GroupState = null,
    group_start: ?*const fn (context: *const anyopaque) void = null,

    fn contextPointer(closure: *AsyncClosure) [*]align(Fiber.max_context_align.toByteUnits()) u8 {
        return @alignCast(@as([*]u8, @ptrCast(closure)) + @sizeOf(AsyncClosure));
    }

    fn call(closure: *AsyncClosure, message: *const SwitchMessage) callconv(.withStackAlign(.c, @alignOf(AsyncClosure))) noreturn {
        message.handle(closure.kqueue);
        const fiber = closure.fiber;
        std.log.debug("{*} performing async", .{fiber});
        if (closure.group) |state| {
            closure.group_start.?(closure.contextPointer());
            return groupFinish(closure.kqueue, fiber, state);
        }
        closure.start(closure.contextPointer(), fiber.resultBytes(closure.result_align));
        const awaiter = @atomicRmw(?*Fiber, &fiber.awaiter, .Xchg, Fiber.finished, .acq_rel);
        const ready_awaiter = r: {
            const a = awaiter orelse break :r null;
            if (@atomicRmw(bool, &closure.already_awaited, .Xchg, true, .acq_rel)) break :r null;
            break :r a;
        };
        closure.kqueue.yield(ready_awaiter, .nothing);
        unreachable; // switched to dead fiber
    }

    fn fromFiber(fiber: *Fiber) *AsyncClosure {
        return @ptrFromInt(Fiber.max_context_align.max(.of(AsyncClosure)).backward(
            @intFromPtr(fiber.allocatedEnd()) - Fiber.max_context_size,
        ) - @sizeOf(AsyncClosure));
    }
};

pub fn io(k: *Kqueue) Io {
    return .{
        .userdata = k,
                .vtable = &.{
            .crashHandler = Io.Threaded.crashHandler,
            .async = async,
            .concurrent = concurrent,
            .await = await,
            .cancel = cancel,
            .groupAsync = groupAsync,
            .groupConcurrent = groupConcurrent,
            .groupAwait = groupAwait,
            .groupCancel = groupCancel,
            .recancel = Io.Threaded.recancel,
            .swapCancelProtection = Io.Threaded.swapCancelProtection,
            .checkCancel = Io.Threaded.checkCancel,
            .futexWait = Io.Threaded.futexWait,
            .futexWaitUncancelable = Io.Threaded.futexWaitUncancelable,
            .futexWake = Io.Threaded.futexWake,
            .operate = operate,
            .batchAwaitAsync = batchAwaitAsync,
            .batchAwaitConcurrent = batchAwaitConcurrent,
            .batchCancel = batchCancel,
            .dirCreateDir = dirCreateDir,
            .dirCreateDirPath = dirCreateDirPath,
            .dirCreateDirPathOpen = dirCreateDirPathOpen,
            .dirOpenDir = dirOpenDir,
            .dirStat = dirStat,
            .dirStatFile = dirStatFile,
            .dirAccess = dirAccess,
            .dirCreateFile = dirCreateFile,
            .dirCreateFileAtomic = Io.Threaded.dirCreateFileAtomic,
            .dirOpenFile = dirOpenFile,
            .dirClose = dirClose,
            .dirRead = Io.noDirRead,  // TODO(kqueue) local impl
            .dirRealPath = Io.Threaded.dirRealPathPosix,
            .dirRealPathFile = Io.Threaded.dirRealPathFilePosix,
            .dirDeleteFile = Io.Threaded.dirDeleteFilePosix,
            .dirDeleteDir = Io.Threaded.dirDeleteDirPosix,
            .dirRename = Io.Threaded.dirRenamePosix,
            .dirRenamePreserve = Io.failingDirRenamePreserve,  // TODO(kqueue) local impl
            .dirSymLink = Io.Threaded.dirSymLinkPosix,
            .dirReadLink = Io.Threaded.dirReadLink,
            .dirSetOwner = Io.Threaded.dirSetOwnerPosix,
            .dirSetFileOwner = Io.Threaded.dirSetFileOwner,
            .dirSetPermissions = Io.Threaded.dirSetPermissionsPosix,
            .dirSetFilePermissions = Io.failingDirSetFilePermissions,  // TODO(kqueue) local impl
            .dirSetTimestamps = Io.Threaded.dirSetTimestamps,
            .dirHardLink = Io.Threaded.dirHardLink,
            .fileStat = fileStat,
            .fileLength = Io.failingFileLength,  // TODO(kqueue) local impl
            .fileClose = fileClose,
            .fileWritePositional = fileWritePositional,
            .fileWriteFileStreaming = Io.noFileWriteFileStreaming,  // TODO(kqueue) local impl
            .fileWriteFilePositional = Io.noFileWriteFilePositional,  // TODO(kqueue) local impl
            .fileReadPositional = fileReadPositional,
            .fileSeekBy = fileSeekBy,
            .fileSeekTo = fileSeekTo,
            .fileSync = Io.Threaded.fileSyncPosix,
            .fileIsTty = Io.unreachableFileIsTty,  // TODO(kqueue) local impl
            .fileEnableAnsiEscapeCodes = Io.unreachableFileEnableAnsiEscapeCodes,  // TODO(kqueue) local impl
            .fileSupportsAnsiEscapeCodes = Io.unreachableFileSupportsAnsiEscapeCodes,  // TODO(kqueue) local impl
            .fileSetLength = Io.Threaded.fileSetLength,
            .fileSetOwner = Io.failingFileSetOwner,  // TODO(kqueue) local impl
            .fileSetPermissions = Io.Threaded.fileSetPermissions,
            .fileSetTimestamps = Io.Threaded.fileSetTimestamps,
            .fileLock = Io.Threaded.fileLock,
            .fileTryLock = Io.Threaded.fileTryLock,
            .fileUnlock = Io.Threaded.fileUnlock,
            .fileDowngradeLock = Io.Threaded.fileDowngradeLock,
            .fileRealPath = Io.Threaded.fileRealPathPosix,
            .fileHardLink = Io.Threaded.fileHardLink,
            .fileMemoryMapCreate = Io.failingFileMemoryMapCreate,  // TODO(kqueue) local impl
            .fileMemoryMapDestroy = Io.unreachableFileMemoryMapDestroy,  // TODO(kqueue) local impl
            .fileMemoryMapSetLength = Io.unreachableFileMemoryMapSetLength,  // TODO(kqueue) local impl
            .fileMemoryMapRead = Io.Threaded.fileMemoryMapRead,
            .fileMemoryMapWrite = Io.Threaded.fileMemoryMapWrite,
            .processExecutableOpen = Io.failingProcessExecutableOpen,  // TODO(kqueue) local impl
            .processExecutablePath = Io.failingProcessExecutablePath,  // TODO(kqueue) local impl
            .lockStderr = Io.unreachableLockStderr,  // TODO(kqueue) local impl
            .tryLockStderr = Io.noTryLockStderr,  // TODO(kqueue) local impl
            .unlockStderr = Io.unreachableUnlockStderr,  // TODO(kqueue) local impl
            .processCurrentPath = Io.failingProcessCurrentPath,  // TODO(kqueue) local impl
            .processSetCurrentDir = Io.Threaded.processSetCurrentDir,
            .processSetCurrentPath = Io.Threaded.processSetCurrentPath,
            .processReplace = Io.failingProcessReplace,  // TODO(kqueue) local impl
            .processReplacePath = Io.Threaded.processReplacePath,
            .processSpawn = Io.Threaded.processSpawnPosix,
            .processSpawnPath = Io.Threaded.processSpawnPath,
            .childWait = Io.Threaded.childWait,
            .childKill = Io.unreachableChildKill,  // TODO(kqueue) local impl
            .progressParentFile = Io.failingProgressParentFile,  // TODO(kqueue) local impl
            .now = now,
            .clockResolution = Io.failingClockResolution,  // TODO(kqueue) local impl
            .sleep = sleep,
            .random = Io.noRandom,  // TODO(kqueue) local impl
            .randomSecure = Io.Threaded.randomSecure,
            .netListenIp = netListenIp,
            .netAccept = netAccept,
            .netBindIp = netBindIp,
            .netConnectIp = netConnectIp,
            .netListenUnix = netListenUnix,
            .netConnectUnix = netConnectUnix,
            .netSocketCreatePair = Io.failingNetSocketCreatePair,
            .netWriteFile = Io.failingNetWriteFile,
            .netClose = netClose,
            .netShutdown = netShutdown,
            .netInterfaceNameResolve = netInterfaceNameResolve,
            .netInterfaceName = netInterfaceName,
            .netLookup = netLookup,
        },
    };
}

fn async(
    userdata: ?*anyopaque,
    result: []u8,
    result_alignment: std.mem.Alignment,
    context: []const u8,
    context_alignment: std.mem.Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) ?*Io.AnyFuture {
    return concurrent(userdata, result.len, result_alignment, context, context_alignment, start) catch {
        start(context.ptr, result.ptr);
        return null;
    };
}

fn concurrent(
    userdata: ?*anyopaque,
    result_len: usize,
    result_alignment: Alignment,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque, result: *anyopaque) void,
) Io.ConcurrentError!*Io.AnyFuture {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    assert(result_alignment.compare(.lte, Fiber.max_result_align)); // TODO
    assert(context_alignment.compare(.lte, Fiber.max_context_align)); // TODO
    assert(result_len <= Fiber.max_result_size); // TODO
    assert(context.len <= Fiber.max_context_size); // TODO

    const fiber = Fiber.allocate(k) catch return error.ConcurrencyUnavailable;
    std.log.debug("allocated {*}", .{fiber});

    const closure: *AsyncClosure = .fromFiber(fiber);
    fiber.* = .{
        .required_align = {},
        .context = switch (builtin.cpu.arch) {
            .x86_64 => .{
                .rsp = @intFromPtr(closure) - @sizeOf(usize),
                .rbp = 0,
                .rip = @intFromPtr(&fiberEntry),
            },
            .aarch64 => .{
                .sp = @intFromPtr(closure),
                .fp = 0,
                .pc = @intFromPtr(&fiberEntry),
            },
            else => |arch| @compileError("unimplemented architecture: " ++ @tagName(arch)),
        },
        .awaiter = null,
        .queue_next = null,
        .cancel_thread = null,
        .awaiting_completions = .empty, .batch_waiter = .{},
    };
    closure.* = .{
        .kqueue = k,
        .fiber = fiber,
        .start = start,
        .result_align = result_alignment,
        .already_awaited = false,
    };
    @memcpy(closure.contextPointer(), context);

    k.schedule(.current(), .{ .head = fiber, .tail = fiber });
    return @ptrCast(fiber);
}

fn await(
    userdata: ?*anyopaque,
    any_future: *Io.AnyFuture,
    result: []u8,
    result_alignment: std.mem.Alignment,
) void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const future_fiber: *Fiber = @ptrCast(@alignCast(any_future));
    if (@atomicLoad(?*Fiber, &future_fiber.awaiter, .acquire) != Fiber.finished)
        k.yield(null, .{ .register_awaiter = &future_fiber.awaiter });
    @memcpy(result, future_fiber.resultBytes(result_alignment));
    k.recycle(future_fiber);
}

fn cancel(
    userdata: ?*anyopaque,
    any_future: *Io.AnyFuture,
    result: []u8,
    result_alignment: std.mem.Alignment,
) void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = any_future;
    _ = result;
    _ = result_alignment;
    @panic("TODO");
}

fn cancelRequested(userdata: ?*anyopaque) bool {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    return false; // TODO
}

fn groupAsync(
    userdata: ?*anyopaque,
    type_erased: *Io.Group,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque) void,
) void {
    groupConcurrent(userdata, type_erased, context, context_alignment, start) catch {
        start(context.ptr);
    };
}

fn groupConcurrent(
    userdata: ?*anyopaque,
    type_erased: *Io.Group,
    context: []const u8,
    context_alignment: Alignment,
    start: *const fn (context: *const anyopaque) void,
) Io.ConcurrentError!void {
    assert(context_alignment.compare(.lte, Fiber.max_context_align)); // TODO
    assert(context.len <= Fiber.max_context_size); // TODO
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const state: *GroupState = s: {
        if (type_erased.token.load(.acquire)) |token| break :s @ptrCast(@alignCast(token));
        const created = k.gpa.create(GroupState) catch return error.ConcurrencyUnavailable;
        created.* = .{ .token = type_erased, .members = .init(0), .awaiter = null };
        if (type_erased.token.cmpxchgStrong(null, created, .acq_rel, .acquire)) |existing| {
            k.gpa.destroy(created);
            break :s @ptrCast(@alignCast(existing));
        }
        break :s created;
    };
    _ = state.members.fetchAdd(1, .monotonic);
    errdefer _ = state.members.fetchSub(1, .monotonic);
    const fiber = Fiber.allocate(k) catch return error.ConcurrencyUnavailable;
    const closure: *AsyncClosure = .fromFiber(fiber);
    fiber.* = .{
        .required_align = {},
        .context = switch (builtin.cpu.arch) {
            .x86_64 => .{
                .rsp = @intFromPtr(closure) - @sizeOf(usize),
                .rbp = 0,
                .rip = @intFromPtr(&fiberEntry),
            },
            .aarch64 => .{
                .sp = @intFromPtr(closure),
                .fp = 0,
                .pc = @intFromPtr(&fiberEntry),
            },
            else => |arch| @compileError("unimplemented architecture: " ++ @tagName(arch)),
        },
        .awaiter = null,
        .queue_next = null,
        .cancel_thread = null,
        .awaiting_completions = .empty,
        .batch_waiter = .{},
    };
    closure.* = .{
        .kqueue = k,
        .fiber = fiber,
        .start = undefined,
        .result_align = .@"1",
        .already_awaited = false,
        .group = state,
        .group_start = start,
    };
    @memcpy(closure.contextPointer(), context);

    k.schedule(.current(), .{ .head = fiber, .tail = fiber });
}

/// The last act of a group member fiber: count itself out and, when it is
/// the last member, hand the group's completion to the awaiting fiber (or
/// leave the sentinel for an await that registers later). The fiber then
/// recycles itself on the destination thread's switch task.
fn groupFinish(k: *Kqueue, fiber: *Fiber, state: *GroupState) noreturn {
    const old = state.members.fetchSub(1, .acq_rel);
    if (old == 1) {
        const parked = @atomicRmw(?*Fiber, &state.awaiter, .Xchg, GroupState.finished, .acq_rel);
        if (parked) |awaiter| {
            k.yield(awaiter, .{ .recycle = fiber });
        }
    }
    k.yield(null, .{ .recycle = fiber });
    unreachable; // switched to dead fiber
}

fn groupAwait(userdata: ?*anyopaque, type_erased: *Io.Group, initial_token: *anyopaque) Io.Cancelable!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const state: *GroupState = @ptrCast(@alignCast(initial_token));
    // The register_awaiter switch task swaps this fiber into the slot, or
    // finds `finished` (set by the last member) and schedules it directly;
    // the exiting member that finds the fiber in the slot schedules it.
    k.yield(null, .{ .register_awaiter = &state.awaiter });
    type_erased.token.store(null, .release);
    k.gpa.destroy(state);
}

fn groupCancel(userdata: ?*anyopaque, group: *Io.Group, token: *anyopaque) void {
    // No per-fiber cancellation on this backend yet: network waits are not
    // cancellation points, so members run to completion (the same posture
    // as the Dispatch backend's network operations). Cancel waits.
    groupAwait(userdata, group, token) catch {};
}

fn dirCreateDir(userdata: ?*anyopaque, dir: Dir, sub_path: []const u8, permissions: Dir.Permissions) Dir.CreateDirError!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = dir;
    _ = sub_path;
    _ = permissions;
    @panic("TODO");
}

fn dirCreateDirPath(
    userdata: ?*anyopaque,
    dir: Dir,
    sub_path: []const u8,
    permissions: Dir.Permissions,
) Dir.CreateDirPathError!Dir.CreatePathStatus {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = dir;
    _ = sub_path;
    _ = permissions;
    @panic("TODO");
}

fn dirCreateDirPathOpen(
    userdata: ?*anyopaque,
    dir: Dir,
    sub_path: []const u8,
    permissions: Dir.Permissions,
    options: Dir.OpenOptions,
) Dir.CreateDirPathOpenError!Dir {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = dir;
    _ = sub_path;
    _ = permissions;
    _ = options;
    @panic("TODO");
}

fn dirStat(userdata: ?*anyopaque, dir: Dir) Dir.StatError!Dir.Stat {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = dir;
    @panic("TODO");
}

fn dirStatFile(
    userdata: ?*anyopaque,
    dir: Dir,
    sub_path: []const u8,
    options: Dir.StatFileOptions,
) Dir.StatFileError!File.Stat {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = dir;
    _ = sub_path;
    _ = options;
    @panic("TODO");
}
fn dirAccess(userdata: ?*anyopaque, dir: Dir, sub_path: []const u8, options: Dir.AccessOptions) Dir.AccessError!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = dir;
    _ = sub_path;
    _ = options;
    @panic("TODO");
}
fn dirCreateFile(userdata: ?*anyopaque, dir: Dir, sub_path: []const u8, flags: Dir.CreateFileOptions) File.OpenError!File {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = dir;
    _ = sub_path;
    _ = flags;
    @panic("TODO");
}
fn dirOpenFile(userdata: ?*anyopaque, dir: Dir, sub_path: []const u8, flags: Dir.OpenFileOptions) File.OpenError!File {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = dir;
    _ = sub_path;
    _ = flags;
    @panic("TODO");
}
fn dirOpenDir(userdata: ?*anyopaque, dir: Dir, sub_path: []const u8, options: Dir.OpenOptions) Dir.OpenError!Dir {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = dir;
    _ = sub_path;
    _ = options;
    @panic("TODO");
}
fn dirClose(userdata: ?*anyopaque, dirs: []const Dir) void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = dirs;
    @panic("TODO");
}
fn fileStat(userdata: ?*anyopaque, file: File) File.StatError!File.Stat {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = file;
    @panic("TODO");
}

fn fileClose(userdata: ?*anyopaque, files: []const File) void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = files;
    @panic("TODO");
}

fn fileWriteStreaming(
    userdata: ?*anyopaque,
    file: File,
    header: []const u8,
    data: []const []const u8,
    splat: usize,
) File.Writer.Error!usize {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = file;
    _ = header;
    _ = data;
    _ = splat;
    @panic("TODO");
}

fn fileWritePositional(
    userdata: ?*anyopaque,
    file: File,
    header: []const u8,
    data: []const []const u8,
    splat: usize,
    offset: u64,
) File.WritePositionalError!usize {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = file;
    _ = header;
    _ = data;
    _ = splat;
    _ = offset;
    @panic("TODO");
}

fn fileReadStreaming(userdata: ?*anyopaque, file: File, data: []const []u8) File.Reader.Error!usize {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = file;
    _ = data;
    @panic("TODO");
}

fn fileReadPositional(userdata: ?*anyopaque, file: File, data: []const []u8, offset: u64) File.ReadPositionalError!usize {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = file;
    _ = data;
    _ = offset;
    @panic("TODO");
}
fn fileSeekBy(userdata: ?*anyopaque, file: File, relative_offset: i64) File.SeekError!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = file;
    _ = relative_offset;
    @panic("TODO");
}
fn fileSeekTo(userdata: ?*anyopaque, file: File, absolute_offset: u64) File.SeekError!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = file;
    _ = absolute_offset;
    @panic("TODO");
}

fn now(userdata: ?*anyopaque, clock: Io.Clock) Io.Timestamp {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    return Io.Threaded.nowPosix(clock);
}

/// Parks the fiber on a one-shot EVFILT_TIMER registered through the same
/// `wait_queues` path the readiness waits use; the timer's ident is the
/// fiber pointer, unique per waiting fiber.
fn sleep(userdata: ?*anyopaque, timeout: Io.Timeout) Io.Cancelable!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const thread: *Thread = .current();
    const fiber = thread.currentFiber();
    const ms: i64 = switch (timeout) {
        .none => return,
        .duration => |duration| @intCast(@max(0, duration.raw.toMilliseconds())),
        .deadline => |deadline| ms: {
            const now_ts = Io.Threaded.nowPosix(deadline.clock);
            const remaining_ns: i128 = @as(i128, deadline.raw.toNanoseconds()) - now_ts.toNanoseconds();
            if (remaining_ns <= 0) break :ms 0;
            break :ms @intCast(@min(@as(i128, std.math.maxInt(i64)), @divTrunc(remaining_ns, std.time.ns_per_ms)));
        },
    };
    const ident: usize = @intFromPtr(fiber);
    const filter = std.c.EVFILT.TIMER;
    const gop = thread.wait_queues.getOrPut(k.gpa, .{
        .ident = ident,
        .filter = filter,
    }) catch {
        // Out of memory for the registration: block the thread the plain
        // way. Other fibers on this worker stall for the duration; this is
        // the never-taken fallback.
        var one_event: [1]posix.Kevent = undefined;
        const ts: posix.timespec = .{
            .sec = @divTrunc(ms, 1000),
            .nsec = @intCast(@mod(ms, 1000) * std.time.ns_per_ms),
        };
        _ = kevent(thread.kq_fd, &.{}, &one_event, &ts) catch {};
        return;
    };
    assert(!gop.found_existing); // one sleep per fiber at a time
    gop.value_ptr.* = fiber;
    const changes = [_]posix.Kevent{
        .{
            .ident = ident,
            .filter = filter,
            .flags = std.c.EV.ADD | std.c.EV.ONESHOT,
            .fflags = 0,
            .data = @max(1, ms),
            .udata = @intFromPtr(fiber),
        },
    };
    assert(0 == (kevent(thread.kq_fd, &changes, &.{}, null) catch |err| {
        // TODO handle EINTR for cancellation purposes
        @panic(@errorName(err)); // TODO
    }));
    yield(k, null, .nothing);
}

fn netListenIp(
    userdata: ?*anyopaque,
    address: *const net.IpAddress,
    options: net.IpAddress.ListenOptions,
) net.IpAddress.ListenError!net.Socket {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = address;
    _ = options;
    @panic("TODO");
}
fn netAccept(userdata: ?*anyopaque, server: net.Socket.Handle, options: net.Server.AcceptOptions) net.Server.AcceptError!net.Socket {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = server;
    _ = options;
    @panic("TODO");
}
fn netBindIp(
    userdata: ?*anyopaque,
    address: *const net.IpAddress,
    options: net.IpAddress.BindOptions,
) net.IpAddress.BindError!net.Socket {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const family = Io.Threaded.posixAddressFamily(address);
    const socket_fd = try openSocketPosix(k, family, options);
    errdefer closeFd(socket_fd);
    if (options.reuse_port) {
        if (comptime !@hasDecl(posix.SO, "REUSEPORT")) return error.OptionUnsupported;
        try setSocketOption(k, socket_fd, posix.SOL.SOCKET, posix.SO.REUSEPORT, 1);
    }
    var storage: Io.Threaded.PosixAddress = undefined;
    var addr_len = Io.Threaded.addressToPosix(address, &storage);
    try posixBind(k, socket_fd, &storage.any, addr_len);
    if (options.allow_broadcast) try setSocketOption(k, socket_fd, posix.SOL.SOCKET, posix.SO.BROADCAST, 1);
    try posixGetSockName(k, socket_fd, &storage.any, &addr_len);
    return .{ .handle = socket_fd, .address = Io.Threaded.addressFromPosix(&storage) };
}
fn netConnectIp(userdata: ?*anyopaque, address: *const net.IpAddress, options: net.IpAddress.ConnectOptions) net.IpAddress.ConnectError!net.Socket {
    if (options.timeout != .none) @panic("TODO");
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const family = Io.Threaded.posixAddressFamily(address);
    const socket_fd = try openSocketPosix(k, family, .{
        .mode = options.mode,
        .protocol = options.protocol,
    });
    errdefer closeFd(socket_fd);
    var storage: Io.Threaded.PosixAddress = undefined;
    var addr_len = Io.Threaded.addressToPosix(address, &storage);
    try posixConnect(k, socket_fd, &storage.any, addr_len);
    try posixGetSockName(k, socket_fd, &storage.any, &addr_len);
    return .{ .handle = socket_fd, .address = Io.Threaded.addressFromPosix(&storage) };
}

fn posixConnect(k: *Kqueue, socket_fd: posix.socket_t, addr: *const posix.sockaddr, addr_len: posix.socklen_t) !void {
    while (true) {
        try k.checkCancel();
        switch (posix.errno(posix.system.connect(socket_fd, addr, addr_len))) {
            .SUCCESS => return,
            .INTR => continue,
            .CANCELED => return error.Canceled,
            .AGAIN => @panic("TODO"),
            .INPROGRESS => return, // Due to TCP fast open, we find out possible error later.

            .ADDRNOTAVAIL => return error.AddressUnavailable,
            .AFNOSUPPORT => return error.AddressFamilyUnsupported,
            .ALREADY => return error.ConnectionPending,
            .BADF => |err| return errnoBug(err), // File descriptor used after closed.
            .CONNREFUSED => return error.ConnectionRefused,
            .CONNRESET => return error.ConnectionResetByPeer,
            .FAULT => |err| return errnoBug(err),
            .ISCONN => |err| return errnoBug(err),
            .HOSTUNREACH => return error.HostUnreachable,
            .NETUNREACH => return error.NetworkUnreachable,
            .NOTSOCK => |err| return errnoBug(err),
            .PROTOTYPE => |err| return errnoBug(err),
            .TIMEDOUT => return error.Timeout,
            .CONNABORTED => |err| return errnoBug(err),
            .ACCES => return error.AccessDenied,
            .PERM => |err| return errnoBug(err),
            .NOENT => |err| return errnoBug(err),
            .NETDOWN => return error.NetworkDown,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn netListenUnix(
    userdata: ?*anyopaque,
    unix_address: *const net.UnixAddress,
    options: net.UnixAddress.ListenOptions,
) net.UnixAddress.ListenError!net.Socket.Handle {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = unix_address;
    _ = options;
    @panic("TODO");
}
fn netConnectUnix(
    userdata: ?*anyopaque,
    unix_address: *const net.UnixAddress,
) net.UnixAddress.ConnectError!net.Socket.Handle {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = unix_address;
    @panic("TODO");
}

fn netSend(
    userdata: ?*anyopaque,
    handle: net.Socket.Handle,
    outgoing_messages: []net.OutgoingMessage,
    flags: net.SendFlags,
) struct { ?net.Socket.SendError, usize } {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));

    const posix_flags: u32 =
        @as(u32, if (@hasDecl(posix.MSG, "CONFIRM") and flags.confirm) posix.MSG.CONFIRM else 0) |
        @as(u32, if (@hasDecl(posix.MSG, "DONTROUTE") and flags.dont_route) posix.MSG.DONTROUTE else 0) |
        @as(u32, if (@hasDecl(posix.MSG, "EOR") and flags.eor) posix.MSG.EOR else 0) |
        @as(u32, if (@hasDecl(posix.MSG, "OOB") and flags.oob) posix.MSG.OOB else 0) |
        @as(u32, if (@hasDecl(posix.MSG, "FASTOPEN") and flags.fastopen) posix.MSG.FASTOPEN else 0) |
        posix.MSG.NOSIGNAL;

    for (outgoing_messages, 0..) |*msg, i| {
        netSendOne(k, handle, msg, posix_flags) catch |err| return .{ err, i };
    }

    return .{ null, outgoing_messages.len };
}

fn netSendOne(
    k: *Kqueue,
    handle: net.Socket.Handle,
    message: *net.OutgoingMessage,
    flags: u32,
) net.Socket.SendError!void {
    var addr: Io.Threaded.PosixAddress = undefined;
    var iovec: posix.iovec_const = .{ .base = @constCast(message.data_ptr), .len = message.data_len };
    const msg: posix.msghdr_const = .{
        .name = &addr.any,
        .namelen = Io.Threaded.addressToPosix(message.address, &addr),
        .iov = (&iovec)[0..1],
        .iovlen = 1,
        // OS returns EINVAL if this pointer is invalid even if controllen is zero.
        .control = if (message.control.len == 0) null else @constCast(message.control.ptr),
        .controllen = @intCast(message.control.len),
        .flags = 0,
    };
    while (true) {
        try k.checkCancel();
        const rc = posix.system.sendmsg(handle, &msg, flags);
        switch (posix.errno(rc)) {
            .SUCCESS => {
                message.data_len = @intCast(rc);
                return;
            },
            .INTR => continue,
            .CANCELED => return error.Canceled,
            .AGAIN => try waitReady(k, @bitCast(@as(i32, handle)), std.c.EVFILT.WRITE),

            .ACCES => return error.AccessDenied,
            .ALREADY => return error.FastOpenAlreadyInProgress,
            .BADF => |err| return errnoBug(err), // File descriptor used after closed.
            .CONNRESET => return error.ConnectionResetByPeer,
            .DESTADDRREQ => |err| return errnoBug(err),
            .FAULT => |err| return errnoBug(err),
            .INVAL => |err| return errnoBug(err),
            .ISCONN => |err| return errnoBug(err),
            .MSGSIZE => return error.MessageOversize,
            .NOBUFS => return error.SystemResources,
            .NOMEM => return error.SystemResources,
            .NOTSOCK => |err| return errnoBug(err),
            .OPNOTSUPP => |err| return errnoBug(err),
            .PIPE => return error.SocketUnconnected,
            .AFNOSUPPORT => return error.AddressFamilyUnsupported,
            .HOSTUNREACH => return error.HostUnreachable,
            .NETUNREACH => return error.NetworkUnreachable,
            .NOTCONN => return error.SocketUnconnected,
            .NETDOWN => return error.NetworkDown,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

/// Non-blocking send of as many messages as the socket accepts right now.
fn netSendManyNonblocking(
    handle: net.Socket.Handle,
    messages: []net.OutgoingMessage,
) union(enum) { full, partial: usize, blocked, err: net.Socket.SendError } {
    var i: usize = 0;
    while (i < messages.len) : (i += 1) {
        netSendOneNonblocking(handle, &messages[i]) catch |err| return switch (err) {
            error.WouldBlock => if (i == 0) .blocked else .{ .partial = i },
            else => |e| .{ .err = e },
        };
    }
    return .full;
}

fn netSendOneNonblocking(handle: net.Socket.Handle, message: *net.OutgoingMessage) (net.Socket.SendError || error{WouldBlock})!void {
    var addr: Io.Threaded.PosixAddress = undefined;
    var iovec: posix.iovec_const = .{ .base = @constCast(message.data_ptr), .len = message.data_len };
    const msg: posix.msghdr_const = .{
        .name = &addr.any,
        .namelen = Io.Threaded.addressToPosix(message.address, &addr),
        .iov = (&iovec)[0..1],
        .iovlen = 1,
        .control = if (message.control.len == 0) null else @constCast(message.control.ptr),
        .controllen = @intCast(message.control.len),
        .flags = 0,
    };
    const rc = posix.system.sendmsg(handle, &msg, posix.MSG.NOSIGNAL | posix.MSG.DONTWAIT);
    switch (posix.errno(rc)) {
        .SUCCESS => {
            message.data_len = @intCast(rc);
            return;
        },
        .INTR => return, // treated as zero sent; the caller retries
        .CANCELED => return error.Canceled,
        .AGAIN => return error.WouldBlock,
        .ACCES => return error.AccessDenied,
        .ALREADY => return error.FastOpenAlreadyInProgress,
        .BADF => |err| return errnoBug(err),
        .CONNRESET => return error.ConnectionResetByPeer,
        .DESTADDRREQ => |err| return errnoBug(err),
        .FAULT => |err| return errnoBug(err),
        .INVAL => |err| return errnoBug(err),
        .ISCONN => |err| return errnoBug(err),
        .MSGSIZE => return error.MessageOversize,
        .NOBUFS => return error.SystemResources,
        .NOMEM => return error.SystemResources,
        .NOTSOCK => |err| return errnoBug(err),
        .OPNOTSUPP => |err| return errnoBug(err),
        .PIPE => return error.SocketUnconnected,
        .AFNOSUPPORT => return error.AddressFamilyUnsupported,
        .HOSTUNREACH => return error.HostUnreachable,
        .NETUNREACH => return error.NetworkUnreachable,
        .NOTCONN => return error.SocketUnconnected,
        .NETDOWN => return error.NetworkDown,
        else => |err| return posix.unexpectedErrno(err),
    }
}

/// One blocking operation; the batch machinery above is the concurrent
/// path. Network operations retry through `waitReady`; file operations
/// block the worker thread (no file async yet on this backend).
fn operate(userdata: ?*anyopaque, operation: Io.Operation) Io.Cancelable!Io.Operation.Result {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    switch (operation) {
        .net_receive => |*o| {
            var data_i: usize = 0;
            var msg_i: usize = 0;
            while (msg_i < o.message_buffer.len) {
                const message = &o.message_buffer[msg_i];
                const remaining = o.data_buffer[data_i..];
                Io.Threaded.netReceivePosix(o.socket_handle, message, remaining, o.flags, true) catch |err| switch (err) {
                    error.Canceled => |e| return e,
                    error.WouldBlock => {
                        if (msg_i != 0) return .{ .net_receive = .{ null, msg_i } };
                        try waitReady(k, @bitCast(@as(isize, o.socket_handle)), std.c.EVFILT.READ);
                        continue;
                    },
                    else => |e| return .{ .net_receive = .{ e, 0 } },
                };
                data_i += message.data.len;
                msg_i += 1;
            }
            return .{ .net_receive = .{ null, msg_i } };
        },
        .net_send => |*o| return .{ .net_send = r: {
            var i: usize = 0;
            while (i < o.messages.len) : (i += 1) {
                netSendOneNonblocking(o.socket_handle, &o.messages[i]) catch |err| switch (err) {
                    error.WouldBlock => {
                        if (i != 0) break :r .{ null, i };
                        try waitReady(k, @bitCast(@as(isize, o.socket_handle)), std.c.EVFILT.WRITE);
                        i -%= 1;
                        continue;
                    },
                    else => |e| break :r .{ e, i },
                };
            }
            break :r .{ null, o.messages.len };
        } },
        .net_read => |*o| {
            while (true) {
                const rc = posix.system.read(o.socket_handle, o.data[0].ptr, o.data[0].len);
                switch (posix.errno(rc)) {
                    .SUCCESS => return .{ .net_read = @intCast(rc) },
                    .INTR => continue,
                    .CANCELED => return error.Canceled,
                    .AGAIN => try waitReady(k, @bitCast(@as(isize, o.socket_handle)), std.c.EVFILT.READ),
                    else => |e| return .{ .net_read = readErrorMap(e) },
                }
            }
        },
        .net_write => |*o| {
            while (true) {
                const rc = posix.system.write(o.socket_handle, o.data[0].ptr, o.data[0].len);
                switch (posix.errno(rc)) {
                    .SUCCESS => return .{ .net_write = @intCast(rc) },
                    .INTR => continue,
                    .CANCELED => return error.Canceled,
                    .AGAIN => try waitReady(k, @bitCast(@as(isize, o.socket_handle)), std.c.EVFILT.WRITE),
                    else => |e| return .{ .net_write = writeErrorMap(e) },
                }
            }
        },
        .file_read_streaming => |o| return .{
            .file_read_streaming = fileReadStreamingBlocking(o.file, o.data) catch |err| switch (err) {
                error.Canceled => |e| return e,
                else => |e| e,
            },
        },
        .file_write_streaming => |o| return .{
            .file_write_streaming = fileWriteStreamingBlocking(o.file, o.header, o.data, o.splat) catch |err| switch (err) {
                error.Canceled => |e| return e,
                else => |e| e,
            },
        },
        // No device_io_control path on this backend yet; the result is a
        // value, so report failure through the payload.
        .device_io_control => return .{ .device_io_control = -1 },
    }
}

/// The error half of `Io.Operation.Result`'s `net_read`/`net_write`
/// payloads: like `net.Stream.Reader.Error` without the cancelable
/// members, which the callers handle before mapping.
const NetRWError = error{
    AccessDenied,
    ConnectionResetByPeer,
    ConnectionTimedOut,
    NetworkDown,
    SocketUnconnected,
    SystemResources,
    Unexpected,
};

/// The error half of `Io.Operation.Result`'s `net_read` payload: like
/// `net.Stream.Reader.Error` without the cancelable members.
const NetReadError = error{
    AccessDenied,
    ConnectionResetByPeer,
    ConnectionTimedOut,
    NetworkDown,
    SocketUnconnected,
    SystemResources,
    Unexpected,
};

fn readErrorMap(e: posix.E) NetReadError {
    return switch (e) {
        .NOBUFS, .NOMEM => error.SystemResources,
        .NOTCONN => error.SocketUnconnected,
        .CONNRESET => error.ConnectionResetByPeer,
        .TIMEDOUT => error.ConnectionTimedOut,
        .NETDOWN => error.NetworkDown,
        .ACCES => error.AccessDenied,
        else => error.Unexpected,
    };
}

const NetWriteError = error{
    AddressFamilyUnsupported,
    ConnectionRefused,
    ConnectionResetByPeer,
    ConnectionTimedOut,
    FastOpenAlreadyInProgress,
    HostUnreachable,
    NetworkDown,
    NetworkUnreachable,
    SocketNotBound,
    SocketUnconnected,
    SystemResources,
    Unexpected,
};

fn writeErrorMap(e: posix.E) NetWriteError {
    return switch (e) {
        .NOBUFS, .NOMEM => error.SystemResources,
        .NOTCONN => error.SocketUnconnected,
        .CONNRESET => error.ConnectionResetByPeer,
        .TIMEDOUT => error.ConnectionTimedOut,
        .NETDOWN => error.NetworkDown,
        .AFNOSUPPORT => error.AddressFamilyUnsupported,
        .HOSTUNREACH => error.HostUnreachable,
        .NETUNREACH => error.NetworkUnreachable,
        .DESTADDRREQ => error.SocketNotBound,
        else => error.Unexpected,
    };
}

/// Blocking streaming read; the file operations on this backend have no
/// evented path yet, so the worker thread blocks. The batch drain treats
/// file operations the same way through `operate`'s fallback shape.
fn fileReadStreamingBlocking(file: File, data: []const []u8) File.ReadStreamingError!usize {
    var total: usize = 0;
    for (data) |buffer| {
        var done: usize = 0;
        while (done < buffer.len) {
            const rc = posix.system.read(file.handle, buffer.ptr + done, buffer.len - done);
            switch (posix.errno(rc)) {
                .SUCCESS => {
                    if (rc == 0) return total;
                    done += @intCast(rc);
                    total += @intCast(rc);
                },
                .INTR => continue,
                .INVAL, .FAULT, .BADF, .ISDIR => return error.Unexpected,
                .IO => return error.InputOutput,
                .NOBUFS, .NOMEM => return error.SystemResources,
                else => return error.Unexpected,
            }
        }
    }
    return total;
}

fn fileWriteStreamingBlocking(file: File, header: []const u8, data: []const []const u8, splat: usize) File.Writer.Error!usize {
    var total: usize = 0;
    for (header) |_| {}
    var writes: usize = if (splat == 0) 1 else splat;
    while (writes > 0) : (writes -= 1) {
        for (data) |buffer| {
            var done: usize = 0;
            while (done < buffer.len) {
                const rc = posix.system.write(file.handle, buffer.ptr + done, buffer.len - done);
                switch (posix.errno(rc)) {
                    .SUCCESS => {
                        done += @intCast(rc);
                        total += @intCast(rc);
                    },
                    .INTR => continue,
                    .INVAL, .FAULT, .BADF, .NOTSOCK => return error.Unexpected,
                    .IO => return error.InputOutput,
                    .NOBUFS, .NOMEM => return error.SystemResources,
                    .PIPE => return error.BrokenPipe,
                    else => return error.Unexpected,
                }
            }
        }
    }
    return total;
}

fn netRead(userdata: ?*anyopaque, fd: net.Socket.Handle, data: [][]u8) net.Stream.Reader.Error!usize {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));

    var iovecs_buffer: [max_iovecs_len]posix.iovec = undefined;
    var i: usize = 0;
    for (data) |buf| {
        if (iovecs_buffer.len - i == 0) break;
        if (buf.len != 0) {
            iovecs_buffer[i] = .{ .base = buf.ptr, .len = buf.len };
            i += 1;
        }
    }
    const dest = iovecs_buffer[0..i];
    assert(dest[0].len > 0);

    while (true) {
        try k.checkCancel();
        const rc = posix.system.readv(fd, dest.ptr, @intCast(dest.len));
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .CANCELED => return error.Canceled,
            .AGAIN => {
                try waitReady(k, @bitCast(@as(isize, fd)), std.c.EVFILT.READ);
                continue;
            },

            .INVAL => |err| return errnoBug(err),
            .FAULT => |err| return errnoBug(err),
            .BADF => |err| return errnoBug(err), // File descriptor used after closed.
            .NOBUFS => return error.SystemResources,
            .NOMEM => return error.SystemResources,
            .NOTCONN => return error.SocketUnconnected,
            .CONNRESET => return error.ConnectionResetByPeer,
            .TIMEDOUT => return error.Timeout,
            .PIPE => return error.SocketUnconnected,
            .NETDOWN => return error.NetworkDown,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn netClose(userdata: ?*anyopaque, sockets: []const net.Socket) void {
    _ = userdata;
    for (sockets) |socket| closeFd(socket.handle);
}

fn netShutdown(userdata: ?*anyopaque, handle: net.Socket.Handle, how: net.ShutdownHow) net.ShutdownError!void {
    _ = userdata;
    const posix_how: i32 = switch (how) {
        .recv => posix.SHUT.RD,
        .send => posix.SHUT.WR,
        .both => posix.SHUT.RDWR,
    };
    while (true) {
        switch (posix.errno(posix.system.shutdown(handle, posix_how))) {
            .SUCCESS => return,
            .INTR => continue,
            .BADF, .NOTSOCK, .INVAL => |err| return errnoBug(err),
            .NOTCONN => return error.SocketUnconnected,
            .NOBUFS => return error.SystemResources,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn netInterfaceNameResolve(
    userdata: ?*anyopaque,
    name: *const net.Interface.Name,
) net.Interface.Name.ResolveError!net.Interface {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = name;
    @panic("TODO");
}

fn netInterfaceName(userdata: ?*anyopaque, interface: net.Interface) net.Interface.NameError!net.Interface.Name {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = interface;
    @panic("TODO");
}

fn netLookup(
    userdata: ?*anyopaque,
    host_name: net.HostName,
    resolved: *Io.Queue(net.HostName.LookupResult),
    options: net.HostName.LookupOptions,
) net.HostName.LookupError!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    _ = host_name;
    _ = resolved;
    _ = options;
    @panic("TODO");
}

fn openSocketPosix(
    k: *Kqueue,
    family: posix.sa_family_t,
    options: IpAddress.BindOptions,
) error{
    AddressFamilyUnsupported,
    ProtocolUnsupportedBySystem,
    ProcessFdQuotaExceeded,
    SystemFdQuotaExceeded,
    SystemResources,
    ProtocolUnsupportedByAddressFamily,
    SocketModeUnsupported,
    OptionUnsupported,
    Unexpected,
    Canceled,
}!posix.socket_t {
    const mode, const protocol = try posixSocketModeProtocol(family, options.mode, options.protocol);
    const socket_fd = while (true) {
        try k.checkCancel();
        const flags: u32 = mode | if (Io.Threaded.socket_flags_unsupported) 0 else posix.SOCK.CLOEXEC;
        const socket_rc = posix.system.socket(family, flags, protocol);
        switch (posix.errno(socket_rc)) {
            .SUCCESS => {
                const fd: posix.fd_t = @intCast(socket_rc);
                errdefer closeFd(fd);
                if (Io.Threaded.socket_flags_unsupported) {
                    while (true) {
                        try k.checkCancel();
                        switch (posix.errno(posix.system.fcntl(fd, posix.F.SETFD, @as(usize, posix.FD_CLOEXEC)))) {
                            .SUCCESS => break,
                            .INTR => continue,
                            .CANCELED => return error.Canceled,
                            else => |err| return posix.unexpectedErrno(err),
                        }
                    }

                    var fl_flags: usize = while (true) {
                        try k.checkCancel();
                        const rc = posix.system.fcntl(fd, posix.F.GETFL, @as(usize, 0));
                        switch (posix.errno(rc)) {
                            .SUCCESS => break @intCast(rc),
                            .INTR => continue,
                            .CANCELED => return error.Canceled,
                            else => |err| return posix.unexpectedErrno(err),
                        }
                    };
                    fl_flags |= @as(usize, 1 << @bitOffsetOf(posix.O, "NONBLOCK"));
                    while (true) {
                        try k.checkCancel();
                        switch (posix.errno(posix.system.fcntl(fd, posix.F.SETFL, fl_flags))) {
                            .SUCCESS => break,
                            .INTR => continue,
                            .CANCELED => return error.Canceled,
                            else => |err| return posix.unexpectedErrno(err),
                        }
                    }
                }
                break fd;
            },
            .INTR => continue,
            .CANCELED => return error.Canceled,

            .AFNOSUPPORT => return error.AddressFamilyUnsupported,
            .INVAL => return error.ProtocolUnsupportedBySystem,
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOBUFS => return error.SystemResources,
            .NOMEM => return error.SystemResources,
            .PROTONOSUPPORT => return error.ProtocolUnsupportedByAddressFamily,
            .PROTOTYPE => return error.SocketModeUnsupported,
            else => |err| return posix.unexpectedErrno(err),
        }
    };
    errdefer closeFd(socket_fd);

    if (options.ip6_only) |ip6_only| {
        if (posix.IPV6 == void) return error.OptionUnsupported;
        try setSocketOption(k, socket_fd, posix.IPPROTO.IPV6, posix.IPV6.V6ONLY, @intFromBool(ip6_only));
    }

    return socket_fd;
}

fn posixBind(
    k: *Kqueue,
    socket_fd: posix.socket_t,
    addr: *const posix.sockaddr,
    addr_len: posix.socklen_t,
) !void {
    while (true) {
        try k.checkCancel();
        switch (posix.errno(posix.system.bind(socket_fd, addr, addr_len))) {
            .SUCCESS => break,
            .INTR => continue,
            .CANCELED => return error.Canceled,

            .ACCES => return error.AccessDenied,
            .ADDRINUSE => return error.AddressInUse,
            .BADF => |err| return errnoBug(err), // File descriptor used after closed.
            .INVAL => |err| return errnoBug(err), // invalid parameters
            .NOTSOCK => |err| return errnoBug(err), // invalid `sockfd`
            .AFNOSUPPORT => return error.AddressFamilyUnsupported,
            .ADDRNOTAVAIL => return error.AddressUnavailable,
            .FAULT => |err| return errnoBug(err), // invalid `addr` pointer
            .NOMEM => return error.SystemResources,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn posixGetSockName(k: *Kqueue, socket_fd: posix.fd_t, addr: *posix.sockaddr, addr_len: *posix.socklen_t) !void {
    while (true) {
        try k.checkCancel();
        switch (posix.errno(posix.system.getsockname(socket_fd, addr, addr_len))) {
            .SUCCESS => break,
            .INTR => continue,
            .CANCELED => return error.Canceled,

            .BADF => |err| return errnoBug(err), // File descriptor used after closed.
            .FAULT => |err| return errnoBug(err),
            .INVAL => |err| return errnoBug(err), // invalid parameters
            .NOTSOCK => |err| return errnoBug(err), // always a race condition
            .NOBUFS => return error.SystemResources,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn setSocketOption(k: *Kqueue, fd: posix.fd_t, level: i32, opt_name: u32, option: u32) !void {
    const o: []const u8 = @ptrCast(&option);
    while (true) {
        try k.checkCancel();
        switch (posix.errno(posix.system.setsockopt(fd, level, opt_name, o.ptr, @intCast(o.len)))) {
            .SUCCESS => return,
            .INTR => continue,
            .CANCELED => return error.Canceled,

            .BADF => |err| return errnoBug(err), // File descriptor used after closed.
            .NOTSOCK => |err| return errnoBug(err),
            .INVAL => |err| return errnoBug(err),
            .FAULT => |err| return errnoBug(err),
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

/// Parks the calling fiber until `ident`/`filter` becomes ready. Registers
/// through the thread's `wait_queues` so several fibers waiting on the same
/// ident and filter share one kevent, and so the wake path is the ordinary
/// fiber path.
fn waitReady(k: *Kqueue, ident: usize, filter: i16) Io.Cancelable!void {
    const thread: *Thread = .current();
    const fiber = thread.currentFiber();
    const gop = thread.wait_queues.getOrPut(k.gpa, .{
        .ident = ident,
        .filter = filter,
    }) catch {
        // Out of memory for the registration: block this worker thread on
        // the readiness directly. Other fibers on it stall; this is the
        // never-taken fallback.
        const changes = [_]posix.Kevent{.{
            .ident = ident,
            .filter = filter,
            .flags = std.c.EV.ADD | std.c.EV.ONESHOT,
            .fflags = 0,
            .data = 0,
            .udata = 0,
        }};
        var one_event: [1]posix.Kevent = undefined;
        _ = kevent(thread.kq_fd, &changes, &one_event, null) catch {};
        return;
    };
    if (gop.found_existing) {
        const tail_fiber = gop.value_ptr.*;
        assert(tail_fiber.queue_next == null);
        tail_fiber.queue_next = fiber;
        gop.value_ptr.* = fiber;
    } else {
        gop.value_ptr.* = fiber;
        const changes = [_]posix.Kevent{
            .{
                .ident = ident,
                .filter = filter,
                .flags = std.c.EV.ADD | std.c.EV.ONESHOT,
                .fflags = 0,
                .data = 0,
                .udata = @intFromPtr(fiber),
            },
        };
        assert(0 == (kevent(thread.kq_fd, &changes, &.{}, null) catch |err| {
            // TODO handle EINTR for cancellation purposes
            @panic(@errorName(err)); // TODO
        }));
    }
    yield(k, null, .nothing);
}

/// Adds or deletes the calling fiber's batch timer. The timer's ident is
/// the fiber pointer (unique per waiting fiber; fd idents are small), its
/// udata the tagged batch waiter.
fn batchTimerChange(kq_fd: posix.fd_t, fiber: *Fiber, ms: i64, delete: bool) void {
    if (kq_fd < 0) return;
    const changes = [_]posix.Kevent{
        .{
            .ident = @intFromPtr(fiber),
            .filter = std.c.EVFILT.TIMER,
            .flags = if (delete) std.c.EV.DELETE else std.c.EV.ADD | std.c.EV.ONESHOT,
            .fflags = 0,
            .data = ms,
            .udata = @intFromPtr(&fiber.batch_waiter) | batch_userdata_tag,
        },
    };
    // A delete of an already-consumed timer is fine (ENOENT); any other
    // failure only pessimises scheduling.
    _ = kevent(kq_fd, &changes, &.{}, null) catch {};
}

/// Tries every submitted operation without blocking. Operations that
/// complete move to `completed`; the others are (re-)armed as one-shot
/// readiness kevents carrying the tagged batch waiter, and stay in
/// `submitted` for the next wake.
fn batchDrainSubmitted(k: *Kqueue, b: *Io.Batch) Io.Cancelable!void {
    const thread: *Thread = .current();
    const fiber = thread.currentFiber();
    var changes: [changes_buffer_len]posix.Kevent = undefined;
    var changes_len: usize = 0;
    var prev_index: Io.Operation.OptionalIndex = .none;
    var index = b.submitted.head;
    while (index != .none) {
        const storage = &b.storage[index.toIndex()];
        const submission = storage.submission;
        const next_index = submission.node.next;
        var completed_inline = false;
        const result: Io.Operation.Result = switch (submission.operation) {
            .net_receive => |*o| r: {
                var data_i: usize = 0;
                var msg_i: usize = 0;
                break :r drain: for (o.message_buffer) |*message| {
                    const remaining = o.data_buffer[data_i..];
                    Io.Threaded.netReceivePosix(o.socket_handle, message, remaining, o.flags, true) catch |err| switch (err) {
                        error.Canceled => |e| return e,
                        error.WouldBlock => {
                            if (msg_i != 0) break :drain .{ .net_receive = .{ null, msg_i } };
                            arm(k, &changes, &changes_len, fiber, o.socket_handle, std.c.EVFILT.READ);
                            break :r .{ .net_receive = .{ error.SystemResources, 0 } };
                        },
                        else => |e| break :drain .{ .net_receive = .{ e, 0 } },
                    };
                    data_i += message.data.len;
                    msg_i += 1;
                } else .{ .net_receive = .{ null, msg_i } };
            },
            .net_send => |*o| r: {
                const sent = netSendManyNonblocking(o.socket_handle, o.messages);
                switch (sent) {
                    .full => break :r .{ .net_send = .{ null, o.messages.len } },
                    .partial => |n| break :r .{ .net_send = .{ null, n } },
                    .blocked => {
                        arm(k, &changes, &changes_len, fiber, o.socket_handle, std.c.EVFILT.WRITE);
                        break :r .{ .net_send = .{ error.SystemResources, 0 } };
                    },
                    .err => |e| break :r .{ .net_send = .{ e, 0 } },
                }
            },
            .net_read => |*o| r: {
                const rc = posix.system.read(o.socket_handle, o.data[0].ptr, o.data[0].len);
                switch (posix.errno(rc)) {
                    .SUCCESS => break :r .{ .net_read = @intCast(rc) },
                    .INTR => break :r .{ .net_read = 0 },
                    .CANCELED => return error.Canceled,
                    .AGAIN => {
                        arm(k, &changes, &changes_len, fiber, o.socket_handle, std.c.EVFILT.READ);
                        break :r .{ .net_read = error.SystemResources };
                    },
                    else => |e| break :r .{ .net_read = readErrorMap(e) },
                }
            },
            .net_write => |*o| r: {
                const rc = posix.system.write(o.socket_handle, o.data[0].ptr, o.data[0].len);
                switch (posix.errno(rc)) {
                    .SUCCESS => break :r .{ .net_write = @intCast(rc) },
                    .INTR => break :r .{ .net_write = 0 },
                    .CANCELED => return error.Canceled,
                    .AGAIN => {
                        arm(k, &changes, &changes_len, fiber, o.socket_handle, std.c.EVFILT.WRITE);
                        break :r .{ .net_write = error.SystemResources };
                    },
                    else => |e| break :r .{ .net_write = writeErrorMap(e) },
                }
            },
            else => .{ .device_io_control = 0 },
        };
        completed_inline = !isArmedResult(result);
        if (completed_inline) {
            // unlink from submitted, append to completed
            switch (prev_index) {
                .none => b.submitted.head = next_index,
                else => |p| b.storage[p.toIndex()].submission.node.next = next_index,
            }
            if (next_index == .none) b.submitted.tail = prev_index;
            switch (b.completed.tail) {
                .none => b.completed.head = index,
                else => |tail| b.storage[tail.toIndex()].completion.node.next = index,
            }
            storage.* = .{ .completion = .{ .node = .{ .next = .none }, .result = result } };
            b.completed.tail = index;
        } else prev_index = index;
        index = next_index;
    }
    if (changes_len != 0) {
        assert(0 == (kevent(thread.kq_fd, changes[0..changes_len], &.{}, null) catch |err| {
            // TODO handle EINTR for cancellation purposes
            @panic(@errorName(err)); // TODO
        }));
    }
}

/// The result a still-armed operation reports to hold its place: it is
/// replaced on completion; the caller never sees it unless the wait is
/// dropped without cancelling (a contract violation on other backends
/// too).
fn isArmedResult(result: Io.Operation.Result) bool {
    return switch (result) {
        .net_receive => |r| r[0] != null and r[0].? == error.SystemResources and r[1] == 0,
        .net_send => |r| r[0] != null and r[0].? == error.SystemResources and r[1] == 0,
        .net_read, .net_write => |e| e == error.SystemResources,
        else => false,
    };
}

fn arm(
    k: *Kqueue,
    changes: *[changes_buffer_len]posix.Kevent,
    changes_len: *usize,
    fiber: *Fiber,
    handle: net.Socket.Handle,
    filter: i16,
) void {
    _ = k;
    if (changes_len.* == changes.len) return; // XXX overflow: batch larger than buffer
    changes[changes_len.*] = .{
        .ident = @bitCast(@as(isize, handle)),
        .filter = filter,
        .flags = std.c.EV.ADD | std.c.EV.ONESHOT,
        .fflags = 0,
        .data = 0,
        .udata = @intFromPtr(&fiber.batch_waiter) | batch_userdata_tag,
    };
    changes_len.* += 1;
}

fn batchAwaitAsync(userdata: ?*anyopaque, b: *Io.Batch) Io.Cancelable!void {
    // The fiber backend is inherently concurrent, and `.none` never times
    // out, so the wider error set cannot actually occur.
    return batchAwaitConcurrent(userdata, b, .none) catch |err| switch (err) {
        error.ConcurrencyUnavailable, error.Timeout => unreachable,
        error.Canceled => |e| return e,
    };
}

fn batchAwaitConcurrent(
    userdata: ?*anyopaque,
    b: *Io.Batch,
    timeout: Io.Timeout,
) Io.Batch.AwaitConcurrentError!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const fiber = Thread.current().currentFiber();
    const waiter = &fiber.batch_waiter;
    while (true) {
        // Events must find the slot empty while the fiber runs, so a
        // stale event only records the wake; the register_awaiter switch
        // task below parks this fiber atomically and schedules it right
        // away when a wake already landed.
        @atomicStore(?*Fiber, &waiter.parked, null, .release);
        try batchDrainSubmitted(k, b);
        if (b.submitted.head == .none) return; // everything completed
        if (b.completed.head != .none) return; // something completed inline
        // Re-arming consumed one-shots is an EV_ADD refresh. The drain ran
        // on this thread, so this wait's kevents live on its kq; after a
        // work-stealing migration the deletes must still target it.
        waiter.kq_fd = Thread.current().kq_fd;
        var when_ns: ?i64 = null;
        if (timeout != .none) {
            when_ns = switch (timeout) {
                .none => null,
                .duration => |duration| @as(i64, @intCast(Io.Threaded.nowPosix(duration.clock).toNanoseconds() + duration.raw.toNanoseconds())),
                .deadline => |deadline| @intCast(deadline.raw.toNanoseconds()),
            };
            waiter.when_ns = when_ns;
            const ms: i64 = switch (timeout) {
                .none => unreachable,
                .duration => |duration| @max(1, duration.raw.toMilliseconds()),
                .deadline => |deadline| ms: {
                    const remaining: i64 = @intCast(when_ns.? - Io.Threaded.nowPosix(deadline.clock).toNanoseconds());
                    if (remaining <= 0) break :ms 1;
                    break :ms @divTrunc(remaining, std.time.ns_per_ms);
                },
            };
            batchTimerChange(waiter.kq_fd, fiber, ms, false);
        }
        yield(k, null, .{ .register_batch_waiter = &waiter.parked });
        // A stale wake (an event for an earlier wait of this fiber, or a
        // spurious one) just goes back to sleep after the deadline check.
        if (b.submitted.head == .none) {
            if (timeout != .none) batchTimerChange(waiter.kq_fd, fiber, 0, true);
            return;
        }
        if (timeout != .none) {
            const now_ns = Io.Threaded.nowPosix(.awake).toNanoseconds();
            if (now_ns < when_ns.?) {
                // Stale timer fire: the wait continues. Delete nothing; the
                // timer was consumed by firing.
                continue;
            }
            // The deadline passed with operations still armed: they stay
            // submitted and armed, as on the other backends, for a later
            // await or `Batch.cancel`.
            return error.Timeout;
        }
    }
}

fn batchCancel(userdata: ?*anyopaque, b: *Io.Batch) void {
    _ = userdata;
    const fiber = Thread.current().currentFiber();
    // The fiber runs (slot empty), so in-flight events can only arrive
    // later and find the `finished` sentinel: no-ops.
    _ = @atomicRmw(?*Fiber, &fiber.batch_waiter.parked, .Xchg, Fiber.finished, .acq_rel);
    if (fiber.batch_waiter.when_ns != null) batchTimerChange(fiber.batch_waiter.kq_fd, fiber, 0, true);
    var changes: [changes_buffer_len]posix.Kevent = undefined;
    var changes_len: usize = 0;
    var index = b.submitted.head;
    while (index != .none) {
        const storage = &b.storage[index.toIndex()];
        const submission = storage.submission;
        const filter: i16 = switch (submission.operation) {
            .net_receive, .net_read => std.c.EVFILT.READ,
            .net_send, .net_write => std.c.EVFILT.WRITE,
            else => break,
        };
        const handle: net.Socket.Handle = switch (submission.operation) {
            .net_receive => |o| o.socket_handle,
            .net_send => |o| o.socket_handle,
            .net_read => |o| o.socket_handle,
            .net_write => |o| o.socket_handle,
            else => break,
        };
        if (changes_len == changes.len) break;
        changes[changes_len] = .{
            .ident = @bitCast(@as(isize, handle)),
            .filter = filter,
            .flags = std.c.EV.DELETE,
            .fflags = 0,
            .data = 0,
            .udata = 0,
        };
        changes_len += 1;
        index = submission.node.next;
    }
    if (changes_len != 0 and fiber.batch_waiter.kq_fd >= 0) {
        _ = kevent(fiber.batch_waiter.kq_fd, changes[0..changes_len], &.{}, null) catch {};
    }
    // Return the storages to the unused list, as the other backends do.
    index = b.submitted.head;
    while (index != .none) {
        const storage = &b.storage[index.toIndex()];
        const next_index = storage.submission.node.next;
        const tail_index = b.unused.tail;
        switch (tail_index) {
            .none => b.unused.head = index,
            else => |tail| b.storage[tail.toIndex()].unused.next = index,
        }
        storage.* = .{ .unused = .{ .prev = tail_index, .next = .none } };
        b.unused.tail = index;
        index = next_index;
    }
    b.submitted = .empty;
}

fn checkCancel(k: *Kqueue) error{Canceled}!void {
    if (cancelRequested(k)) return error.Canceled;
}

pub const KEventError = error{
    /// The process does not have permission to register a filter.
    AccessDenied,
    /// The event could not be found to be modified or deleted.
    EventNotFound,
    /// No memory was available to register the event.
    SystemResources,
    /// The specified process to attach to does not exist.
    ProcessNotFound,
    /// changelist or eventlist had too many items on it.
    /// TODO remove this possibility
    Overflow,
};

pub fn kevent(
    kq: i32,
    changelist: []const posix.Kevent,
    eventlist: []posix.Kevent,
    timeout: ?*const posix.timespec,
) KEventError!usize {
    while (true) {
        const rc = posix.system.kevent(
            kq,
            changelist.ptr,
            std.math.cast(c_int, changelist.len) orelse return error.Overflow,
            eventlist.ptr,
            std.math.cast(c_int, eventlist.len) orelse return error.Overflow,
            timeout,
        );
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .ACCES => return error.AccessDenied,
            .FAULT => unreachable, // TODO use error.Unexpected for these
            .BADF => unreachable, // Always a race condition.
            .INTR => continue, // TODO handle cancelation
            .INVAL => unreachable,
            .NOENT => return error.EventNotFound,
            .NOMEM => return error.SystemResources,
            .SRCH => return error.ProcessNotFound,
            else => unreachable,
        }
    }
}
