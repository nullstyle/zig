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
/// Synchronous filesystem and process operations share the Threaded
/// implementation, with its own allocator, environment, and random state.
/// No Threaded workers or process-wide signal handlers are installed.
threaded: Io.Threaded,
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
fiber_pool: ?*Fiber,
fiber_pool_mutex: NativeMutex = .{},
futex_mutex: NativeMutex = .{},
futex_waiters: std.DoublyLinkedList = .{},

/// Empirically saw >128KB being used by the self-hosted backend to panic.
const idle_stack_size = 256 * 1024;

const max_idle_search = 4;
const max_steal_ready_search = 4;
const max_iovecs_len = 8;

const changes_buffer_len = 64;

// These locks protect short scheduler metadata updates across OS threads;
// they must not park a fiber while its registration is being changed.
const NativeMutex = struct {
    state: Io.Mutex = .init,

    fn lock(m: *NativeMutex) void {
        Io.Threaded.mutexLock(&m.state);
    }

    fn unlock(m: *NativeMutex) void {
        Io.Threaded.mutexUnlock(&m.state);
    }
};

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
    wait_queues: std.array_hash_map.Auto(WaitQueueKey, std.DoublyLinkedList),
    wait_mutex: NativeMutex = .{},

    const WaitQueueKey = struct {
        ident: usize,
        filter: i32,
    };

    const canceling: ?*Thread = @ptrFromInt(@alignOf(Thread));

    threadlocal var self: *Thread = undefined;

    /// Reading the identity is a call that cannot be inlined, and its
    /// load is volatile: fibers migrate between OS threads inside
    /// `contextSwitch`, but the compiler is free to assume a threadlocal
    /// cannot change within a function. A non-volatile read, or an
    /// inlined one whose thread-pointer address computation gets hoisted
    /// out of a park loop and spilled across the switch (a register
    /// clobber does not invalidate a stack spill), makes a migrated
    /// fiber read the OLD thread's slot — memory the thread library
    /// frees when that thread exits — and then lock, arm, and park
    /// against garbage. The call re-executes the address computation on
    /// the current thread; the volatile load cannot be merged across
    /// calls.
    ///
    /// Do not copy the Dispatch/Uring `asm` form here: their `self`
    /// holds the struct itself, so `&self` is the pointer; this `self`
    /// is already a pointer, and the asm would return the slot's
    /// address.
    noinline fn current() *Thread {
        return @as(*volatile *Thread, @ptrCast(&self)).*;
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
    /// Null while no cancelation request is pending; `Thread.canceling`
    /// once one is. `checkCancel` consumes a pending request (the contract
    /// is that only the next cancelation point signals); `recancel`
    /// re-arms one.
    cancel_thread: ?*Thread,
    cancel_protection: Io.CancelProtection,
    awaiting_completions: std.bit_set.Static(3),
    /// The fiber's side of a batched wait; lives here so events that arrive
    /// late (after the wait completed or was cancelled) reference memory
    /// that is stable for the fiber's whole life.
    batch_waiter: BatchWaiter,
    group_node: std.DoublyLinkedList.Node = .{},
    futex_node: std.DoublyLinkedList.Node = .{},
    futex_ptr: ?*const u32 = null,

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
        k.fiber_pool_mutex.lock();
        if (k.fiber_pool) |fiber| {
            k.fiber_pool = fiber.queue_next;
            fiber.queue_next = null;
            k.fiber_pool_mutex.unlock();
            return fiber;
        }
        k.fiber_pool_mutex.unlock();
        return @ptrCast(try allocateStack(k.gpa, .of(Fiber), allocation_size));
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

    const Queue = struct { head: *Fiber, tail: *Fiber };
};

fn allocateStack(gpa: Allocator, comptime alignment: Alignment, len: usize) Allocator.Error![]align(alignment.toByteUnits()) u8 {
    if (builtin.os.tag != .openbsd) return gpa.alignedAlloc(u8, alignment, len);
    // OpenBSD validates the stack pointer at syscall entry. These stacks need
    // their own mappings: an allocator may share ordinary heap pages.
    const memory = posix.mmap(null, len, .{ .READ = true, .WRITE = true }, .{
        .TYPE = .PRIVATE,
        .ANONYMOUS = true,
        .STACK = true,
    }, -1, 0) catch return error.OutOfMemory;
    return @alignCast(memory);
}

fn freeStack(gpa: Allocator, memory: anytype) void {
    if (builtin.os.tag == .openbsd) {
        posix.munmap(@alignCast(memory));
    } else {
        gpa.free(memory);
    }
}

fn recycle(k: *Kqueue, fiber: *Fiber) void {
    std.log.debug("recyling {*}", .{fiber});
    assert(fiber.queue_next == null);
    // Protect both the head and queue_next. A pointer-only CAS permits
    // ABA when another worker pops, uses and recycles the same fiber.
    k.fiber_pool_mutex.lock();
    defer k.fiber_pool_mutex.unlock();
    fiber.queue_next = k.fiber_pool;
    k.fiber_pool = fiber;
}

pub const InitOptions = struct {
    n_threads: ?usize = null,
    argv0: Io.Threaded.Argv0 = .empty,
    environ: std.process.Environ = .empty,
};

pub const InitError = Allocator.Error || CreateFileDescriptorError;

pub fn init(k: *Kqueue, gpa: Allocator, options: InitOptions) !void {
    assert(options.n_threads != 0);

    const n_threads = @max(1, options.n_threads orelse std.Thread.getCpuCount() catch 1);
    const threads_size = n_threads * @sizeOf(Thread);
    const idle_stack_end_offset = std.mem.alignForward(usize, threads_size + idle_stack_size, std.heap.page_size_max);
    const allocated_slice = try allocateStack(gpa, .of(Thread), idle_stack_end_offset);
    errdefer freeStack(gpa, allocated_slice);
    k.* = .{
        .gpa = gpa,
        .threaded = .init_single_threaded,
        .mutex = .init,
        .main_fiber_buffer = undefined,
        .fiber_pool = null,
        .threads = .{
            .allocated = @ptrCast(allocated_slice[0..threads_size]),
            .reserved = 1,
            .active = 1,
        },
    };
    k.threaded.allocator = gpa;
    k.threaded.argv0 = options.argv0;
    k.threaded.environ = .{ .process_environ = options.environ };
    k.threaded.environ_initialized = options.environ.block.isEmpty();
    const main_fiber: *Fiber = @ptrCast(&k.main_fiber_buffer);
    main_fiber.* = .{
        .required_align = {},
        .context = undefined,
        .awaiter = null,
        .queue_next = null,
        .cancel_thread = null,
        .cancel_protection = .unblocked,
        .awaiting_completions = .empty,
        .batch_waiter = .{},
    };
    const main_thread = &k.threads.allocated[0];
    Thread.self = main_thread;
    // Keep the initial SP inside the mapping, including before the entry
    // prologue: OpenBSD validates it when resolving instruction page faults.
    const idle_stack_end: [*]align(16) usize = @ptrCast(@alignCast(allocated_slice[idle_stack_end_offset - 16 ..].ptr));
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
    // Wake every thread with the exit event directly from this fiber's
    // thread; a kevent change needs no context switch. The previous
    // design parked this fiber via `yield(null, .exit)` and ran the
    // triggers from the idle context, which left this thread's idle loop
    // spinning through the window where workers erased their `Thread`
    // structs — its ready-fiber search then dereferenced them.
    for (k.threads.allocated[0..active_threads]) |*thread| {
        triggerWakeupEvent(thread.kq_fd, @backingInt(Completion.UserData.exit));
    }
    // Join the workers before touching any `Thread` or the fiber pool: a
    // worker still inside `idle` can be scheduling fibers or recycling
    // into the pool. Workers retire their queues with the `finished`
    // lock before exiting, so the join is the final synchronization
    // point; all per-thread cleanup happens here, after it. The calling
    // fiber may itself have migrated to a worker (a group handoff or a
    // stolen wake moves fibers between threads), so never join — or
    // otherwise wait on — the current thread: it keeps running the
    // caller through the rest of teardown and the process exit, on the
    // caller's stack, and is never joined.
    const gpa = k.gpa;
    const current_thread: *Thread = .current();
    // Index 0 is the calling OS thread's own struct and is never joinable:
    // either it is the current thread, or it already exited via
    // `mainIdle`'s `pthread_exit` after the main fiber migrated here.
    for (k.threads.allocated[1..active_threads]) |*thread| {
        if (thread != current_thread) thread.thread.join();
    }
    assert(k.futex_waiters.first == null);
    while (k.fiber_pool) |fiber| {
        k.fiber_pool = fiber.queue_next;
        freeStack(gpa, fiber.allocatedSlice());
    }
    for (k.threads.allocated[0..active_threads]) |*thread| thread.deinit(gpa);
    const allocated_ptr: [*]align(@alignOf(Thread)) u8 = @ptrCast(@alignCast(k.threads.allocated.ptr));
    const idle_stack_end_offset = std.mem.alignForward(usize, k.threads.allocated.len * @sizeOf(Thread) + idle_stack_size, std.heap.page_size_max);
    freeStack(gpa, allocated_ptr[0..idle_stack_end_offset]);
    k.threaded.deinit();
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
            // EV_CLEAR re-arms the knote after each delivery. Without it a
            // triggered user event stays ready, every kevent() call returns
            // immediately, and idle threads busy-spin (a healthy benchmark
            // run burned ~2 minutes of CPU across idle workers).
            .flags = std.c.EV.ADD | std.c.EV.CLEAR,
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
    )) |old_head| {
        // Never splice the lock sentinel into the chain: retry until the
        // owner's pop stores the real head. (A fiber linked with
        // `queue_next == finished` would store the sentinel back as the
        // queue itself, and the next pop would treat it as a fiber.)
        if (old_head != Fiber.finished) ready_queue.tail.queue_next = old_head;
    }
}

fn mainIdle(k: *Kqueue, message: *const SwitchMessage) callconv(.withStackAlign(.c, @max(@alignOf(Thread), @alignOf(Io.fiber.Context)))) noreturn {
    message.handle(k);
    k.idle(&k.threads.allocated[0]);
    // `idle` only returns on the exit event, by which time the main fiber
    // may have migrated to a worker and be running `deinit` there.
    // Resuming it here would execute one stack on two threads, so this
    // thread's part in the process is over: the main fiber finishes the
    // teardown (and the process) wherever it happens to be running.
    posix.system.pthread_exit(@ptrFromInt(0));
}

fn threadEntry(k: *Kqueue, index: u32) void {
    const thread: *Thread = &k.threads.allocated[index];
    Thread.self = thread;
    std.log.debug("created thread idle {*}", .{&thread.idle_context});
    k.idle(thread);
    // Retire the queue with the permanent `finished` lock: steal searches
    // skip it and cross-thread pushes fail their null-expected CAS. The
    // `Thread` struct itself stays valid — its fd and wait map are closed
    // by `deinit` after joining — so a peer that is mid-`findReadyFiber`
    // or `schedule` when this thread exits never races freed or poisoned
    // memory. (Erasing the struct here, as this used to do, left the
    // struct readable through `threads.allocated[0..active]` while its
    // fields turned to `undefined`: the steal loop then treated the
    // poison as a fiber pointer and faulted reading `queue_next`.)
    if (@cmpxchgStrong(
        ?*Fiber,
        &thread.ready_queue,
        null,
        Fiber.finished,
        .release,
        .monotonic,
    )) |stray| assert(stray == Fiber.finished); // push after the exit event: pending async
}

/// The group owns a list of live fibers so cancellation can reach every
/// member. The awaiter frees the state after the last member removes itself.
const GroupState = struct {
    mutex: NativeMutex = .{},
    members: std.DoublyLinkedList = .{},
    awaiter: ?*Fiber = null,
    canceling: bool = false,
};

/// One membership in a worker's shared descriptor/filter registration.
/// A batch has one stable node per operation; a direct wait uses a stack
/// node. Only the owner's wait_mutex may inspect or change registered.
const SocketWait = struct {
    node: std.DoublyLinkedList.Node = .{},
    owner: ?*Thread = null,
    key: Thread.WaitQueueKey = undefined,
    fiber: *Fiber = undefined,
    registered: bool = false,
};

/// Tag bit in kevent `udata` distinguishing a waiter from control events.
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
const BatchWaiter = struct {
    /// null while the fiber runs; the fiber while it is parked; the
    /// `finished` sentinel once an event has claimed the wake.
    parked: ?*Fiber = null,
    /// The kqueue descriptor this wait's kevents were registered on.
    kq_fd: posix.fd_t = -1,
};

const Completion = struct {
    const UserData = enum(usize) {
        unused,
        wakeup,
        cleanup,
        exit,
        readiness,
        /// Tagged *BatchWaiter.
        _,
    };
};

fn wakeWaiter(k: *Kqueue, waiter: *BatchWaiter) void {
    const parked = @atomicRmw(?*Fiber, &waiter.parked, .Xchg, Fiber.finished, .acq_rel);
    if (parked) |fiber| {
        if (fiber != Fiber.finished) k.schedule(.current(), .{ .head = fiber, .tail = fiber });
    }
}

fn idle(k: *Kqueue, thread: *Thread) void {
    var events: [changes_buffer_len]posix.Kevent = undefined;
    while (true) {
        while (k.findReadyFiber(thread)) |fiber| k.yield(fiber, .nothing);
        const n = kevent(thread.kq_fd, &.{}, &events, null) catch |err| @panic(@errorName(err));
        for (events[0..n]) |event| switch (@as(Completion.UserData, @fromBackingInt(@intCast(event.udata)))) {
            .unused => unreachable,
            .wakeup => {},
            .cleanup => @panic("failed to notify other threads that we are exiting"),
            .exit => return,
            .readiness => {
                // Readiness and cancellation may run on different workers.
                // Remove all waiters under the registration owner's lock;
                // each wake still claims the fiber's park slot exactly once.
                thread.wait_mutex.lock();
                defer thread.wait_mutex.unlock();
                var list = (thread.wait_queues.fetchSwapRemove(.{
                    .ident = event.ident,
                    .filter = event.filter,
                }) orelse continue).value;
                while (list.popFirst()) |node| {
                    const wait: *SocketWait = @fieldParentPtr("node", node);
                    wait.registered = false;
                    k.wakeWaiter(&wait.fiber.batch_waiter);
                }
            },
            _ => {
                assert(event.udata & batch_userdata_tag != 0);
                const waiter: *BatchWaiter = @ptrFromInt(event.udata & ~batch_userdata_tag);
                k.wakeWaiter(waiter);
            },
        };
    }
}

const SwitchMessage = struct {
    contexts: Io.fiber.Switch,
    pending_task: PendingTask,

    const PendingTask = union(enum) {
        nothing,
        recycle: *Fiber,
        complete_future: *Fiber,
        group_finish: struct { fiber: *Fiber, state: *GroupState },
        register_awaiter: *?*Fiber,
        /// Parks the switching fiber in a batch waiter slot. Unlike
        /// `register_awaiter`, this only claims the slot when it is empty:
        /// a wake that landed between arming the kevents and the switch
        /// has already left the `finished` sentinel, and the fiber must
        /// schedule itself — without overwriting the sentinel, which later
        /// stale events must keep seeing.
        register_batch_waiter: *?*Fiber,
    };

    fn handle(message: *const SwitchMessage, k: *Kqueue) void {
        const thread: *Thread = .current();
        thread.current_context = message.contexts.new;
        switch (message.pending_task) {
            .nothing => {},
            .recycle => |fiber| {
                k.recycle(fiber);
            },
            .complete_future => |fiber| {
                // Publish only after the completing fiber has parked. The
                // awaiter may immediately recycle its stack on another CPU.
                const awaiter = @atomicRmw(?*Fiber, &fiber.awaiter, .Xchg, Fiber.finished, .acq_rel);
                if (awaiter) |f| {
                    assert(f != Fiber.finished);
                    k.schedule(thread, .{ .head = f, .tail = f });
                }
            },
            .group_finish => |finished| groupMemberFinished(k, finished.fiber, finished.state),
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
        closure.kqueue.yield(null, .{ .complete_future = fiber });
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
            .recancel = recancel,
            .swapCancelProtection = swapCancelProtection,
            .checkCancel = checkCancelVTable,
            .futexWait = futexWait,
            .futexWaitUncancelable = futexWaitUncancelable,
            .futexWake = futexWake,
            .operate = operate,
            .batchAwaitAsync = batchAwaitAsync,
            .batchAwaitConcurrent = batchAwaitConcurrent,
            .batchCancel = batchCancel,
            .dirCreateDir = comptime threadedAdapter("dirCreateDir"),
            .dirCreateDirPath = comptime threadedAdapter("dirCreateDirPath"),
            .dirCreateDirPathOpen = comptime threadedAdapter("dirCreateDirPathOpen"),
            .dirOpenDir = comptime threadedAdapter("dirOpenDir"),
            .dirStat = comptime threadedAdapter("dirStat"),
            .dirStatFile = comptime threadedAdapter("dirStatFile"),
            .dirAccess = comptime threadedAdapter("dirAccess"),
            .dirCreateFile = comptime threadedAdapter("dirCreateFile"),
            .dirCreateFileAtomic = comptime threadedAdapter("dirCreateFileAtomic"),
            .dirOpenFile = comptime threadedAdapter("dirOpenFile"),
            .dirClose = comptime threadedAdapter("dirClose"),
            .dirRead = comptime threadedAdapter("dirRead"),
            .dirRealPath = comptime threadedAdapter("dirRealPath"),
            .dirRealPathFile = comptime threadedAdapter("dirRealPathFile"),
            .dirDeleteFile = comptime threadedAdapter("dirDeleteFile"),
            .dirDeleteDir = comptime threadedAdapter("dirDeleteDir"),
            .dirRename = comptime threadedAdapter("dirRename"),
            .dirRenamePreserve = comptime threadedAdapter("dirRenamePreserve"),
            .dirSymLink = comptime threadedAdapter("dirSymLink"),
            .dirReadLink = comptime threadedAdapter("dirReadLink"),
            .dirSetOwner = comptime threadedAdapter("dirSetOwner"),
            .dirSetFileOwner = comptime threadedAdapter("dirSetFileOwner"),
            .dirSetPermissions = comptime threadedAdapter("dirSetPermissions"),
            .dirSetFilePermissions = comptime threadedAdapter("dirSetFilePermissions"),
            .dirSetTimestamps = comptime threadedAdapter("dirSetTimestamps"),
            .dirHardLink = comptime threadedAdapter("dirHardLink"),
            .fileStat = comptime threadedAdapter("fileStat"),
            .fileLength = comptime threadedAdapter("fileLength"),
            .fileClose = comptime threadedAdapter("fileClose"),
            .fileWritePositional = comptime threadedAdapter("fileWritePositional"),
            .fileWriteFileStreaming = comptime threadedAdapter("fileWriteFileStreaming"),
            .fileWriteFilePositional = comptime threadedAdapter("fileWriteFilePositional"),
            .fileReadPositional = comptime threadedAdapter("fileReadPositional"),
            .fileSeekBy = comptime threadedAdapter("fileSeekBy"),
            .fileSeekTo = comptime threadedAdapter("fileSeekTo"),
            .fileSync = comptime threadedAdapter("fileSync"),
            .fileIsTty = comptime threadedAdapter("fileIsTty"),
            .fileEnableAnsiEscapeCodes = comptime threadedAdapter("fileEnableAnsiEscapeCodes"),
            .fileSupportsAnsiEscapeCodes = comptime threadedAdapter("fileSupportsAnsiEscapeCodes"),
            .fileSetLength = comptime threadedAdapter("fileSetLength"),
            .fileSetOwner = comptime threadedAdapter("fileSetOwner"),
            .fileSetPermissions = comptime threadedAdapter("fileSetPermissions"),
            .fileSetTimestamps = comptime threadedAdapter("fileSetTimestamps"),
            .fileLock = comptime threadedAdapter("fileLock"),
            .fileTryLock = comptime threadedAdapter("fileTryLock"),
            .fileUnlock = comptime threadedAdapter("fileUnlock"),
            .fileDowngradeLock = comptime threadedAdapter("fileDowngradeLock"),
            .fileRealPath = comptime threadedAdapter("fileRealPath"),
            .fileHardLink = comptime threadedAdapter("fileHardLink"),
            .fileMemoryMapCreate = comptime threadedAdapter("fileMemoryMapCreate"),
            .fileMemoryMapDestroy = comptime threadedAdapter("fileMemoryMapDestroy"),
            .fileMemoryMapSetLength = comptime threadedAdapter("fileMemoryMapSetLength"),
            .fileMemoryMapRead = comptime threadedAdapter("fileMemoryMapRead"),
            .fileMemoryMapWrite = comptime threadedAdapter("fileMemoryMapWrite"),
            .processExecutableOpen = comptime threadedAdapter("processExecutableOpen"),
            .processExecutablePath = comptime threadedAdapter("processExecutablePath"),
            .lockStderr = comptime threadedAdapter("lockStderr"),
            .tryLockStderr = comptime threadedAdapter("tryLockStderr"),
            .unlockStderr = comptime threadedAdapter("unlockStderr"),
            .processCurrentPath = comptime threadedAdapter("processCurrentPath"),
            .processSetCurrentDir = comptime threadedAdapter("processSetCurrentDir"),
            .processSetCurrentPath = comptime threadedAdapter("processSetCurrentPath"),
            .processReplace = comptime threadedAdapter("processReplace"),
            .processReplacePath = processReplacePath,
            .processSpawn = comptime threadedAdapter("processSpawn"),
            .processSpawnPath = processSpawnPath,
            .childWait = comptime threadedAdapter("childWait"),
            .childKill = comptime threadedAdapter("childKill"),
            .progressParentFile = comptime threadedAdapter("progressParentFile"),
            .now = now,
            .clockResolution = comptime threadedAdapter("clockResolution"),
            .sleep = sleep,
            .random = comptime threadedAdapter("random"),
            .randomSecure = comptime threadedAdapter("randomSecure"),
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
            .netInterfaceNameResolve = comptime threadedAdapter("netInterfaceNameResolve"),
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
        .cancel_protection = .unblocked,
        .awaiting_completions = .empty,
        .batch_waiter = .{},
    };
    closure.* = .{
        .kqueue = k,
        .fiber = fiber,
        .start = start,
        .result_align = result_alignment,
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

/// Request cancellation without scheduling a fiber until its park slot
/// grants ownership. Protected operations retain the request for later.
fn requestCancel(k: *Kqueue, fiber: *Fiber) void {
    if (@cmpxchgStrong(?*Thread, &fiber.cancel_thread, null, Thread.canceling, .acq_rel, .acquire) == null) {
        if (@atomicLoad(Io.CancelProtection, &fiber.cancel_protection, .acquire) == .unblocked)
            k.wakeWaiter(&fiber.batch_waiter);
    }
}

fn cancel(
    userdata: ?*anyopaque,
    any_future: *Io.AnyFuture,
    result: []u8,
    result_alignment: std.mem.Alignment,
) void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    k.requestCancel(@ptrCast(@alignCast(any_future)));
    await(userdata, any_future, result, result_alignment);
}

fn cancelRequested(userdata: ?*anyopaque) bool {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    return @atomicLoad(?*Thread, &Thread.current().currentFiber().cancel_thread, .acquire) != null;
}

/// Consumes a pending cancelation request. Only the next cancelation
/// point signals; `recancel` re-arms a consumed request.
fn checkCancel(k: *Kqueue) error{Canceled}!void {
    _ = k;
    const fiber = Thread.current().currentFiber();
    if (@atomicLoad(Io.CancelProtection, &fiber.cancel_protection, .acquire) == .blocked) return;
    if (@atomicRmw(?*Thread, &fiber.cancel_thread, .Xchg, null, .acq_rel) != null) {
        return error.Canceled;
    }
}

fn checkCancelVTable(userdata: ?*anyopaque) Io.Cancelable!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    return k.checkCancel();
}

fn recancel(userdata: ?*anyopaque) void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    const fiber = Thread.current().currentFiber();
    if (@cmpxchgStrong(
        ?*Thread,
        &fiber.cancel_thread,
        null,
        Thread.canceling,
        .acq_rel,
        .acquire,
    )) |cancel_thread| assert(cancel_thread == Thread.canceling); // recancel without a consumed request
}

fn swapCancelProtection(userdata: ?*anyopaque, new: Io.CancelProtection) Io.CancelProtection {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    const fiber = Thread.current().currentFiber();
    return @atomicRmw(Io.CancelProtection, &fiber.cancel_protection, .Xchg, new, .acq_rel);
}

fn futexWait(userdata: ?*anyopaque, ptr: *const u32, expected: u32, timeout: Io.Timeout) Io.Cancelable!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const fiber = Thread.current().currentFiber();
    @atomicStore(?*Fiber, &fiber.batch_waiter.parked, null, .release);
    try k.checkCancel();
    const owner = Thread.current();
    const deadline = timeout.toTimestamp(k.io());
    if (deadline) |when| {
        if (when.durationFromNow(k.io()).raw.toNanoseconds() <= 0) return;
    }
    k.futex_mutex.lock();
    if (@atomicLoad(u32, ptr, .acquire) != expected) {
        k.futex_mutex.unlock();
        return;
    }
    fiber.futex_ptr = ptr;
    k.futex_waiters.append(&fiber.futex_node);
    k.futex_mutex.unlock();
    if (deadline) |when|
        batchTimerChange(owner.kq_fd, fiber, timerMilliseconds(when.durationFromNow(k.io()).raw.toNanoseconds()), false);
    k.yield(null, .{ .register_batch_waiter = &fiber.batch_waiter.parked });
    k.futex_mutex.lock();
    if (fiber.futex_ptr != null) {
        k.futex_waiters.remove(&fiber.futex_node);
        fiber.futex_ptr = null;
    }
    k.futex_mutex.unlock();
    if (deadline != null) batchTimerChange(owner.kq_fd, fiber, 0, true);
    try k.checkCancel();
}

fn futexWaitUncancelable(userdata: ?*anyopaque, ptr: *const u32, expected: u32) void {
    const previous = swapCancelProtection(userdata, .blocked);
    defer _ = swapCancelProtection(userdata, previous);
    futexWait(userdata, ptr, expected, .none) catch unreachable;
}

fn futexWake(userdata: ?*anyopaque, ptr: *const u32, max_waiters: u32) void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    k.futex_mutex.lock();
    defer k.futex_mutex.unlock();
    var node = k.futex_waiters.first;
    var remaining = max_waiters;
    while (node) |n| {
        if (remaining == 0) break;
        node = n.next;
        const fiber: *Fiber = @fieldParentPtr("futex_node", n);
        if (fiber.futex_ptr != ptr) continue;
        k.futex_waiters.remove(n);
        fiber.futex_ptr = null;
        remaining -= 1;
        k.wakeWaiter(&fiber.batch_waiter);
    }
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
        created.* = .{};
        if (type_erased.token.cmpxchgStrong(null, created, .acq_rel, .acquire)) |existing| {
            k.gpa.destroy(created);
            break :s @ptrCast(@alignCast(existing));
        }
        break :s created;
    };
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
        .cancel_protection = .unblocked,
        .awaiting_completions = .empty,
        .batch_waiter = .{},
    };
    closure.* = .{
        .kqueue = k,
        .fiber = fiber,
        .start = undefined,
        .result_align = .@"1",
        .group = state,
        .group_start = start,
    };
    @memcpy(closure.contextPointer(), context);

    state.mutex.lock();
    state.members.append(&fiber.group_node);
    if (state.canceling) fiber.cancel_thread = Thread.canceling;
    state.mutex.unlock();
    k.schedule(.current(), .{ .head = fiber, .tail = fiber });
}

fn groupFinish(k: *Kqueue, fiber: *Fiber, state: *GroupState) noreturn {
    k.yield(null, .{ .group_finish = .{ .fiber = fiber, .state = state } });
    unreachable;
}

/// Runs only after the member's stack is no longer executing. A group
/// must not report completion while a member can still access its backend.
fn groupMemberFinished(k: *Kqueue, fiber: *Fiber, state: *GroupState) void {
    state.mutex.lock();
    state.members.remove(&fiber.group_node);
    const awaiter = if (state.members.first == null) state.awaiter else null;
    k.recycle(fiber);
    if (awaiter) |f| k.wakeWaiter(&f.batch_waiter);
    state.mutex.unlock();
}

fn groupRequestCancel(k: *Kqueue, state: *GroupState) void {
    state.mutex.lock();
    defer state.mutex.unlock();
    state.canceling = true;
    var node = state.members.first;
    while (node) |member| : (node = member.next) {
        const fiber: *Fiber = @fieldParentPtr("group_node", member);
        k.requestCancel(fiber);
    }
}

fn groupAwait(userdata: ?*anyopaque, type_erased: *Io.Group, initial_token: *anyopaque) Io.Cancelable!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const state: *GroupState = @ptrCast(@alignCast(initial_token));
    const fiber = Thread.current().currentFiber();
    var canceled = false;
    while (true) {
        @atomicStore(?*Fiber, &fiber.batch_waiter.parked, null, .release);
        k.checkCancel() catch {
            canceled = true;
            k.groupRequestCancel(state);
        };
        state.mutex.lock();
        if (state.members.first == null) {
            state.mutex.unlock();
            break;
        }
        state.awaiter = fiber;
        state.mutex.unlock();
        k.yield(null, .{ .register_batch_waiter = &fiber.batch_waiter.parked });
    }
    type_erased.token.store(null, .release);
    k.gpa.destroy(state);
    if (canceled) return error.Canceled;
}

fn groupCancel(userdata: ?*anyopaque, group: *Io.Group, token: *anyopaque) void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    k.groupRequestCancel(@ptrCast(@alignCast(token)));
    groupAwait(userdata, group, token) catch |err| switch (err) {
        error.Canceled => k.io().recancel(),
    };
}

/// Every forwarded vtable call must translate Kqueue userdata to the owned
/// Threaded instance. Passing it through unchanged corrupts allocator and
/// random state even when neighboring stateless POSIX helpers happen to work.
fn threadedAdapter(comptime name: []const u8) @FieldType(Io.VTable, name) {
    const Fn = @typeInfo(@typeInfo(@FieldType(Io.VTable, name)).pointer.child).@"fn";
    const P = Fn.param_types;
    const R = Fn.return_type.?;
    const Adapter = struct {
        fn call(args: anytype) R {
            const k: *Kqueue = @ptrCast(@alignCast(args[0]));
            const Errors = switch (@typeInfo(R)) {
                .error_union => |info| info.error_set,
                .error_set => R,
                else => error{},
            };
            if (comptime errorSetHasCanceled(Errors)) try k.checkCancel();
            var forwarded = args;
            forwarded[0] = &k.threaded;
            return @call(.auto, @field(k.threaded.io().vtable, name), forwarded);
        }
        fn call1(a0: P[0].?) R {
            return call(.{a0});
        }
        fn call2(a0: P[0].?, a1: P[1].?) R {
            return call(.{ a0, a1 });
        }
        fn call3(a0: P[0].?, a1: P[1].?, a2: P[2].?) R {
            return call(.{ a0, a1, a2 });
        }
        fn call4(a0: P[0].?, a1: P[1].?, a2: P[2].?, a3: P[3].?) R {
            return call(.{ a0, a1, a2, a3 });
        }
        fn call5(a0: P[0].?, a1: P[1].?, a2: P[2].?, a3: P[3].?, a4: P[4].?) R {
            return call(.{ a0, a1, a2, a3, a4 });
        }
        fn call6(a0: P[0].?, a1: P[1].?, a2: P[2].?, a3: P[3].?, a4: P[4].?, a5: P[5].?) R {
            return call(.{ a0, a1, a2, a3, a4, a5 });
        }
        fn call7(a0: P[0].?, a1: P[1].?, a2: P[2].?, a3: P[3].?, a4: P[4].?, a5: P[5].?, a6: P[6].?) R {
            return call(.{ a0, a1, a2, a3, a4, a5, a6 });
        }
    };
    return switch (P.len) {
        1 => &Adapter.call1,
        2 => &Adapter.call2,
        3 => &Adapter.call3,
        4 => &Adapter.call4,
        5 => &Adapter.call5,
        6 => &Adapter.call6,
        7 => &Adapter.call7,
        else => @compileError("unsupported Threaded vtable arity"),
    };
}

fn errorSetHasCanceled(comptime Errors: type) bool {
    const names = @typeInfo(Errors).error_set.error_names orelse return true;
    for (names) |name| if (std.mem.eql(u8, name, "Canceled")) return true;
    return false;
}

fn processSpawnPath(
    userdata: ?*anyopaque,
    dir: Dir,
    options: std.process.SpawnOptions,
) std.process.SpawnError!std.process.Child {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    try k.checkCancel();
    var arena_allocator = std.heap.ArenaAllocator.init(k.gpa);
    defer arena_allocator.deinit();
    const arena = arena_allocator.allocator();
    const argv = try arena.alloc([]const u8, options.argv.len);
    argv[0] = try argv0Path(arena, options.argv[0]);
    @memcpy(argv[1..], options.argv[1..]);
    var path_options = options;
    path_options.argv = argv;
    path_options.cwd = .{ .dir = dir };
    const fallback = k.threaded.io();
    return fallback.vtable.processSpawn(fallback.userdata, path_options);
}

fn processReplacePath(
    userdata: ?*anyopaque,
    dir: Dir,
    options: std.process.ReplaceOptions,
) std.process.ReplaceError {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    try k.checkCancel();
    var arena_allocator = std.heap.ArenaAllocator.init(k.gpa);
    defer arena_allocator.deinit();
    const arena = arena_allocator.allocator();
    const argv = try arena.alloc([]const u8, options.argv.len);
    argv[0] = try argv0Path(arena, options.argv[0]);
    @memcpy(argv[1..], options.argv[1..]);
    var path_options = options;
    path_options.argv = argv;
    path_options.expand_arg0 = .no_expand;
    try Io.Threaded.fchdir(dir.handle);
    const fallback = k.threaded.io();
    return fallback.vtable.processReplace(fallback.userdata, path_options);
}

fn argv0Path(gpa: Allocator, arg0: []const u8) Allocator.Error![]const u8 {
    if (std.mem.findScalar(u8, arg0, '/') != null) return arg0;
    return std.fmt.allocPrint(gpa, "./{s}", .{arg0});
}

fn now(userdata: ?*anyopaque, clock: Io.Clock) Io.Timestamp {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    _ = k;
    return Io.Threaded.nowPosix(clock);
}

/// Sleep shares the cancellable park slot with socket and batch waits.
fn sleep(userdata: ?*anyopaque, timeout: Io.Timeout) Io.Cancelable!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const fiber = Thread.current().currentFiber();
    const deadline = timeout.toTimestamp(k.io());
    while (true) {
        @atomicStore(?*Fiber, &fiber.batch_waiter.parked, null, .release);
        try k.checkCancel();
        const owner = Thread.current();
        if (deadline) |when| {
            const remaining = when.durationFromNow(k.io()).raw.toNanoseconds();
            if (remaining <= 0) return;
            batchTimerChange(owner.kq_fd, fiber, timerMilliseconds(remaining), false);
        }
        k.yield(null, .{ .register_batch_waiter = &fiber.batch_waiter.parked });
        if (deadline != null) batchTimerChange(owner.kq_fd, fiber, 0, true);
        // Recheck the actual deadline after a stale or protected wake.
    }
}

fn timerMilliseconds(nanoseconds: i96) i64 {
    return @intCast(@min(std.math.maxInt(i64), @max(1, @divTrunc(@max(0, @as(i128, nanoseconds)) + std.time.ns_per_ms - 1, std.time.ns_per_ms))));
}

/// Sets `O_NONBLOCK` and `FD_CLOEXEC` on an existing socket. Kqueue-driven
/// sockets must not block the worker thread; `accept` has no per-call
/// `MSG_DONTWAIT`, so a listening socket (and each accepted socket) needs
/// the flag applied here.
fn setSocketFlagsPosix(k: *Kqueue, socket_fd: posix.fd_t) error{ Unexpected, Canceled }!void {
    var fl_flags: usize = while (true) {
        try k.checkCancel();
        const rc = posix.system.fcntl(socket_fd, posix.F.GETFL, @as(usize, 0));
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
        switch (posix.errno(posix.system.fcntl(socket_fd, posix.F.SETFL, fl_flags))) {
            .SUCCESS => break,
            .INTR => continue,
            .CANCELED => return error.Canceled,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
    while (true) {
        try k.checkCancel();
        switch (posix.errno(posix.system.fcntl(socket_fd, posix.F.SETFD, @as(usize, posix.FD_CLOEXEC)))) {
            .SUCCESS => return,
            .INTR => continue,
            .CANCELED => return error.Canceled,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn netListenIp(
    userdata: ?*anyopaque,
    address: *const net.IpAddress,
    options: net.IpAddress.ListenOptions,
) net.IpAddress.ListenError!net.Socket {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const family = Io.Threaded.posixAddressFamily(address);
    const socket_fd = try openSocketPosix(k, family, .{
        .mode = options.mode,
        .protocol = options.protocol,
    });
    errdefer closeFd(socket_fd);
    try setSocketFlagsPosix(k, socket_fd);

    if (options.reuse_address) {
        try setSocketOption(k, socket_fd, posix.SOL.SOCKET, posix.SO.REUSEADDR, 1);
        if (@hasDecl(posix.SO, "REUSEPORT"))
            try setSocketOption(k, socket_fd, posix.SOL.SOCKET, posix.SO.REUSEPORT, 1);
    }

    var storage: Io.Threaded.PosixAddress = undefined;
    var addr_len = Io.Threaded.addressToPosix(address, &storage);
    try posixBind(k, socket_fd, &storage.any, addr_len);

    const backlog: c_uint = if (options.kernel_backlog > std.math.maxInt(c_uint))
        std.math.maxInt(c_uint)
    else
        @intCast(options.kernel_backlog);
    while (true) {
        try k.checkCancel();
        switch (posix.errno(posix.system.listen(socket_fd, backlog))) {
            .SUCCESS => break,
            .INTR => continue,
            .CANCELED => return error.Canceled,
            .ADDRINUSE => return error.AddressInUse,
            .BADF => |err| return errnoBug(err), // File descriptor used after closed.
            else => |err| return posix.unexpectedErrno(err),
        }
    }

    try posixGetSockName(k, socket_fd, &storage.any, &addr_len);
    return .{ .handle = socket_fd, .address = Io.Threaded.addressFromPosix(&storage) };
}

fn netAccept(
    userdata: ?*anyopaque,
    server: net.Socket.Handle,
    options: net.Server.AcceptOptions,
) net.Server.AcceptError!net.Socket {
    _ = options;
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    while (true) {
        try k.checkCancel();
        var storage: Io.Threaded.PosixAddress = undefined;
        var addr_len: posix.socklen_t = @sizeOf(Io.Threaded.PosixAddress);
        const rc = posix.system.accept(server, &storage.any, &addr_len);
        switch (posix.errno(rc)) {
            .SUCCESS => {
                const socket_fd: posix.fd_t = @intCast(rc);
                errdefer closeFd(socket_fd);
                try setSocketFlagsPosix(k, socket_fd);
                return .{ .handle = socket_fd, .address = Io.Threaded.addressFromPosix(&storage) };
            },
            .INTR => continue,
            // The listening socket is nonblocking; park until a connection
            // is pending.
            .AGAIN => try waitReady(k, @bitCast(@as(isize, server)), std.c.EVFILT.READ),
            .BADF => |err| return errnoBug(err), // File descriptor used after closed.
            .CONNABORTED => return error.ConnectionAborted,
            .FAULT => |err| return errnoBug(err),
            .INVAL => return error.SocketNotListening,
            .NOTSOCK => |err| return errnoBug(err),
            .MFILE => return error.ProcessFdQuotaExceeded,
            .NFILE => return error.SystemFdQuotaExceeded,
            .NOBUFS => return error.SystemResources,
            .NOMEM => return error.SystemResources,
            .OPNOTSUPP => |err| return errnoBug(err),
            .PROTO => return error.ProtocolFailure,
            .PERM => return error.BlockedByFirewall,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
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
    if (options.reuse_port_load_balance) {
        if (comptime builtin.os.tag != .freebsd) return error.OptionUnsupported;
        try setSocketOption(k, socket_fd, posix.SOL.SOCKET, posix.SO.REUSEPORT_LB, 1);
    } else if (options.reuse_port) {
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
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const family = Io.Threaded.posixAddressFamily(address);
    const socket_fd = try openSocketPosix(k, family, .{
        .mode = options.mode,
        .protocol = options.protocol,
    });
    errdefer closeFd(socket_fd);
    var storage: Io.Threaded.PosixAddress = undefined;
    var addr_len = Io.Threaded.addressToPosix(address, &storage);
    posixConnect(k, socket_fd, &storage.any, addr_len, options.timeout) catch |err| switch (err) {
        error.FileNotFound, error.NotDir, error.SymLinkLoop => return error.Unexpected,
        else => |e| return e,
    };
    try posixGetSockName(k, socket_fd, &storage.any, &addr_len);
    return .{ .handle = socket_fd, .address = Io.Threaded.addressFromPosix(&storage) };
}

fn posixConnect(
    k: *Kqueue,
    socket_fd: std.c.fd_t,
    addr: *const posix.sockaddr,
    addr_len: posix.socklen_t,
    timeout: Io.Timeout,
) ConnectError!void {
    while (true) {
        try k.checkCancel();
        switch (std.c.errno(std.c.connect(socket_fd, addr, addr_len))) {
            .SUCCESS => return,
            .INTR => continue,
            // The socket is nonblocking; the outcome is determined once the
            // socket becomes writable.
            .AGAIN, .INPROGRESS => return connectFinish(k, socket_fd, timeout),
            .ADDRNOTAVAIL => return error.AddressUnavailable,
            .AFNOSUPPORT => return error.AddressFamilyUnsupported,
            .ALREADY => return error.ConnectionPending,
            .CONNREFUSED => return error.ConnectionRefused,
            .CONNRESET => return error.ConnectionResetByPeer,
            .HOSTUNREACH => return error.HostUnreachable,
            .NETUNREACH => return error.NetworkUnreachable,
            .TIMEDOUT => return error.Timeout,
            .ACCES => return error.AccessDenied,
            .NETDOWN => return error.NetworkDown,
            .BADF => |err| return errnoBug(err), // File descriptor used after closed.
            .CONNABORTED => |err| return errnoBug(err),
            .FAULT => |err| return errnoBug(err),
            .ISCONN => |err| return errnoBug(err),
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .LOOP => return error.SymLinkLoop,
            .NOTSOCK => |err| return errnoBug(err),
            .PERM => |err| return errnoBug(err),
            .PROTOTYPE => |err| return errnoBug(err),
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

const ConnectError = error{
    FileNotFound,
    NotDir,
    SymLinkLoop,
    Canceled,
    AccessDenied,
    AddressFamilyUnsupported,
    AddressUnavailable,
    ConnectionPending,
    ConnectionRefused,
    ConnectionResetByPeer,
    HostUnreachable,
    NetworkDown,
    NetworkUnreachable,
    SystemResources,
    Timeout,
    Unexpected,
};

fn connectFinish(k: *Kqueue, socket_fd: std.c.fd_t, timeout: Io.Timeout) ConnectError!void {
    try waitReadyTimeout(k, @bitCast(@as(isize, socket_fd)), std.c.EVFILT.WRITE, timeout);
    var value: c_int = undefined;
    var len: posix.socklen_t = @sizeOf(c_int);
    switch (std.c.errno(std.c.getsockopt(socket_fd, posix.SOL.SOCKET, posix.SO.ERROR, &value, &len))) {
        .SUCCESS => {},
        .BADF => |err| return errnoBug(err), // File descriptor used after closed.
        .NOTSOCK => |err| return errnoBug(err),
        .INVAL => |err| return errnoBug(err),
        .FAULT => |err| return errnoBug(err),
        else => |err| return posix.unexpectedErrno(err),
    }
    return switch (@as(std.c.E, @fromBackingInt(@intCast(@as(u16, @truncate(@as(u32, @bitCast(value)))))))) {
        .SUCCESS => {},
        .ADDRNOTAVAIL => error.AddressUnavailable,
        .CONNREFUSED => error.ConnectionRefused,
        .CONNRESET => error.ConnectionResetByPeer,
        .HOSTUNREACH => error.HostUnreachable,
        .NETUNREACH => error.NetworkUnreachable,
        .NETDOWN => error.NetworkDown,
        .TIMEDOUT => error.Timeout,
        .ACCES => error.AccessDenied,
        .PERM => error.AccessDenied,
        else => |err| posix.unexpectedErrno(err),
    };
}

fn netListenUnix(
    userdata: ?*anyopaque,
    address: *const net.UnixAddress,
    options: net.UnixAddress.ListenOptions,
) net.UnixAddress.ListenError!net.Socket.Handle {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const socket_fd = openSocketPosix(k, posix.AF.UNIX, .{ .mode = .stream }) catch |err| switch (err) {
        error.ProtocolUnsupportedBySystem,
        error.ProtocolUnsupportedByAddressFamily,
        error.SocketModeUnsupported,
        => return error.AddressFamilyUnsupported,
        error.OptionUnsupported => return error.Unexpected,
        else => |e| return e,
    };
    errdefer closeFd(socket_fd);

    var storage: Io.Threaded.UnixAddress = undefined;
    const addr_len = Io.Threaded.addressUnixToPosix(address, &storage);
    try posixBindUnix(socket_fd, &storage.any, addr_len);
    while (true) {
        try k.checkCancel();
        switch (posix.errno(posix.system.listen(socket_fd, options.kernel_backlog))) {
            .SUCCESS => break,
            .INTR => continue,
            .ADDRINUSE => return error.AddressInUse,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
    return socket_fd;
}

fn netConnectUnix(
    userdata: ?*anyopaque,
    address: *const net.UnixAddress,
) net.UnixAddress.ConnectError!net.Socket.Handle {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const socket_fd = openSocketPosix(k, posix.AF.UNIX, .{ .mode = .stream }) catch |err| switch (err) {
        error.ProtocolUnsupportedByAddressFamily,
        error.SocketModeUnsupported,
        => return error.AddressFamilyUnsupported,
        error.OptionUnsupported => return error.Unexpected,
        else => |e| return e,
    };
    errdefer closeFd(socket_fd);
    var storage: Io.Threaded.UnixAddress = undefined;
    const addr_len = Io.Threaded.addressUnixToPosix(address, &storage);
    posixConnect(k, socket_fd, &storage.any, addr_len, .none) catch |err| switch (err) {
        error.AddressUnavailable,
        error.ConnectionPending,
        error.ConnectionResetByPeer,
        error.HostUnreachable,
        error.NetworkUnreachable,
        error.Timeout,
        => return error.Unexpected, // only possible for IP sockets
        else => |e| return e,
    };
    return socket_fd;
}

fn posixBindUnix(socket_fd: std.c.fd_t, addr: *const posix.sockaddr, addr_len: posix.socklen_t) error{
    AccessDenied,
    AddressInUse,
    AddressFamilyUnsupported,
    AddressUnavailable,
    SystemResources,
    SymLinkLoop,
    FileNotFound,
    NotDir,
    ReadOnlyFileSystem,
    PermissionDenied,
    Unexpected,
}!void {
    while (true) {
        switch (std.c.errno(std.c.bind(socket_fd, addr, addr_len))) {
            .SUCCESS => return,
            .INTR => continue,
            .ACCES => return error.AccessDenied,
            .ADDRINUSE => return error.AddressInUse,
            .AFNOSUPPORT => return error.AddressFamilyUnsupported,
            .ADDRNOTAVAIL => return error.AddressUnavailable,
            .NOMEM => return error.SystemResources,
            .LOOP => return error.SymLinkLoop,
            .NOENT => return error.FileNotFound,
            .NOTDIR => return error.NotDir,
            .ROFS => return error.ReadOnlyFileSystem,
            .PERM => return error.PermissionDenied,
            .BADF => |err| return errnoBug(err), // File descriptor used after closed.
            .INVAL => |err| return errnoBug(err), // invalid parameters
            .NOTSOCK => |err| return errnoBug(err), // invalid `sockfd`
            .FAULT => |err| return errnoBug(err), // invalid `addr` pointer
            .NAMETOOLONG => |err| return errnoBug(err),
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn netSend(
    userdata: ?*anyopaque,
    handle: net.Socket.Handle,
    outgoing_messages: []net.OutgoingMessage,
    flags: net.SendFlags,
) struct { ?net.Socket.SendError, usize } {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));

    const posix_flags = posixSendFlags(flags);

    for (outgoing_messages, 0..) |*msg, i| {
        netSendOne(k, handle, msg, posix_flags) catch |err| return .{ err, i };
    }

    return .{ null, outgoing_messages.len };
}

fn posixSendFlags(flags: net.SendFlags) u32 {
    return @as(u32, if (@hasDecl(posix.MSG, "CONFIRM") and flags.confirm) posix.MSG.CONFIRM else 0) |
        @as(u32, if (@hasDecl(posix.MSG, "DONTROUTE") and flags.dont_route) posix.MSG.DONTROUTE else 0) |
        @as(u32, if (@hasDecl(posix.MSG, "EOR") and flags.eor) posix.MSG.EOR else 0) |
        @as(u32, if (@hasDecl(posix.MSG, "OOB") and flags.oob) posix.MSG.OOB else 0) |
        @as(u32, if (@hasDecl(posix.MSG, "FASTOPEN") and flags.fastopen) posix.MSG.FASTOPEN else 0) |
        posix.MSG.NOSIGNAL;
}

fn netSendOne(
    k: *Kqueue,
    handle: net.Socket.Handle,
    message: *net.OutgoingMessage,
    flags: u32,
) net.Socket.SendError!void {
    var addr: Io.Threaded.PosixAddress = undefined;
    var message_iovec: posix.iovec_const = .{ .base = @constCast(message.data_ptr), .len = message.data_len };
    const msg: posix.msghdr_const = .{
        .name = &addr.any,
        .namelen = Io.Threaded.addressToPosix(message.address, &addr),
        .iov = (&message_iovec)[0..1],
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
    flags: net.SendFlags,
) union(enum) { full, partial: usize, blocked, err: struct { net.Socket.SendError, usize } } {
    var i: usize = 0;
    while (i < messages.len) : (i += 1) {
        netSendOneNonblocking(handle, &messages[i], posixSendFlags(flags)) catch |err| return switch (err) {
            error.WouldBlock => if (i == 0) .blocked else .{ .partial = i },
            else => |e| .{ .err = .{ e, i } },
        };
    }
    return .full;
}

fn netSendOneNonblocking(handle: net.Socket.Handle, message: *net.OutgoingMessage, flags: u32) (net.Socket.SendError || error{WouldBlock})!void {
    var addr: Io.Threaded.PosixAddress = undefined;
    var message_iovec: posix.iovec_const = .{ .base = @constCast(message.data_ptr), .len = message.data_len };
    const msg: posix.msghdr_const = .{
        .name = &addr.any,
        .namelen = Io.Threaded.addressToPosix(message.address, &addr),
        .iov = (&message_iovec)[0..1],
        .iovlen = 1,
        .control = if (message.control.len == 0) null else @constCast(message.control.ptr),
        .controllen = @intCast(message.control.len),
        .flags = 0,
    };
    while (true) {
        const rc = posix.system.sendmsg(handle, &msg, flags | posix.MSG.DONTWAIT);
        switch (posix.errno(rc)) {
            .SUCCESS => {
                message.data_len = @intCast(rc);
                return;
            },
            .INTR => continue,
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
}

/// One blocking operation; the batch machinery above is the concurrent
/// path. Network operations retry through `waitReady`; file operations
/// block the worker thread (no file async yet on this backend).
fn operate(userdata: ?*anyopaque, operation: Io.Operation) Io.Cancelable!Io.Operation.Result {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    try k.checkCancel();
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
                        waitReady(k, @bitCast(@as(isize, o.socket_handle)), std.c.EVFILT.READ) catch |e| switch (e) {
                            error.Canceled => return error.Canceled,
                            else => return .{ .net_receive = .{ e, 0 } },
                        };
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
                netSendOneNonblocking(o.socket_handle, &o.messages[i], posixSendFlags(o.flags)) catch |err| switch (err) {
                    error.WouldBlock => {
                        if (i != 0) break :r .{ null, i };
                        waitReady(k, @bitCast(@as(isize, o.socket_handle)), std.c.EVFILT.WRITE) catch |e| switch (e) {
                            error.Canceled => return error.Canceled,
                            else => break :r .{ e, 0 },
                        };
                        i -%= 1;
                        continue;
                    },
                    else => |e| break :r .{ e, i },
                };
            }
            break :r .{ null, o.messages.len };
        } },
        .net_read => |o| return .{
            .net_read = netRead(k, o.socket_handle, o.data) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => |e| e,
            },
        },
        .net_write => |o| return .{
            .net_write = netWrite(k, o.socket_handle, o.header, o.data, o.splat) catch |err| switch (err) {
                error.Canceled => return error.Canceled,
                else => |e| e,
            },
        },
        .file_read_streaming, .file_write_streaming => {
            try k.checkCancel();
            const fallback = k.threaded.io();
            return fallback.vtable.operate(fallback.userdata, operation);
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

const iovec = posix.iovec;
const iovec_const = posix.iovec_const;
const iovlen_t = @FieldType(posix.msghdr_const, "iovlen");
const splat_buffer_size = Io.Threaded.splat_buffer_size;

/// Performs one nonblocking `readv` attempt on a socket.
fn netReadOnce(handle: posix.fd_t, data: [][]u8) (Io.Operation.NetRead.Error || error{WouldBlock})!usize {
    var iovecs: [max_iovecs_len]iovec = undefined;
    var iovlen: iovlen_t = 0;
    var remaining: Io.Limit = .unlimited;
    for (data) |buf| addBuf(false, &iovecs, &iovlen, &remaining, buf);
    if (iovlen == 0) return 0;
    while (true) {
        const rc = posix.system.readv(handle, &iovecs, iovlen);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .INVAL => |err| return errnoBug(err),
            .FAULT => |err| return errnoBug(err),
            .BADF => |err| return errnoBug(err), // File descriptor used after closed.
            .NOBUFS => return error.SystemResources,
            .NOMEM => return error.SystemResources,
            .NOTCONN => return error.SocketUnconnected,
            .CONNRESET => return error.ConnectionResetByPeer,
            .TIMEDOUT => return error.ConnectionTimedOut,
            .PIPE => return error.SocketUnconnected,
            .NETDOWN => return error.NetworkDown,
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn netRead(k: *Kqueue, handle: posix.fd_t, data: [][]u8) (Io.Operation.NetRead.Error || Io.Cancelable)!usize {
    while (true) {
        try k.checkCancel();
        return netReadOnce(handle, data) catch |err| switch (err) {
            error.WouldBlock => {
                try waitReady(k, @bitCast(@as(isize, handle)), std.c.EVFILT.READ);
                continue;
            },
            else => |e| return e,
        };
    }
}

/// Performs one nonblocking `sendmsg` attempt on a socket, transferring
/// `header` followed by `data` and `splat`.
fn netWriteOnce(
    handle: posix.fd_t,
    header: []const u8,
    data: []const []const u8,
    splat: usize,
) (Io.Operation.NetWrite.Error || error{WouldBlock})!usize {
    var iovecs: [max_iovecs_len]iovec_const = undefined;
    var iovlen: iovlen_t = 0;
    var remaining: Io.Limit = .unlimited;
    addBuf(true, &iovecs, &iovlen, &remaining, header);
    for (data[0..data.len -| 1]) |bytes| addBuf(true, &iovecs, &iovlen, &remaining, bytes);
    const pattern: []const u8 = if (data.len == 0) &.{} else data[data.len - 1];
    var backup_buffer: [splat_buffer_size]u8 = undefined;
    if (iovecs.len - iovlen != 0 and remaining != .nothing) switch (splat) {
        0 => {},
        1 => addBuf(true, &iovecs, &iovlen, &remaining, pattern),
        else => switch (pattern.len) {
            0 => {},
            1 => {
                const splat_buffer = &backup_buffer;
                const memset_len = @min(splat_buffer.len, splat);
                const buf = splat_buffer[0..memset_len];
                @memset(buf, pattern[0]);
                addBuf(true, &iovecs, &iovlen, &remaining, buf);
                var remaining_splat = splat - buf.len;
                while (remaining_splat > splat_buffer.len and iovecs.len - iovlen != 0 and remaining != .nothing) {
                    assert(buf.len == splat_buffer.len);
                    addBuf(true, &iovecs, &iovlen, &remaining, splat_buffer);
                    remaining_splat -= splat_buffer.len;
                }
                addBuf(true, &iovecs, &iovlen, &remaining, splat_buffer[0..@min(remaining_splat, splat_buffer.len)]);
            },
            else => for (0..@min(splat, iovecs.len - iovlen)) |_| {
                if (remaining == .nothing) break;
                addBuf(true, &iovecs, &iovlen, &remaining, pattern);
            },
        },
    };
    if (iovlen == 0) return 0;
    const msg: posix.msghdr_const = .{
        .name = null,
        .namelen = 0,
        .iov = &iovecs,
        .iovlen = @intCast(iovlen),
        .control = null,
        .controllen = 0,
        .flags = 0,
    };
    while (true) {
        const rc = posix.system.sendmsg(handle, &msg, posix.MSG.NOSIGNAL);
        switch (posix.errno(rc)) {
            .SUCCESS => return @intCast(rc),
            .INTR => continue,
            .AGAIN => return error.WouldBlock,
            .ALREADY => return error.FastOpenAlreadyInProgress,
            .CONNRESET => return error.ConnectionResetByPeer,
            .NOBUFS => return error.SystemResources,
            .NOMEM => return error.SystemResources,
            .PIPE => return error.SocketUnconnected,
            .AFNOSUPPORT => return error.AddressFamilyUnsupported,
            .HOSTUNREACH => return error.HostUnreachable,
            .NETUNREACH => return error.NetworkUnreachable,
            .NOTCONN => return error.SocketUnconnected,
            .TIMEDOUT => return error.ConnectionTimedOut,
            .NETDOWN => return error.NetworkDown,
            .BADF => |err| return errnoBug(err), // File descriptor used after closed.
            .DESTADDRREQ => |err| return errnoBug(err), // The socket is not connection-mode, and no peer address is set.
            .FAULT => |err| return errnoBug(err), // An invalid user space address was specified for an argument.
            .INVAL => |err| return errnoBug(err), // Invalid argument passed.
            .ISCONN => |err| return errnoBug(err), // connection-mode socket was connected already but a recipient was specified
            .NOTSOCK => |err| return errnoBug(err), // The file descriptor sockfd does not refer to a socket.
            .OPNOTSUPP => |err| return errnoBug(err), // Some bit in the flags argument is inappropriate for the socket type.
            else => |err| return posix.unexpectedErrno(err),
        }
    }
}

fn netWrite(
    k: *Kqueue,
    handle: posix.fd_t,
    header: []const u8,
    data: []const []const u8,
    splat: usize,
) (Io.Operation.NetWrite.Error || Io.Cancelable)!usize {
    while (true) {
        try k.checkCancel();
        return netWriteOnce(handle, header, data, splat) catch |err| switch (err) {
            error.WouldBlock => {
                try waitReady(k, @bitCast(@as(isize, handle)), std.c.EVFILT.WRITE);
                continue;
            },
            else => |e| return e,
        };
    }
}

fn addBuf(
    comptime is_const: bool,
    vec: []if (is_const) iovec_const else iovec,
    vec_len: *iovlen_t,
    remaining: *Io.Limit,
    bytes: if (is_const) []const u8 else []u8,
) void {
    if (vec.len - vec_len.* == 0) return;
    const len = remaining.minInt(bytes.len);
    if (len == 0) return;
    vec[vec_len.*] = .{ .base = bytes.ptr, .len = len };
    vec_len.* += 1;
    remaining.* = remaining.subtract(len).?;
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

fn netInterfaceName(userdata: ?*anyopaque, interface: net.Interface) net.Interface.NameError!net.Interface.Name {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    try k.checkCancel();
    const libc = struct {
        extern "c" fn if_indextoname(c_uint, [*]u8) ?[*:0]u8;
    };
    var name: net.Interface.Name = undefined;
    if (libc.if_indextoname(interface.index, &name.bytes) == null) return error.InterfaceNotFound;
    return name;
}

/// The platform resolver is synchronous and can block this worker. Results
/// are delivered through Kqueue's queue operations, so a small result queue
/// may park the fiber without blocking its consumer or mixing I/O backends.
fn netLookup(
    userdata: ?*anyopaque,
    host_name: net.HostName,
    resolved: *Io.Queue(net.HostName.LookupResult),
    options: net.HostName.LookupOptions,
) net.HostName.LookupError!void {
    const k: *Kqueue = @ptrCast(@alignCast(userdata));
    const k_io = k.io();
    defer resolved.close(k_io);
    try k.checkCancel();
    var name_buffer: [net.HostName.max_len:0]u8 = undefined;
    @memcpy(name_buffer[0..host_name.bytes.len], host_name.bytes);
    name_buffer[host_name.bytes.len] = 0;
    var port_buffer: [8]u8 = undefined;
    const port_c = std.mem.printSentinel(&port_buffer, "{d}", .{options.port}, 0) catch unreachable;
    const hints: posix.addrinfo = .{
        .flags = .{ .CANONNAME = options.canonical_name_buffer != null, .NUMERICSERV = true },
        .family = if (options.family) |family| switch (family) {
            .ip4 => posix.AF.INET,
            .ip6 => posix.AF.INET6,
        } else posix.AF.UNSPEC,
        .socktype = posix.SOCK.STREAM,
        .protocol = posix.IPPROTO.TCP,
        .canonname = null,
        .addr = null,
        .addrlen = 0,
        .next = null,
    };
    var result: ?*posix.addrinfo = null;
    while (true) switch (posix.system.getaddrinfo(&name_buffer, port_c.ptr, &hints, &result)) {
        @as(posix.system.EAI, @fromBackingInt(@intCast(0))) => break,
        .SYSTEM => switch (posix.errno(-1)) {
            .INTR => {
                try k.checkCancel();
                continue;
            },
            else => |err| return posix.unexpectedErrno(err),
        },
        .ADDRFAMILY, .FAMILY => return error.AddressFamilyUnsupported,
        .AGAIN, .FAIL => return error.NameServerFailure,
        .MEMORY => return error.SystemResources,
        .NODATA, .NONAME => return error.UnknownHostName,
        else => return error.Unexpected,
    };
    defer if (result) |some| posix.system.freeaddrinfo(some);
    try k.checkCancel();
    var cursor = result;
    var address_count: usize = 0;
    var canonical: ?[*:0]const u8 = null;
    while (cursor) |info| : (cursor = info.next) {
        if (canonical == null) canonical = info.canonname;
        if (address_count == 15) continue;
        const address = info.addr orelse continue;
        if (info.family != posix.AF.INET and info.family != posix.AF.INET6) continue;
        resolved.putOne(k_io, .{ .address = Io.Threaded.addressFromPosix(@alignCast(@fieldParentPtr("any", address))) }) catch |err| switch (err) {
            error.Closed => unreachable, // Caller must wait until lookup returns.
            error.Canceled => return error.Canceled,
        };
        address_count += 1;
    }
    if (address_count == 0) return error.NoAddressReturned;
    if (canonical) |name| {
        if (Io.Threaded.copyCanon(options.canonical_name_buffer, std.mem.span(name))) |canon| {
            resolved.putOne(k_io, .{ .canonical_name = canon }) catch |err| switch (err) {
                error.Closed => unreachable,
                error.Canceled => return error.Canceled,
            };
        }
    }
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
        const flags: u32 = mode | if (Io.Threaded.socket_flags_unsupported) 0 else posix.SOCK.CLOEXEC | posix.SOCK.NONBLOCK;
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

/// Socket waits share a registration per worker and descriptor/filter.
/// A canceled waiter removes only its own list node, leaving peers armed.
fn waitReady(k: *Kqueue, ident: usize, filter: i16) error{ Canceled, SystemResources, Unexpected }!void {
    return waitReadyTimeout(k, ident, filter, .none) catch |err| switch (err) {
        error.Timeout => unreachable,
        else => |e| e,
    };
}

fn waitReadyTimeout(k: *Kqueue, ident: usize, filter: i16, timeout: Io.Timeout) error{ Canceled, SystemResources, Unexpected, Timeout }!void {
    const fiber = Thread.current().currentFiber();
    const waiter = &fiber.batch_waiter;
    @atomicStore(?*Fiber, &waiter.parked, null, .release);
    try k.checkCancel();
    const owner = Thread.current();
    const deadline = timeout.toTimestamp(k.io());
    var registration: SocketWait = .{};
    try socketWaitArm(k, &registration, owner, fiber, ident, filter);
    defer socketWaitDisarm(&registration);
    if (deadline) |when|
        batchTimerChange(owner.kq_fd, fiber, timerMilliseconds(when.durationFromNow(k.io()).raw.toNanoseconds()), false);
    k.yield(null, .{ .register_batch_waiter = &waiter.parked });
    socketWaitDisarm(&registration);
    if (deadline != null) batchTimerChange(owner.kq_fd, fiber, 0, true);
    try k.checkCancel();
    if (deadline) |when| {
        if (when.durationFromNow(k.io()).raw.toNanoseconds() <= 0) return error.Timeout;
    }
}

/// Share a single kernel registration without exposing a stack node in
/// udata. Late events can only wake current members of the same key.
fn socketWaitArm(k: *Kqueue, wait: *SocketWait, owner: *Thread, fiber: *Fiber, ident: usize, filter: i16) error{ SystemResources, Unexpected }!void {
    assert(wait.owner == null);
    wait.* = .{ .owner = owner, .key = .{ .ident = ident, .filter = filter }, .fiber = fiber };
    owner.wait_mutex.lock();
    defer owner.wait_mutex.unlock();
    const gop = owner.wait_queues.getOrPut(k.gpa, wait.key) catch {
        wait.owner = null;
        return error.SystemResources;
    };
    if (!gop.found_existing) gop.value_ptr.* = .{};
    gop.value_ptr.append(&wait.node);
    wait.registered = true;
    const changes = [_]posix.Kevent{.{
        .ident = ident,
        .filter = filter,
        .flags = std.c.EV.ADD | std.c.EV.ONESHOT,
        .fflags = 0,
        .data = 0,
        .udata = @backingInt(Completion.UserData.readiness),
    }};
    _ = kevent(owner.kq_fd, &changes, &.{}, null) catch |err| {
        gop.value_ptr.remove(&wait.node);
        if (gop.value_ptr.first == null) _ = owner.wait_queues.swapRemove(wait.key);
        wait.registered = false;
        wait.owner = null;
        return switch (err) {
            error.SystemResources => error.SystemResources,
            else => error.Unexpected,
        };
    };
}

fn socketWaitDisarm(wait: *SocketWait) void {
    const owner = wait.owner orelse return;
    owner.wait_mutex.lock();
    defer owner.wait_mutex.unlock();
    if (wait.registered) {
        const list = owner.wait_queues.getPtr(wait.key).?;
        list.remove(&wait.node);
        wait.registered = false;
        if (list.first == null) {
            _ = owner.wait_queues.swapRemove(wait.key);
            const changes = [_]posix.Kevent{.{
                .ident = wait.key.ident,
                .filter = @intCast(wait.key.filter),
                .flags = std.c.EV.DELETE,
                .fflags = 0,
                .data = 0,
                .udata = 0,
            }};
            _ = kevent(owner.kq_fd, &changes, &.{}, null) catch {};
        }
    }
    wait.owner = null;
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
/// complete move to `completed`; the others join the shared readiness
/// registration and stay in `submitted` for the next wake.
fn batchDrainSubmitted(k: *Kqueue, b: *Io.Batch, registrations: []SocketWait) Io.Cancelable!void {
    const thread: *Thread = .current();
    const fiber = thread.currentFiber();
    var prev_index: Io.Operation.OptionalIndex = .none;
    var index = b.submitted.head;
    while (index != .none) {
        const storage = &b.storage[index.toIndex()];
        const submission = storage.submission;
        const next_index = submission.node.next;
        var completed_inline = true;
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
                            socketWaitArm(k, &registrations[index.toIndex()], thread, fiber, @bitCast(@as(isize, o.socket_handle)), std.c.EVFILT.READ) catch |e|
                                break :r .{ .net_receive = .{ e, 0 } };
                            completed_inline = false;
                            break :r .{ .net_receive = .{ null, 0 } };
                        },
                        else => |e| break :drain .{ .net_receive = .{ e, 0 } },
                    };
                    data_i += message.data.len;
                    msg_i += 1;
                } else .{ .net_receive = .{ null, msg_i } };
            },
            .net_send => |*o| r: {
                const sent = netSendManyNonblocking(o.socket_handle, o.messages, o.flags);
                switch (sent) {
                    .full => break :r .{ .net_send = .{ null, o.messages.len } },
                    .partial => |n| break :r .{ .net_send = .{ null, n } },
                    .blocked => {
                        socketWaitArm(k, &registrations[index.toIndex()], thread, fiber, @bitCast(@as(isize, o.socket_handle)), std.c.EVFILT.WRITE) catch |e|
                            break :r .{ .net_send = .{ e, 0 } };
                        completed_inline = false;
                        break :r .{ .net_send = .{ null, 0 } };
                    },
                    .err => |e| break :r .{ .net_send = .{ e[0], e[1] } },
                }
            },
            .net_read => |o| r: {
                const n = netReadOnce(o.socket_handle, o.data) catch |err| switch (err) {
                    error.WouldBlock => {
                        socketWaitArm(k, &registrations[index.toIndex()], thread, fiber, @bitCast(@as(isize, o.socket_handle)), std.c.EVFILT.READ) catch |e|
                            break :r .{ .net_read = e };
                        completed_inline = false;
                        break :r .{ .net_read = 0 };
                    },
                    else => |e| break :r .{ .net_read = e },
                };
                break :r .{ .net_read = n };
            },
            .net_write => |o| r: {
                const n = netWriteOnce(o.socket_handle, o.header, o.data, o.splat) catch |err| switch (err) {
                    error.WouldBlock => {
                        socketWaitArm(k, &registrations[index.toIndex()], thread, fiber, @bitCast(@as(isize, o.socket_handle)), std.c.EVFILT.WRITE) catch |e|
                            break :r .{ .net_write = e };
                        completed_inline = false;
                        break :r .{ .net_write = 0 };
                    },
                    else => |e| break :r .{ .net_write = e },
                };
                break :r .{ .net_write = n };
            },
            else => try operate(k, submission.operation),
        };
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
}

fn batchAwaitAsync(userdata: ?*anyopaque, b: *Io.Batch) Io.Cancelable!void {
    return batchAwaitConcurrent(userdata, b, .none) catch |err| switch (err) {
        error.Timeout => unreachable,
        error.Canceled => |e| return e,
        error.ConcurrencyUnavailable => {
            // A large batch may not have memory for concurrent registrations.
            // Async is allowed to complete a single operation synchronously.
            const index = b.submitted.head;
            if (index == .none or b.completed.head != .none) return;
            const storage = &b.storage[index.toIndex()];
            const submission = storage.submission;
            const result = try operate(userdata, submission.operation);
            b.submitted.head = submission.node.next;
            if (b.submitted.head == .none) b.submitted.tail = .none;
            storage.* = .{ .completion = .{ .node = .{ .next = .none }, .result = result } };
            b.completed = .{ .head = index, .tail = index };
        },
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
    const deadline = timeout.toTimestamp(k.io());
    var registration_buffer: [changes_buffer_len]SocketWait = undefined;
    const registrations = if (b.storage.len <= registration_buffer.len)
        registration_buffer[0..b.storage.len]
    else
        k.gpa.alloc(SocketWait, b.storage.len) catch return error.ConcurrencyUnavailable;
    defer if (b.storage.len > registration_buffer.len) k.gpa.free(registrations);
    @memset(registrations, .{});
    defer batchDisarm(fiber, registrations);
    while (true) {
        // Reset before checking cancellation so a concurrent request cannot
        // be erased between the check and the actual park.
        @atomicStore(?*Fiber, &waiter.parked, null, .release);
        try k.checkCancel();
        waiter.kq_fd = Thread.current().kq_fd;
        try batchDrainSubmitted(k, b, registrations);
        if (b.submitted.head == .none or b.completed.head != .none) return;
        if (deadline) |when| {
            const remaining = when.durationFromNow(k.io()).raw.toNanoseconds();
            if (remaining <= 0) return error.Timeout;
            batchTimerChange(waiter.kq_fd, fiber, timerMilliseconds(remaining), false);
        }
        k.yield(null, .{ .register_batch_waiter = &waiter.parked });
        batchDisarm(fiber, registrations);
        // Retry syscalls before reporting expiry; a completion can have won
        // the race. The original deadline survives every stale wake.
    }
}

/// Remove only this batch's memberships, on each registration's owner.
/// Peers waiting on the same descriptor/filter keep the kernel event armed.
fn batchDisarm(fiber: *Fiber, registrations: []SocketWait) void {
    const fd = fiber.batch_waiter.kq_fd;
    if (fd < 0) return;
    _ = @atomicRmw(?*Fiber, &fiber.batch_waiter.parked, .Xchg, Fiber.finished, .acq_rel);
    batchTimerChange(fd, fiber, 0, true);
    for (registrations) |*registration| socketWaitDisarm(registration);
    fiber.batch_waiter.kq_fd = -1;
}

fn batchCancel(userdata: ?*anyopaque, b: *Io.Batch) void {
    _ = userdata;
    // Every await disarms before returning. Batch.cancel has already moved
    // remaining submitted operations to unused; no kernel requests survive.
    assert(b.pending.head == .none);
    assert(b.userdata == null);
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
