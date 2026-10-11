const std = @import("std");
const builtin = @import("builtin");
const Dwarf = std.debug.Dwarf;
const SelfUnwinder = Dwarf.SelfUnwinder;
const RegisterRule = Dwarf.Unwind.VirtualMachine.RegisterRule;

fn unwind(cfa: usize, rule: ?RegisterRule, expected: usize) !void {
    const ip_reg = Dwarf.ipRegNum(builtin.cpu.arch).?;
    const sp_reg = Dwarf.spRegNum(builtin.cpu.arch);
    var context = std.mem.zeroes(std.debug.cpu_context.Native);
    (try SelfUnwinder.regNative(&context, ip_reg)).* = 0x1234;
    (try SelfUnwinder.regNative(&context, sp_reg)).* = cfa;
    var unwinder: SelfUnwinder = .init(&context);
    defer unwinder.deinit();

    // Cached rules let us exercise the actual memory-reading unwind paths
    // without relying on a particular compiler-generated stack frame.
    var cie: Dwarf.Unwind.CommonInformationEntry = undefined;
    cie.format = .@"32";
    cie.return_address_register = @intCast(ip_reg);
    cie.is_signal_frame = false;
    var entry: SelfUnwinder.CacheEntry = .{
        .pc = unwinder.pc,
        .cie = &cie,
        .cfa_rule = .{ .reg_off = .{ .register = @intCast(sp_reg), .offset = 0 } },
        .num_rules = if (rule == null) 0 else 1,
        .rules_regs = undefined,
        .rules = undefined,
    };
    if (rule) |r| {
        entry.rules_regs[0] = ip_reg;
        entry.rules[0] = r;
    }
    try std.testing.expectEqual(expected, try unwinder.next(std.debug.getDebugInfoAllocator(), &entry));
    try std.testing.expectEqual(expected, unwinder.cpu_state.getPc());
}

pub fn main() !void {
    for ([_]usize{ 0, 1, std.heap.page_size_min - 1 }) |address| {
        try unwind(address, null, 0);
        try unwind(std.heap.page_size_min, .{
            .offset = @as(i64, @intCast(address)) - std.heap.page_size_min,
        }, 0);
    }
    // DW_OP_lit1 describes an address to dereference for .expression.
    try unwind(std.heap.page_size_min, .{ .expression = &.{std.dwarf.OP.lit1} }, 0);

    // Small immediate values are legitimate; only memory addresses get the
    // null-page guard. A normal stack memory read must still recover its RA.
    try unwind(std.heap.page_size_min, .{ .val_offset = 1 - @as(i64, std.heap.page_size_min) }, 1);
    try unwind(std.heap.page_size_min, .{ .val_expression = &.{std.dwarf.OP.lit1} }, 1);
    var return_address: usize = 0x4321;
    try unwind(@intFromPtr(&return_address), .{ .offset = 0 }, return_address);
}

// run
// backend=llvm
// target=aarch64-macos,x86_64-macos,aarch64-linux,x86_64-linux,aarch64-freebsd,x86_64-freebsd
// link_libc=true
