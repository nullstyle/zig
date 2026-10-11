const std = @import("std");
const c = @import("c");

pub fn main() !void {
    var amount: c_int = 1;
    while (amount < 8) : (amount += 1) {
        try std.testing.expectEqual(@as(c_ulonglong, 6), c.shift_unsigned(3, amount));
        try std.testing.expectEqual(@as(c_ulonglong, 3) << @intCast(amount), c.shift_signed(3, @intCast(amount)));
    }
    try std.testing.expectEqual(std.math.maxInt(c_ulonglong), c.widen_signed(-1));
    try std.testing.expectEqual(@as(c_int, -1), c.narrow_unsigned(std.math.maxInt(c_ulonglong)));
}
