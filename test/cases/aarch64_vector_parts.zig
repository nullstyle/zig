// A vector wider than a SIMD register must be split when copied through
// memory. Keep the check scalar so it does not depend on vector lane-load
// support, and use C main to avoid the backend's separate std.start ABI gap.
const Vector = @Vector(32, u8);
var input: Vector = .{
    0,  1,  2,  3,  4,  5,  6,  7,
    8,  9,  10, 11, 12, 13, 14, 15,
    16, 17, 18, 19, 20, 21, 22, 23,
    24, 25, 26, 27, 28, 29, 30, 31,
};

noinline fn copyVector(source: *align(1) const Vector, destination: *[32]u8) void {
    var bytes: [32]u8 = source.*;
    _ = &bytes;
    destination.* = bytes;
}

const HalfVector = @Vector(2, f16);
var half_input: HalfVector = .{ 1.5, -2.0 };

noinline fn copyHalves(source: *const HalfVector, destination: *[2]f16) void {
    var elements: [2]f16 = source.*;
    _ = &elements;
    destination.* = elements;
}

pub export fn main() c_int {
    var output: [32]u8 = undefined;
    copyVector(&input, &output);
    for (output, 0..) |byte, index| {
        if (byte != index) return 1;
    }

    // The register partition must not increase the promised memory alignment
    // or overwrite bytes adjacent to an unaligned destination.
    var unaligned: [34]u8 align(16) = @splat(0xff);
    const destination: *[32]u8 = @ptrCast(&unaligned[1]);
    copyVector(&input, destination);
    if (unaligned[0] != 0xff or unaligned[33] != 0xff) return 2;
    for (destination, 0..) |byte, index| {
        if (byte != index) return 3;
    }
    @memset(&output, 0xff);
    copyVector(@ptrCast(destination), &output);
    for (output, 0..) |byte, index| {
        if (byte != index) return 4;
    }

    // Each f16 element occupies a partial SIMD register, unlike byte parts.
    var halves: [2]f16 = undefined;
    copyHalves(&half_input, &halves);
    const half_bits: *const [2]u16 = @ptrCast(&halves);
    if (half_bits[0] != 0x3e00 or half_bits[1] != 0xc000) return 5;
    return 0;
}

// run
// backend=selfhosted,llvm
// target=aarch64-linux,aarch64-freebsd
// link_libc=true
