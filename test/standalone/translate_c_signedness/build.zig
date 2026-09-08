const std = @import("std");

pub fn build(b: *std.Build) void {
    const test_step = b.step("test", "Run translated sign-changing casts");
    b.default_step = test_step;

    for ([_]std.builtin.Optimize{ .debug, .safe }) |optimize| {
        const translated = b.addTranslateC(.{
            .root_source_file = b.path("casts.h"),
            .target = b.graph.host,
            .optimize = optimize,
        });
        const exe = b.addExecutable(.{
            .name = "translated-casts",
            .root_module = b.createModule(.{
                .root_source_file = b.path("main.zig"),
                .target = b.graph.host,
                .optimize = optimize,
                .imports = &.{.{ .name = "c", .module = translated.createModule() }},
            }),
        });
        const run = b.addRunArtifact(exe);
        run.expectExitCode(0);
        test_step.dependOn(&run.step);
    }
}
