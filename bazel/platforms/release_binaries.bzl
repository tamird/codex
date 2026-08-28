"""Rules for building release binaries across supported target platforms."""

load("@rules_platform//platform_data:defs.bzl", "platform_data")

_RELEASE_PLATFORMS = {
    "linux_arm64_musl": struct(triple = "aarch64-unknown-linux-musl", v8_cpu = "arm64"),
    "linux_amd64_musl": struct(triple = "x86_64-unknown-linux-musl", v8_cpu = "x64"),
    "macos_amd64": struct(triple = "x86_64-apple-darwin", v8_cpu = "x64"),
    "macos_arm64": struct(triple = "aarch64-apple-darwin", v8_cpu = "arm64"),
    "windows_amd64": struct(triple = "x86_64-pc-windows-msvc", v8_cpu = "x64"),
    "windows_arm64": struct(triple = "aarch64-pc-windows-msvc", v8_cpu = "arm64"),
}

PLATFORMS = _RELEASE_PLATFORMS.keys()

def declare_release_platforms():
    """Declare release platforms with matching Rust and V8 target CPUs."""
    for name, platform in _RELEASE_PLATFORMS.items():
        native.platform(
            name = name,
            # platform_data leaves the legacy --cpu V8 otherwise reads unchanged.
            flags = ["--@v8//bazel/config:v8_target_cpu=" + platform.v8_cpu],
            parents = ["@rules_rs//rs/platforms:" + platform.triple],
            visibility = ["//visibility:public"],
        )

def multiplatform_binaries(name, platforms = PLATFORMS, release_binaries_name = "release_binaries"):
    """Build a binary for a subset of the declared release platforms."""
    for platform in platforms:
        if platform not in _RELEASE_PLATFORMS:
            fail("Unknown release platform '{}'; expected one of {}".format(platform, PLATFORMS))
        platform_data(
            name = name + "_" + platform,
            platform = Label("//bazel/platforms:" + platform),
            target = name,
            tags = ["manual"],
        )

    native.filegroup(
        name = release_binaries_name,
        srcs = [name + "_" + platform for platform in platforms],
        tags = ["manual"],
    )
