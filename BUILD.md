# Building for wasm32-wasip1

libsql-ffi's bundled sqlite3.c references POSIX mmap/shm symbols
(PROT_READ, MAP_SHARED, ...) that WASI's libc headers don't define even
with the `wasm32-wasi-vfs` feature selecting the WASI-safe VFS at the Rust
level -- the C compile itself still needs these shimmed via CFLAGS, or the
build fails with `use of undeclared identifier 'PROT_READ'` etc.

Required environment (same values rs-plugkit's own CI uses):

```
WASI_SDK_PATH=<path to wasi-sdk>
CC_wasm32_wasip1=$WASI_SDK_PATH/bin/clang
CFLAGS_wasm32_wasip1=--sysroot=$WASI_SDK_PATH/share/wasi-sysroot -DLONGDOUBLE_TYPE=double -DSQLITE_OMIT_LOAD_EXTENSION -DSQLITE_MAX_MMAP_SIZE=0 -DSQLITE_DEFAULT_MMAP_SIZE=0 -DSQLITE_THREADSAFE=0 -DSQLITE_TEMP_STORE=3 -DHAVE_FDATASYNC=0 -DHAVE_POSIX_FALLOCATE=0 -DHAVE_PREAD=0 -DHAVE_PWRITE=0 -DPROT_READ=1 -DPROT_WRITE=2 -DMAP_SHARED=1 -DMAP_FAILED=((void*)-1) -DMAP_PRIVATE=2 -Wno-error=implicit-function-declaration -Wno-error=int-conversion
```

Without the `-DSQLITE_MAX_MMAP_SIZE=0 -DSQLITE_DEFAULT_MMAP_SIZE=0` pair,
sqlite3.c's mmap code paths are still reachable at runtime even once they
compile (the shimmed PROT_READ/MAP_SHARED values are placeholders, not a
real mmap implementation) -- disabling mmap entirely is the actual fix,
the shim defines just get the file to compile.
