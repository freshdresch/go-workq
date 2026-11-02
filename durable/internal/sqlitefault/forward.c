//go:build durable_faulttest && linux && amd64 && cgo

#include "abi.h"
#include <errno.h>
#include <stddef.h>
#include <stdlib.h>
#include <string.h>
#include <sys/stat.h>

extern int goFaultPath(uintptr_t, char *);
extern int goFaultOpenBoundary(uintptr_t, char *, int, int);
extern int goFaultEvent(uintptr_t, char *, int, int, int, int, int, long long, long long);

extern int goFaultDelete(uintptr_t, char *, unsigned long long, unsigned long long, int, int, int);

typedef struct {
    sqlite3_vfs vfs;
    sqlite3_vfs *native;
    uintptr_t handle;
    int opened;
} FaultVFS;

typedef struct {
    sqlite3_file file;
    sqlite3_file *native;
    FaultVFS *owner;
    char *path;
    int descriptor;
} FaultFile;

// Pinned unixFile prefix in sqlite3-binding.c. Never used in normal builds.
typedef struct {
    const struct sqlite3_io_methods *methods;
    sqlite3_vfs *vfs;
    void *inode;
    int descriptor;
} UnixPrefix;

static int faultClose(sqlite3_file *file) {
    FaultFile *wrapped = (FaultFile *)file;
    int result = wrapped->native->pMethods->xClose(wrapped->native);
    wrapped->owner->opened--;
    free(wrapped->path);
    file->pMethods = NULL;
    return result;
}

static int faultSync(sqlite3_file *file, int flags) {
    FaultFile *wrapped = (FaultFile *)file;
    int result = goFaultEvent(wrapped->owner->handle, wrapped->path,
        wrapped->descriptor, 2, 1, 0, 0, 0, 0);
    int saved_errno = 0;
    if (result == 0) {
        result = wrapped->native->pMethods->xSync(wrapped->native, flags);
        saved_errno = result != 0 ? errno : 0;
    }
    int observed = goFaultEvent(wrapped->owner->handle, wrapped->path,
        wrapped->descriptor, 2, 2, result, saved_errno, 0, 0);
    return result != 0 ? result : observed;
}

static int faultRead(sqlite3_file *file, void *buffer, int amount, sqlite3_int64 offset) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xRead(native, buffer, amount, offset);
}

static int faultWrite(sqlite3_file *file, const void *buffer, int amount, sqlite3_int64 offset) {
    FaultFile *wrapped = (FaultFile *)file;
    sqlite3_file *native = wrapped->native;
    int result = goFaultEvent(wrapped->owner->handle, wrapped->path,
        wrapped->descriptor, 3, 1, 0, 0, offset, amount);
    if (result == 0) {
        result = native->pMethods->xWrite(native, buffer, amount, offset);
    }
    int observed = goFaultEvent(wrapped->owner->handle, wrapped->path,
        wrapped->descriptor, 3, 2, result, result != 0 ? errno : 0, offset, amount);
    return result != 0 ? result : observed;
}

static int faultTruncate(sqlite3_file *file, sqlite3_int64 size) {
    FaultFile *wrapped = (FaultFile *)file;
    sqlite3_file *native = wrapped->native;
    int result = goFaultEvent(wrapped->owner->handle, wrapped->path,
        wrapped->descriptor, 4, 1, 0, 0, 0, size);
    if (result == 0) {
        result = native->pMethods->xTruncate(native, size);
    }
    int observed = goFaultEvent(wrapped->owner->handle, wrapped->path,
        wrapped->descriptor, 4, 2, result, result != 0 ? errno : 0, 0, size);
    return result != 0 ? result : observed;
}

static int faultFileSize(sqlite3_file *file, sqlite3_int64 *size) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xFileSize(native, size);
}

static int faultLock(sqlite3_file *file, int lock) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xLock(native, lock);
}

static int faultUnlock(sqlite3_file *file, int lock) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xUnlock(native, lock);
}

static int faultCheckReservedLock(sqlite3_file *file, int *result) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xCheckReservedLock(native, result);
}

static int faultFileControl(sqlite3_file *file, int operation, void *argument) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xFileControl(native, operation, argument);
}

static int faultSectorSize(sqlite3_file *file) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xSectorSize(native);
}

static int faultDeviceCharacteristics(sqlite3_file *file) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xDeviceCharacteristics(native);
}

static int faultShmMap(sqlite3_file *file, int page, int size, int extend, void volatile **result) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xShmMap(native, page, size, extend, result);
}

static int faultShmLock(sqlite3_file *file, int offset, int count, int flags) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xShmLock(native, offset, count, flags);
}

static int faultShmUnmap(sqlite3_file *file, int remove) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xShmUnmap(native, remove);
}

static int faultFetch(sqlite3_file *file, sqlite3_int64 offset, int amount, void **result) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xFetch(native, offset, amount, result);
}

static int faultUnfetch(sqlite3_file *file, sqlite3_int64 offset, void *buffer) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    return native->pMethods->xUnfetch(native, offset, buffer);
}

static void faultShmBarrier(sqlite3_file *file) {
    sqlite3_file *native = ((FaultFile *)file)->native;
    native->pMethods->xShmBarrier(native);
}

static const struct sqlite3_io_methods methods = {
    3, faultClose, faultRead, faultWrite, faultTruncate, faultSync,
    faultFileSize, faultLock, faultUnlock, faultCheckReservedLock,
    faultFileControl, faultSectorSize, faultDeviceCharacteristics,
    faultShmMap, faultShmLock, faultShmBarrier, faultShmUnmap, faultFetch, faultUnfetch
};

static int faultOpen(sqlite3_vfs *vfs, const char *path, sqlite3_file *file,
                     int flags, int *out_flags) {
    FaultVFS *owner = (FaultVFS *)vfs;
    FaultFile *wrapped = (FaultFile *)file;
    memset(wrapped, 0, sizeof(*wrapped));
    if (path == NULL || goFaultPath(owner->handle, (char *)path) != 0) {
        return 14;
    }
    wrapped->native = (sqlite3_file *)(wrapped + 1);
    wrapped->owner = owner;
    wrapped->path = strdup(path);
    if (wrapped->path == NULL) {
        return 7;
    }
    int result = goFaultOpenBoundary(owner->handle, wrapped->path, 1, 0);
    if (result != 0) {
        free(wrapped->path);
        return result;
    }
    result = owner->native->xOpen(owner->native, path, wrapped->native, flags, out_flags);
    if (result != 0) {
        goFaultOpenBoundary(owner->handle, wrapped->path, 2, result);
    }
    if (result == 0) {
        UnixPrefix prefix;
        memcpy(&prefix, wrapped->native, sizeof(prefix));
        wrapped->descriptor = prefix.descriptor;
        if (prefix.vfs != owner->native || wrapped->native->pMethods->iVersion != 3) {
            result = 10;
        } else {
            result = goFaultEvent(owner->handle, wrapped->path,
                wrapped->descriptor, 1, 2, 0, 0, 0, 0);
        }
    }
    if (result != 0) {
        if (wrapped->native->pMethods != NULL) {
            wrapped->native->pMethods->xClose(wrapped->native);
        }
        free(wrapped->path);
        return result;
    }
    owner->opened++;
    file->pMethods = &methods;
    return 0;
}

static int faultDelete(sqlite3_vfs *vfs, const char *path, int sync) {
    if (goFaultPath(((FaultVFS *)vfs)->handle, (char *)path) != 0) {
        return 10;
    }
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    struct stat status;
    memset(&status, 0, sizeof(status));
    if (lstat(path, &status) != 0 && errno != ENOENT) {
        return 10;
    }
    int result = goFaultDelete(((FaultVFS *)vfs)->handle, (char *)path,
        status.st_dev, status.st_ino, 1, 0, 0);
    if (result == 0) {
        result = native->xDelete(native, path, sync);
    }
    int observed = goFaultDelete(((FaultVFS *)vfs)->handle, (char *)path,
        status.st_dev, status.st_ino, 2, result, result != 0 ? errno : 0);
    return result != 0 ? result : observed;
}

static int faultAccess(sqlite3_vfs *vfs, const char *path, int flags, int *result) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    return native->xAccess(native, path, flags, result);
}

static int faultFullPathname(sqlite3_vfs *vfs, const char *path, int size, char *result) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    return native->xFullPathname(native, path, size, result);
}

static void * faultDlOpen(sqlite3_vfs *vfs, const char *path) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    return native->xDlOpen(native, path);
}

static void faultDlError(sqlite3_vfs *vfs, int size, char *result) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    native->xDlError(native, size, result);
}

static void faultDlClose(sqlite3_vfs *vfs, void *handle) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    native->xDlClose(native, handle);
}

static int faultRandomness(sqlite3_vfs *vfs, int size, char *result) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    return native->xRandomness(native, size, result);
}

static int faultSleep(sqlite3_vfs *vfs, int duration) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    return native->xSleep(native, duration);
}

static int faultCurrentTime(sqlite3_vfs *vfs, double *result) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    return native->xCurrentTime(native, result);
}

static int faultGetLastError(sqlite3_vfs *vfs, int size, char *result) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    return native->xGetLastError(native, size, result);
}

static int faultCurrentTimeInt64(sqlite3_vfs *vfs, sqlite3_int64 *result) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    return native->xCurrentTimeInt64(native, result);
}

static int faultSetSystemCall(sqlite3_vfs *vfs, const char *name, sqlite3_syscall_ptr call) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    return native->xSetSystemCall(native, name, call);
}

static sqlite3_syscall_ptr faultGetSystemCall(sqlite3_vfs *vfs, const char *name) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    return native->xGetSystemCall(native, name);
}

static const char * faultNextSystemCall(sqlite3_vfs *vfs, const char *name) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    return native->xNextSystemCall(native, name);
}

static void (*faultDlSym(sqlite3_vfs *vfs, void *handle, const char *name))(void) {
    sqlite3_vfs *native = ((FaultVFS *)vfs)->native;
    return native->xDlSym(native, handle, name);
}

void *fault_register(const char *name, uintptr_t handle) {
    const char *source = "2026-07-24 19:02:57 "
        "bf7c7f30031888f4e796e429ab3978879485813aaca6f641c7b33e4e09459bcc";
    sqlite3_vfs *native = sqlite3_vfs_find("unix");
    if (sqlite3_libversion_number() != 3053004 || strcmp(sqlite3_sourceid(), source) != 0) {
        return NULL;
    }
    if (native == NULL || native->iVersion != 3 || strcmp(native->zName, "unix") != 0
        || native->szOsFile < (int)sizeof(UnixPrefix)) {
        return NULL;
    }
    FaultVFS *owner = calloc(1, sizeof(*owner));
    if (owner == NULL) {
        return NULL;
    }
    owner->native = native;
    owner->handle = handle;
    owner->vfs = *native;
    owner->vfs.zName = strdup(name);
    owner->vfs.pNext = NULL;
    owner->vfs.pAppData = NULL;
    owner->vfs.szOsFile = sizeof(FaultFile) + native->szOsFile;
    owner->vfs.xOpen = faultOpen;
    owner->vfs.xDelete = faultDelete;
    owner->vfs.xAccess = faultAccess;
    owner->vfs.xFullPathname = faultFullPathname;
    owner->vfs.xDlOpen = faultDlOpen;
    owner->vfs.xDlError = faultDlError;
    owner->vfs.xDlClose = faultDlClose;
    owner->vfs.xRandomness = faultRandomness;
    owner->vfs.xSleep = faultSleep;
    owner->vfs.xCurrentTime = faultCurrentTime;
    owner->vfs.xGetLastError = faultGetLastError;
    owner->vfs.xCurrentTimeInt64 = faultCurrentTimeInt64;
    owner->vfs.xSetSystemCall = faultSetSystemCall;
    owner->vfs.xGetSystemCall = faultGetSystemCall;
    owner->vfs.xNextSystemCall = faultNextSystemCall;
    owner->vfs.xDlSym = faultDlSym;
    if (owner->vfs.zName == NULL || sqlite3_vfs_register(&owner->vfs, 0) != 0) {
        free((void *)owner->vfs.zName);
        free(owner);
        return NULL;
    }
    return owner;
}

int fault_unregister(void *registration) {
    FaultVFS *owner = registration;
    if (owner->opened != 0) {
        return 5;
    }
    int result = sqlite3_vfs_unregister(&owner->vfs);
    if (result != 0) {
        return result;
    }
    free((void *)owner->vfs.zName);
    free(owner);
    return 0;
}
