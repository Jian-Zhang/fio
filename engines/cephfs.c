#include "../fio.h"
#include <cephfs/libcephfs.h>

struct cephfs_ctx {
	struct ceph_mount_info *mount;
};

/*
 * cephfs engine uses engine_data to store last offset
 */
#define LAST_POS(f)	((f)->engine_data)

/*
 * Each thread appears to get a copy of io_ops, so .data isn't shared across
 * threads. This means that each thread gets a separate libcephfs
 * context/mount. We may want to support an option where all the threads are
 * sharing a single mount.
 */
static int fio_cephfs_setup(struct thread_data *td)
{
	struct cephfs_ctx *cephfs;
	int ret;

	if (td->io_ops->data)
		return 0;

	cephfs = malloc(sizeof(*cephfs));
	if (!cephfs) {
		log_err("malloc failed\n");
		return -ENOMEM;
	}

	ret = ceph_create(&cephfs->mount, NULL);
	if (ret) {
		log_err("ceph_create failed: ret %d\n", ret);
		goto out;
	}

	ret = ceph_conf_read_file(cephfs->mount, NULL);
	if (ret) {
		log_err("ceph_conf_read_file failed: ret %d\n", ret);
		goto out_release;
	}

	ret = ceph_mount(cephfs->mount, NULL);
	if (ret) {
		log_err("ceph_mount failed: ret %d\n", ret);
		goto out_release;
	}

	dprint(FD_FILE, "fio cephfs setup %p\n", cephfs);
	td->io_ops->data = cephfs;

	return 0;

out_release:
	ceph_release(cephfs->mount);
out:
	free(cephfs);
	return ret;
}

static int fio_cephfs_open_file(struct thread_data *td, struct fio_file *f)
{
	struct cephfs_ctx *cephfs = td->io_ops->data;
	int ret;
	int flags = 0;

	dprint(FD_FILE, "fio cephfs open file %s\n", f->file_name);

	if (td_write(td)) {
		if (!read_only)
			flags = O_RDWR;
	} else if (td_read(td)) {
		if (!read_only)
			flags = O_RDWR;
		else
			flags = O_RDONLY;
	}

	if (td->o.sync_io)
		flags |= O_SYNC;

	flags |= O_CREAT;

	ret = ceph_open(cephfs->mount, f->file_name, flags, 0600);
	if (ret < 0) {
		log_err("ceph_open failed: ret %d\n", ret);
		return ret;
	}

	f->fd = ret;

	return 0;
}

static int fio_cephfs_close_file(struct thread_data fio_unused *td, struct fio_file *f)
{
	struct cephfs_ctx *cephfs = td->io_ops->data;
	int ret;

	dprint(FD_FILE, "fd close %s\n", f->file_name);

	assert(cephfs);
	assert(f->fd > 0);

	ret = ceph_close(cephfs->mount, f->fd);

	f->fd = -1;
	f->engine_data = 0;

	return ret;
}

/*
 * Adapated from sync engine.
 */
static int fio_cephfs_prep(struct thread_data *td, struct io_u *io_u)
{
	struct cephfs_ctx *cephfs = td->io_ops->data;
	struct fio_file *f = io_u->file;
	int64_t ret;

	// DDIR_TRIM ?
	if (!ddir_rw(io_u->ddir))
		return 0;

	if (LAST_POS(f) != -1ULL && LAST_POS(f) == io_u->offset)
		return 0;

	ret = ceph_lseek(cephfs->mount, f->fd, io_u->offset, SEEK_SET);
	if (ret < 0) {
		td_verror(td, (int)ret, "ceph_lseek");
		return 1;
	}

	return 0;
}

/*
 * Adapated from sync engine.
 */
static int fio_io_end(struct thread_data *td, struct io_u *io_u, int ret)
{
	if (io_u->file && ret >= 0 && ddir_rw(io_u->ddir))
		LAST_POS(io_u->file) = io_u->offset + ret;

	if (ret != (int) io_u->xfer_buflen) {
		if (ret >= 0) {
			io_u->resid = io_u->xfer_buflen - ret;
			io_u->error = 0;
			return FIO_Q_COMPLETED;
		} else
			io_u->error = ret; // invert sign?
	}

	if (io_u->error)
		td_verror(td, io_u->error, "xfer");

	return FIO_Q_COMPLETED;
}

/*
 * Adapated from sync engine.
 */
static int fio_cephfs_queue(struct thread_data *td, struct io_u *io_u)
{
	struct cephfs_ctx *cephfs = td->io_ops->data;
	struct fio_file *f = io_u->file;
	int ret;

	fio_ro_check(td, io_u);

	if (io_u->ddir == DDIR_READ)
		ret = ceph_read(cephfs->mount, f->fd, io_u->xfer_buf,
				io_u->xfer_buflen, -1);

	else if (io_u->ddir == DDIR_WRITE)
		ret = ceph_write(cephfs->mount, f->fd, io_u->xfer_buf,
				io_u->xfer_buflen, -1);

	else if (io_u->ddir == DDIR_SYNC)
		ret = ceph_fsync(cephfs->mount, f->fd, 0);

	else if (io_u->ddir == DDIR_DATASYNC)
		ret = ceph_fsync(cephfs->mount, f->fd, 1);

	else {
		log_err("unsupported operation\n");
		return -EINVAL;
	}

	return fio_io_end(td, io_u, ret);
}

static int fio_cephfs_get_file_size(struct thread_data *td, struct fio_file *f)
{
	struct cephfs_ctx *cephfs = td->io_ops->data;
	struct stat st;
	int ret;

	assert(cephfs);

	if (fio_file_size_known(f))
		return 0;

	ret = ceph_lstat(cephfs->mount, f->file_name, &st);
	if (ret < 0) {
		log_err("ceph_lstat failed %d\n", ret);
		return ret;
	}

	f->real_file_size = st.st_size;
	fio_file_set_size_known(f);

	return 0;
}

static void fio_cephfs_cleanup(struct thread_data *td)
{
	struct cephfs_ctx *cephfs = td->io_ops->data;
	int ret;

	assert(cephfs);
	ret = ceph_unmount(cephfs->mount);
	if (ret)
		log_err("ceph_unmount %d\n", ret);
	free(cephfs);
	td->io_ops->data = NULL;
}

static int fio_cephfs_unlink_file(struct thread_data *td, struct fio_file *f)
{
	struct cephfs_ctx *cephfs = td->io_ops->data;
	int ret;

	ret = ceph_unlink(cephfs->mount, f->file_name);
	if (ret < 0) {
		log_err("ceph_unlink %d\n", ret);
		return ret;
	}

	return 0;
}

static struct ioengine_ops ioengine = {
	.name			= "cephfs",
	.version		= FIO_IOOPS_VERSION,
	.setup			= fio_cephfs_setup,
	.cleanup		= fio_cephfs_cleanup,
	.prep			= fio_cephfs_prep,
	.queue			= fio_cephfs_queue,
	.open_file		= fio_cephfs_open_file,
	.close_file		= fio_cephfs_close_file,
	.unlink_file		= fio_cephfs_unlink_file,
	.get_file_size 		= fio_cephfs_get_file_size,
	.flags 			= FIO_SYNCIO | FIO_DISKLESSIO,
};

static void fio_init fio_cephfs_register(void)
{
	register_ioengine(&ioengine);
}

static void fio_exit fio_cephfs_unregister(void)
{
	unregister_ioengine(&ioengine);
}
