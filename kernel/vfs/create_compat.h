#ifndef ZEROFS_VFS_CREATE_COMPAT_H
#define ZEROFS_VFS_CREATE_COMPAT_H

#include <linux/fs.h>

/* The Rust callback has one stable ABI; it never used the old exclusive flag. */
int zerofs_vfs_create(struct mnt_idmap *idmap, struct inode *parent,
		     struct dentry *dentry, umode_t mode);

static inline int zerofs_vfs_create_exclusive(struct mnt_idmap *idmap,
					    struct inode *parent,
					    struct dentry *dentry,
					    umode_t mode, bool exclusive)
{
	(void)exclusive;
	return zerofs_vfs_create(idmap, parent, dentry, mode);
}

/* Newer kernels pass a read-only idmap. The Rust create callback does not
 * access this argument; the wrapper preserves its C ABI without casting any
 * function pointer to an incompatible type.
 */
static inline int zerofs_vfs_create_const(const struct mnt_idmap *idmap,
                                         struct inode *parent,
                                         struct dentry *dentry, umode_t mode)
{
    return zerofs_vfs_create((struct mnt_idmap *)idmap, parent, dentry, mode);
}

static inline int zerofs_vfs_create_const_exclusive(const struct mnt_idmap *idmap,
                                                   struct inode *parent,
                                                   struct dentry *dentry,
                                                   umode_t mode, bool exclusive)
{
    (void)exclusive;
    return zerofs_vfs_create_const(idmap, parent, dentry, mode);
}

static inline void zerofs_vfs_assign_create(struct inode_operations *operations)
{
	/* Linux 7.3 removed exclusive. Select by type, including vendor backports,
	 * so neither ABI needs a function-pointer cast or a version-number guess.
	 */
	operations->create = _Generic(operations->create,
		int (*)(struct mnt_idmap *, struct inode *, struct dentry *, umode_t):
			zerofs_vfs_create,
		int (*)(struct mnt_idmap *, struct inode *, struct dentry *, umode_t, bool):
			zerofs_vfs_create_exclusive,
		int (*)(const struct mnt_idmap *, struct inode *, struct dentry *, umode_t):
			zerofs_vfs_create_const,
		int (*)(const struct mnt_idmap *, struct inode *, struct dentry *, umode_t, bool):
			zerofs_vfs_create_const_exclusive);
}

#endif /* ZEROFS_VFS_CREATE_COMPAT_H */
