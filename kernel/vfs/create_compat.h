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

static inline void zerofs_vfs_assign_create(struct inode_operations *operations)
{
	/* Linux 7.3 removed exclusive. Select by type, including vendor backports,
	 * so neither ABI needs a function-pointer cast or a version-number guess.
	 */
	operations->create = _Generic(operations->create,
		int (*)(struct mnt_idmap *, struct inode *, struct dentry *, umode_t):
			zerofs_vfs_create,
		int (*)(struct mnt_idmap *, struct inode *, struct dentry *, umode_t, bool):
			zerofs_vfs_create_exclusive);
}

#endif /* ZEROFS_VFS_CREATE_COMPAT_H */
