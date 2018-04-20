/*
 * file:        homework.c
 * description: skeleton file for CS 5600/7600 file system
 *
 * CS 5600, Computer Systems, Northeastern CCIS
 * Peter Desnoyers, November 2016
 */

#define FUSE_USE_VERSION 27

#include <stdlib.h>
#include <stddef.h>
#include <unistd.h>
#include <fuse.h>
#include <fcntl.h>
#include <string.h>
#include <stdio.h>
#include <errno.h>

#include "fsx600.h"
#include "blkdev.h"

extern int homework_part; /* set by '-part n' command-line option */

/* 
 * disk access - the global variable 'disk' points to a blkdev
 * structure which has been initialized to access the image file.
 *
 * NOTE - blkdev access is in terms of 1024-byte blocks
 */
extern struct blkdev *disk;

/* by defining bitmaps as 'fd_set' pointers, you can use existing
 * macros to handle them. 
 *   FD_ISSET(##, inode_map);
 *   FD_CLR(##, block_map);
 *   FD_SET(##, block_map);
 */
fd_set *inode_map; /* = malloc(sb.inode_map_size * FS_BLOCK_SIZE); */
fd_set *block_map;

fd_set *inode_map;
int inode_map_base;

struct fs_inode *inodes;
int n_inodes;
int inode_base;

fd_set *block_map;
int block_map_base;

int n_blocks;
int root_inode;

// define constants
int num_entry = FS_BLOCK_SIZE / sizeof(struct fs_dirent);
int num_entry_in_blk = BLOCK_SIZE / sizeof(uint32_t);
int direct_sz = N_DIRECT * BLOCK_SIZE;
int indirect_level1_sz = BLOCK_SIZE / sizeof(uint32_t) * BLOCK_SIZE;
int indirect_level2_sz = BLOCK_SIZE / sizeof(uint32_t) * BLOCK_SIZE / sizeof(uint32_t) * BLOCK_SIZE;
/* init - this is called once by the FUSE framework at startup. Ignore
 * the 'conn' argument.
 * recommended actions:
 *   - read superblock
 *   - allocate memory, read bitmaps and inodes
 */
void *fs_init(struct fuse_conn_info *conn)
{
    struct fs_super sb;
    if (disk->ops->read(disk, 0, 1, &sb) < 0)
        exit(1);

    /* The inode map and block map are written directly to the disk after the superblock */

    inode_map_base = 1;
    inode_map = malloc(sb.inode_map_sz * FS_BLOCK_SIZE);
    if (disk->ops->read(disk, inode_map_base, sb.inode_map_sz, inode_map) < 0)
        exit(1);

    block_map_base = inode_map_base + sb.inode_map_sz;
    block_map = malloc(sb.block_map_sz * FS_BLOCK_SIZE);
    if (disk->ops->read(disk, block_map_base, sb.block_map_sz, block_map) < 0)
        exit(1);

    /* The inode data is written to the next set of blocks */

    inode_base = block_map_base + sb.block_map_sz;
    n_inodes = sb.inode_region_sz * INODES_PER_BLK;
    inodes = malloc(sb.inode_region_sz * FS_BLOCK_SIZE);
    if (disk->ops->read(disk, inode_base, sb.inode_region_sz, inodes) < 0)
        exit(1);

    /* your code here */

    n_blocks = sb.num_blocks;
    root_inode = sb.root_inode;
    return NULL;
}

// lookup the path
static int lookup(const char *path)
{
    if (strcmp(path, "/") == 0)
        return root_inode;
    // copy the path
    char _path[strlen(path) + 1];
    strcpy(_path, path);

    // preivous inode
    int inode_index = root_inode;
    struct fs_inode *cur_inode = NULL;
    struct fs_dirent *cur_dir = NULL;
    struct fs_dirent entry[num_entry];

    // traverse
    char *token = strtok(_path, "/");
    while (token != NULL && strlen(token) > 0)
    {
        if (cur_dir != NULL)
        {
            if (!cur_dir->valid)
            {
                return -EINVAL;
            }
            if (!cur_dir->isDir)
            {
                return -ENOTDIR;
            }
        }
        cur_inode = &inodes[inode_index];

        // reset entry
        bzero(entry, num_entry * sizeof(struct fs_dirent));
        if (disk->ops->read(disk, cur_inode->direct[0], 1, entry) < 0)
        {
            exit(1);
        }

        // look up for the dir name
        int i;
        int found = 0;
        for (i = 0; i < num_entry; i++)
        {
            if (entry[i].valid && strcmp(entry[i].name, token) == 0)
            {
                inode_index = entry[i].inode;
                cur_dir = &entry[i];
                found = 1;
                break;
            }
        }

        // if not found
        if (found == 0)
        {
            return -ENOENT;
        }

        // next inode
        token = strtok(NULL, "/");
    }
    return inode_index;
}


static int setStat(struct fs_inode *inode, struct stat *sb)
{
    if (inode == NULL || sb == NULL)
    {
        return -1;
    }
    //clear bitset
    memset(sb, 0, sizeof(*sb));

    sb->st_uid = inode->uid;
    sb->st_gid = inode->gid;
    sb->st_mode = inode->mode;
    sb->st_atime = inode->mtime;
    sb->st_ctime = inode->ctime;
    sb->st_mtime = inode->mtime;
    sb->st_size = inode->size;
    sb->st_nlink = 1;
    sb->st_blocks = (inode->size + FS_BLOCK_SIZE - 1) / FS_BLOCK_SIZE;

    return SUCCESS;
}

/* Note on path translation errors:
 * In addition to the method-specific errors listed below, almost
 * every method can return one of the following errors if it fails to
 * locate a file or directory corresponding to a specified path.
 *
 * ENOENT - a component of the path is not present.
 * ENOTDIR - an intermediate component of the path (e.g. 'b' in
 *           /a/b/c) is not a directory
 */

/* note on splitting the 'path' variable:
 * the value passed in by the FUSE framework is declared as 'const',
 * which means you can't modify it. The standard mechanisms for
 * splitting strings in C (strtok, strsep) modify the string in place,
 * so you have to copy the string and then free the copy when you're
 * done. One way of doing this:
 *
 *    char *_path = strdup(path);
 *    int inum = translate(_path);
 *    free(_path);
 */

/* getattr - get file or directory attributes. For a description of
 *  the fields in 'struct stat', see 'man lstat'.
 *
 * Note - fields not provided in fsx600 are:
 *    st_nlink - always set to 1
 *    st_atime, st_ctime - set to same value as st_mtime
 *
 * errors - path translation, ENOENT
 */
static int fs_getattr(const char *path, struct stat *sb)
{
    fs_init(NULL);
    int inode_index = lookup(path);
    // directory not exists
    if (inode_index < 0)
    {
        return inode_index;
    }
    int val = setStat(&inodes[inode_index], sb);
    return val;
}

/* readdir - get directory contents.
 *
 * for each entry in the directory, invoke the 'filler' function,
 * which is passed as a function pointer, as follows:
 *     filler(buf, <name>, <statbuf>, 0)
 * where <statbuf> is a struct stat, just like in getattr.
 *
 * Errors - path resolution, ENOTDIR, ENOENT
 */
static int fs_readdir(const char *path, void *ptr, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    char _path[strlen(path) + 1];
    strcpy(_path, path);
    int inode_index = lookup(_path);
    // check whether path exists
    if (inode_index < 0)
    {
        return inode_index;
    }
    struct fs_inode *inode = &inodes[inode_index];
    // check whether path is directory
    if (!S_ISDIR(inode->mode))
        return -ENOTDIR;

    struct fs_dirent entry[num_entry];
    struct stat sb;
    if (disk->ops->read(disk, inode->direct[0], 1, entry) < 0)
    {
        exit(1);
    }
    int i;
    for (i = 0; i < num_entry; i++)
    {
        if (entry[i].valid)
        {
            setStat(&inodes[entry[i].inode], &sb);
            filler(ptr, entry[i].name, &sb, 0);
        }
    }
    return SUCCESS;
}

/* see description of Part 2. In particular, you can save information 
 * in fi->fh. If you allocate memory, free it in fs_releasedir.
 */
static int is_dir(const char *path)
{
    char _path[strlen(path) + 1];
    strcpy(_path, path);
    int inode_index = lookup(_path);
    // check whether path exists
    if (inode_index < 0)
    {
        return -ENOENT;
    }
    // check whether path is directory
    if (!S_ISDIR(inodes[inode_index].mode))
    {
        return -ENOTDIR;
    }
    return SUCCESS;
}

static int is_file(const char *path)
{
    char _path[strlen(path) + 1];
    strcpy(_path, path);

    int inode_index = lookup(_path);
    if (inode_index < 0)
    {
        return -ENOENT;
    }
    if (S_ISDIR(inodes[inode_index].mode))
    {
        return -EISDIR;
    }
    return SUCCESS;
}

static int fs_opendir(const char *path, struct fuse_file_info *fi)
{
    int val;
    if ((val = is_dir(path)) == SUCCESS)
        fi->fh = lookup(path);

    return val;
}

static int fs_releasedir(const char *path, struct fuse_file_info *fi)
{
    int val;
    if ((val = is_dir(path)) == SUCCESS)
        fi->fh = -1;

    return val;
}

static int search_available_inode()
{
    int i;
    for (i = 2; i < n_inodes; i++)
    {
        if (!FD_ISSET(i, inode_map))
        {
            return i;
        }
    }
    return -ENOSPC;
}

static int search_available_blk()
{
    int i;
    for (i = 0; i < n_blocks; i++)
    {
        if (!FD_ISSET(i, block_map))
        {
            // resset block
            char clear_buffer[BLOCK_SIZE];
            bzero(clear_buffer, BLOCK_SIZE);
            if (disk->ops->write(disk, i, 1, clear_buffer) < 0)
            {
                exit(1);
            }
            return i;
        }
    }
    return -ENOSPC;
}

static void set_map()
{
    if (disk->ops->write(disk, inode_map_base, block_map_base - inode_map_base, inode_map) < 0)
    {
        exit(1);
    }
    if (disk->ops->write(disk, block_map_base, inode_base - block_map_base, block_map) < 0)
    {
        exit(1);
    }
}

static void set_inode(int inode_index)
{
    int offset = inode_base + inode_index / INODES_PER_BLK;
    int index = inode_index - inode_index % INODES_PER_BLK;

    if (disk->ops->write(disk, offset, 1, &inodes[index]) < 0)
    {
        exit(1);
    }
}

/* mknod - create a new file with permissions (mode & 01777)
 *
 * Errors - path resolution, EEXIST
 *          in particular, for mknod("/a/b/c") to succeed,
 *          "/a/b" must exist, and "/a/b/c" must not.
 *
 * If a file or directory of this name already exists, return -EEXIST.
 * If this would result in >32 entries in a directory, return -ENOSPC
 * if !S_ISREG(mode) return -EINVAL [i.e. 'mode' specifies a device special
 * file or other non-file object]
 */
static int fs_mknod(const char *path, mode_t mode, dev_t dev)
{
    // make sure mode is regular file
    if (!S_ISREG(mode))
    {
        return -EINVAL;
    }
    if (strcmp(path, "/") == 0)
    {
        return -EINVAL;
    }

    char previous[strlen(path) + 1];
    strcpy(previous, path);
    previous[strrchr(previous, '/') - previous] = '\0';
    if (strlen(previous) == 0)
    {
        strcpy(previous, "/");
    }

    // check whether previous dir inode exists
    int dir_inode_index = lookup(previous);
    if (dir_inode_index < 0)
    {
        return dir_inode_index;
    }

    // check whether the file exists
    int inode_index = lookup(path);
    if (inode_index >= 0)
    {
        return -EEXIST;
    }

    // find previous dir inode
    struct fs_inode *dir_inode = &inodes[dir_inode_index];
    if (!S_ISDIR(dir_inode->mode))
    {
        return -ENOTDIR;
    }
    struct fs_dirent entry[num_entry];
    if (disk->ops->read(disk, dir_inode->direct[0], 1, entry) < 0)
    {
        exit(1);
    }

    // search available entry
    int available_entry;
    for (available_entry = 0; available_entry < num_entry; available_entry++)
    {
        if (!entry[available_entry].valid)
        {
            break;
        }
    }
    if (available_entry == num_entry)
    {
        return -ENOSPC;
    }

    // create file, copy file name
    char *file_name = strrchr(path, '/') + 1;
    if (strlen(file_name) >= 28)
        return -EINVAL;
    strcpy(entry[available_entry].name, file_name);

    // search available inode
    int available_inode = search_available_inode();
    if (available_inode < 0)
    {
        return -ENOSPC;
    }
    entry[available_entry].inode = available_inode;
    entry[available_entry].isDir = 0;
    entry[available_entry].valid = 1;
    FD_SET(available_inode, inode_map);

    time_t ctime = time(NULL);
    inodes[available_inode].uid = getuid();
    inodes[available_inode].gid = getgid();
    inodes[available_inode].mode = mode;
    inodes[available_inode].ctime = ctime;
    inodes[available_inode].mtime = ctime;
    inodes[available_inode].size = 0;

    // write disk
    set_inode(available_inode);
    set_map();
    if (disk->ops->write(disk, dir_inode->direct[0], 1, entry) < 0)
        exit(1);

    return SUCCESS;
}

/* mkdir - create a directory with the given mode.
 * Errors - path resolution, EEXIST
 * Conditions for EEXIST are the same as for create. 
 * If this would result in >32 entries in a directory, return -ENOSPC
 *
 * Note that you may want to combine the logic of fs_mknod and
 * fs_mkdir. 
 */
static int fs_mkdir(const char *path, mode_t mode)
{
    // make sure mode is regular file
    if (!S_ISREG(mode))
    {
        return -EINVAL;
    }
    if (strcmp(path, "/") == 0)
    {
        return -EINVAL;
    }

    char _path[strlen(path) + 1];
    strcpy(_path, path);
    // get previous dir
    char previous[strlen(path) + 1];
    strcpy(previous, path);
    previous[strrchr(previous, '/') - previous] = '\0';
    if (strlen(previous) == 0)
    {
        strcpy(previous, "/");
    }
    // check whether previous dir inode exists
    int dir_inode_index = lookup(previous);
    if (dir_inode_index < 0)
    {
        return dir_inode_index;
    }

    // check whether the file exists
    int inode_index = lookup(_path);
    if (inode_index >= 0)
    {
        return -EEXIST;
    }

    // find previous dir inode
    struct fs_inode *dir_inode = &inodes[dir_inode_index];
    if (!S_ISDIR(dir_inode->mode))
    {
        return -ENOTDIR;
    }

    struct fs_dirent entry[num_entry];
    if (disk->ops->read(disk, dir_inode->direct[0], 1, entry) < 0)
    {
        exit(1);
    }

    // search available entry
    int available_entry;
    for (available_entry = 0; available_entry < num_entry; available_entry++)
    {
        if (!entry[available_entry].valid)
        {
            break;
        }
    }
    if (available_entry == num_entry)
    {
        return -ENOSPC;
    }

    // create file, copy file name
    char *file_name = strrchr(path, '/') + 1;
    if (strlen(file_name) >= 28)
        return -EINVAL;
    strcpy(entry[available_entry].name, file_name);

    // search available inode
    int available_inode = search_available_inode();
    if (available_inode < 0)
    {
        return -ENOSPC;
    }
    entry[available_entry].inode = available_inode;
    entry[available_entry].isDir = 1;
    entry[available_entry].valid = 1;
    FD_SET(available_inode, inode_map);

    time_t ctime = time(NULL);
    inodes[available_inode].uid = getuid();
    inodes[available_inode].gid = getgid();
    inodes[available_inode].mode = mode;
    inodes[available_inode].ctime = ctime;
    inodes[available_inode].mtime = ctime;
    inodes[available_inode].size = 0;

    // allocate block
    int available_blk = search_available_blk();
    if (available_blk < 0)
    {
        return -ENOSPC;
    }
    FD_SET(available_blk, block_map);
    inodes[available_inode].direct[0] = available_blk;

    // write disk
    set_inode(available_inode);
    set_map();
    if (disk->ops->write(disk, dir_inode->direct[0], 1, entry) < 0)
        exit(1);

    return SUCCESS;
}

/* truncate - truncate file to exactly 'len' bytes
 * Errors - path resolution, ENOENT, EISDIR, EINVAL
 *    return EINVAL if len > 0.
 */
static void truncate_indir_level1(int blk_num);
static void truncate_indir_level2(int blk_num);

static int fs_truncate(const char *path, off_t len)
{
    /* you can cheat by only implementing this for the case of len==0,
     * and an error otherwise.
     */
    if (len != 0)
        return -EINVAL; /* invalid argument */

    // get inode
    // check whether the file exists
    char _path[strlen(path) + 1];
    strcpy(_path, path);
    int inode_index = lookup(_path);
    if (inode_index < 0)
    {
        return inode_index;
    }
    struct fs_inode *inode = &inodes[inode_index];
    if (S_ISDIR(inode->mode))
    {
        return -EISDIR;
    }

    // reset blks
    int i = 0;
    for (i = 0; i < N_DIRECT; i++)
    {
        if (inode->direct[i])
        {
            FD_CLR(inode->direct[i], block_map);
            set_map();
        }
        inode->direct[i] = 0;
    }

    // reset dir_level1 blks
    if (inode->indir_1)
        truncate_indir_level1(inode->indir_1);

    // reset dir_level2 blks
    if (inode->indir_2)
    {
        truncate_indir_level2(inode->indir_2);
    }

    inode->size = 0;
    inode->indir_1 = 0;
    inode->indir_2 = 0;
    set_inode(inode_index);
    printf("truncate success");
    return SUCCESS;
}

static void truncate_indir_level1(int blk_num)
{
    // read from blocks
    int tmp[num_entry_in_blk];
    bzero(tmp, BLOCK_SIZE);
    if (disk->ops->read(disk, blk_num, 1, tmp) < 0)
        exit(1);

    // reset blks
    int i;
    for (i = 0; i < num_entry_in_blk; i++)
    {
        if (tmp[i])
        {
            FD_CLR(tmp[i], block_map);
        }
    }
    FD_CLR(blk_num, block_map);
    set_map();
}

static void truncate_indir_level2(int blk_num)
{
    // read from blks
    int tmp[num_entry_in_blk];
    bzero(tmp, BLOCK_SIZE);
    if (disk->ops->read(disk, blk_num, 1, tmp) < 0)
        exit(1);

    // reset blks
    int i;
    for (i = 0; i < num_entry_in_blk; i++)
    {
        if (tmp[i])
        {
            truncate_indir_level1(tmp[i]);
        }
    }
    FD_CLR(blk_num, block_map);
    set_map();
}

/* unlink - delete a file
 *  Errors - path resolution, ENOENT, EISDIR
 * Note that you have to delete (i.e. truncate) all the data.
 */
static int fs_unlink(const char *path)
{
    // delete all the data
    printf("in unlink");
    int val = fs_truncate(path, 0);
    if (val != SUCCESS)
    {
        return val;
    }

    // get preivous dir
    char preivous[strlen(path) + 1];
    strcpy(preivous, path);
    preivous[strrchr(preivous, '/') - preivous] = '\0';
    // set path to root
    if (strlen(preivous) == 0)
    {
        strcpy(preivous, "/");
    }
    struct fs_inode *preivous_inode = &inodes[lookup(preivous)];

    char *file_name = strrchr(path, '/') + 1;

    // remove entry
    struct fs_dirent entry[num_entry];
    if (disk->ops->read(disk, preivous_inode->direct[0], 1, entry) < 0)
    {
        exit(1);
    }
    int i;
    for (i = 0; i < num_entry; i++)
    {
        if (entry[i].valid)
        {
            if (strcmp(entry[i].name, file_name) == 0)
            {
                memset(&entry[i], 0, sizeof(struct fs_dirent));
            }
        }
    }

    int inode_index = lookup(path);
    struct fs_inode *inode = &inodes[inode_index];
    memset(inode, 0, sizeof(struct fs_inode));
    FD_CLR(inode_index, inode_map);
    set_inode(inode_index);
    set_map();
    // write disk
    if (disk->ops->write(disk, preivous_inode->direct[0], 1, entry) < 0)
    {
        exit(1);
    }
    return SUCCESS;
}

/* rmdir - remove a directory
 *  Errors - path resolution, ENOENT, ENOTDIR, ENOTEMPTY
 */
static int fs_rmdir(const char *path)
{
    // if path is root
    if (strcmp(path, "/") == 0)
    {
        return -EINVAL;
    }

    // get preivous dir
    char preivous[strlen(path) + 1];
    strcpy(preivous, path);
    preivous[strrchr(preivous, '/') - preivous] = '\0';
    if (strlen(preivous) == 0)
    {
        strcpy(preivous, "/");
    }

    // get preivous dir inode
    int dir_inode_index = lookup(preivous);
    if (dir_inode_index < 0)
    {
        return dir_inode_index;
    }
    struct fs_inode *dir_inode = &inodes[dir_inode_index];
    if (!S_ISDIR(dir_inode->mode))
    {
        return -ENOTDIR;
    }

    // get dir inode
    int inode_index = lookup(path);
    if (inode_index < 0)
    {
        return inode_index;
    }
    struct fs_inode *inode = &inodes[inode_index];
    if (!S_ISDIR(dir_inode->mode))
    {
        return -ENOTDIR;
    }

    // check whether dir is empty
    struct fs_dirent entry[num_entry];
    if (disk->ops->read(disk, inode->direct[0], 1, entry) < 0)
    {
        exit(1);
    }
    int i;
    for (i = 0; i < num_entry; i++)
    {
        if (entry[i].valid)
        {
            return -ENOTEMPTY;
        }
    }

    // get dir name
    char *dir_name = strrchr(path, '/') + 1;

    // read preivous dir entry
    struct fs_dirent dir_entry[num_entry];
    if (disk->ops->read(disk, dir_inode->direct[0], 1, dir_entry) < 0)
    {
        exit(1);
    }

    // delete dir entry
    for (i = 0; i < num_entry; i++)
    {
        if (dir_entry[i].valid && strcmp(dir_entry[i].name, dir_name) == 0)
        {
            memset(&dir_entry[i], 0, sizeof(dir_entry[i]));
        }
    }
    memset(inode, 0, sizeof(inode));

    // write map
    FD_CLR(inode->direct[0], block_map);
    FD_CLR(inode_index, inode_map);
    set_inode(inode_index);
    set_map();
    if (disk->ops->write(disk, dir_inode->direct[0], 1, dir_entry) < 0)
    {
        exit(1);
    }
    return SUCCESS;
}

/* rename - rename a file or directory
 * Errors - path resolution, ENOENT, EINVAL, EEXIST
 *
 * ENOENT - source does not exist
 * EEXIST - destination already exists
 * EINVAL - source and destination are not in the same directory
 *
 * Note that this is a simplified version of the UNIX rename
 * functionality - see 'man 2 rename' for full semantics. In
 * particular, the full version can move across directories, replace a
 * destination file, and replace an empty directory with a full one.
 */
static int fs_rename(const char *src_path, const char *dst_path)
{
    // check whether src and dst path exists
    int src_inode_index = lookup(src_path);
    if (src_inode_index < 0)
    {
        return src_inode_index;
    }
    int dst_inode_index = lookup(dst_path);
    if (dst_inode_index > 0)
    {
        return -EEXIST;
    }

    // get preivous dir
    char src_preivous[strlen(src_path) + 1];
    strcpy(src_preivous, src_path);
    src_preivous[strrchr(src_preivous, '/') - src_preivous] = '\0';

    char dst_preivous[strlen(dst_path) + 1];
    strcpy(dst_preivous, dst_path);
    dst_preivous[strrchr(dst_preivous, '/') - dst_preivous] = '\0';

    if (strcmp(src_preivous, dst_preivous) != 0)
    {
        return -EINVAL;
    }

    // check dir inode exists
    int dir_inode_index = lookup(src_preivous);
    if (dir_inode_index < 0)
    {
        return dir_inode_index;
    }
    struct fs_inode *dir_inode = &inodes[dir_inode_index];

    // get name of src and dst
    char *src_name = strrchr(src_path, '/') + 1;
    char *dst_name = strrchr(dst_path, '/') + 1;
    // name too long
    if (strlen(dst_name) >= 28)
    {
        return -EINVAL;
    }

    // rename entry
    struct fs_dirent entry[num_entry];
    if (disk->ops->read(disk, dir_inode->direct[0], 1, entry) < 0)
    {
        exit(1);
    }
    int i;
    for (i = 0; i < num_entry; i++)
    {
        if (entry[i].valid && strcmp(entry[i].name, src_name) == 0)
        {
            memset(entry[i].name, 0, sizeof(entry[i].name));
            strcpy(entry[i].name, dst_name);
        }
    }
    if (disk->ops->write(disk, dir_inode->direct[0], 1, entry) < 0)
    {
        exit(1);
    }

    return SUCCESS;
}

/* chmod - change file permissions
 * utime - change access and modification times
 *         (for definition of 'struct utimebuf', see 'man utime')
 *
 * Errors - path resolution, ENOENT.
 */
static int fs_chmod(const char *path, mode_t mode)
{
    char _path[strlen(path) + 1];
    strcpy(_path, path);
    int inode_index = lookup(_path);
    if (inode_index < 0)
    {
        return -ENOENT;
    }
    struct fs_inode *inode = &inodes[inode_index];
    if (S_ISDIR(inode->mode))
    {
        inode->mode = mode | S_IFDIR;
    }
    else
    {
        inode->mode = mode | S_IFREG;
    }
    set_inode(inode_index);
    return SUCCESS;
}

int fs_utime(const char *path, struct utimbuf *ut)
{
    char _path[strlen(path) + 1];
    strcpy(_path, path);
    int inode_index = lookup(_path);
    if (inode_index < 0)
    {
        return inode_index;
    }
    struct fs_inode *inode = &inodes[inode_index];
    inode->mtime = ut->modtime;
    set_inode(inode_index);
    return SUCCESS;
}

/* read - read data from an open file.
 * should return exactly the number of bytes requested, except:
 *   - if offset >= file len, return 0
 *   - if offset+len > file len, return bytes from offset to EOF
 *   - on error, return <0
 * Errors - path resolution, ENOENT, EISDIR
 */
static int fs_read_block(int blk_num, off_t offset, int len, char *buf);
static int fs_read_direct(struct fs_inode *inode, off_t offset, size_t len, char *buf);
static int fs_read_indir1(size_t blk, off_t offset, int len, char *buf);
static int fs_read_indir2(size_t start_block, off_t offset, int len, char *buf);

static int fs_read(const char *path, char *buf, size_t len, off_t offset, struct fuse_file_info *fi)
{
    char _path[strlen(path) + 1];
    strcpy(_path, path);
    int inode_index = lookup(_path);
    if (inode_index < 0)
    {
        return inode_index;
    }
    struct fs_inode *inode = &inodes[inode_index];
    if (!S_ISREG(inode->mode))
    {
        return -EISDIR;
    }

    if (offset >= inode->size)
    {
        return 0;
    }
    if (offset + len > inode->size)
    {
        len = inode->size - offset;
    }

    size_t len_read = 0;
    size_t len_bak = len;

    // read direct
    if (len_bak > 0 && offset < direct_sz)
    {
        // printf("reading direct..");
        len_read = fs_read_direct(inode, offset, len_bak, buf);
        offset += len_read;
        len_bak -= len_read;
        buf += len_read;
        // printf("read direct success");
    }

    // read level 1
    if (len_bak > 0 && offset < direct_sz + indirect_level1_sz)
    {
        // printf("reading indir1");
        len_read = fs_read_indir1(inode->indir_1, offset - direct_sz, len_bak, buf);
        offset += len_read;
        len_bak -= len_read;
        buf += len_read;
        // printf("read indir1 successs");
    }

    // read level2
    if (len_bak > 0 && offset < direct_sz + indirect_level1_sz + indirect_level2_sz)
    {
        // printf("reading indir2");
        len_read = fs_read_indir2(inode->indir_2, offset - direct_sz - indirect_level1_sz, len_bak, buf);
        offset += len_read;
        len_bak -= len_read;
        buf += len_read;
        // printf("read indir2 success");
    }
    // return actual length read
    // printf("len: %d, read: %d\n", len, len - len_bak);
    return len - len_bak;
}

static int fs_read_direct(struct fs_inode *inode, off_t offset, size_t len, char *buf)
{
    size_t len_read, len_bak = len;
    int blk_num, blk_offset;
    for (blk_num = offset / BLOCK_SIZE, blk_offset = offset % BLOCK_SIZE;
         blk_num < N_DIRECT && len_bak > 0;
         blk_num++, blk_offset = 0)
    {

        len_read = blk_offset + len_bak > BLOCK_SIZE ? BLOCK_SIZE - blk_offset : len_bak;

        if (!inode->direct[blk_num])
            return len - len_bak;
        len_read = fs_read_block(inode->direct[blk_num], blk_offset, len_read, buf);

        buf += len_read;
        len_bak -= len_read;
    }
    return len - len_bak;
}

static int fs_read_indir1(size_t blk, off_t offset, int len, char *buf)
{
    int blk_index[num_entry_in_blk];
    if (disk->ops->read(disk, blk, 1, blk_index) < 0)
    {
        exit(1);
    }

    size_t len_read, len_bak = len;
    int blk_num, blk_offset;
    for (blk_num = offset / BLOCK_SIZE, blk_offset = offset % BLOCK_SIZE;
         blk_num < num_entry_in_blk && len_bak > 0;
         blk_num++)
    {
        // calculate length to read
        if (blk_offset + len_bak > BLOCK_SIZE)
        {
            len_read = BLOCK_SIZE - blk_offset;
        }
        else
        {
            len_read = len_bak;
        }

        if (!blk_index[blk_num])
        {
            return len - len_bak;
        }
        len_read = fs_read_block(blk_index[blk_num], blk_offset, len_read, buf);

        buf += len_read;
        len_bak -= len_read;
        blk_offset = 0;
    }
    return len - len_bak;
}

static int fs_read_indir2(size_t blk, off_t offset, int len, char *buf)
{
    int blk_index[num_entry_in_blk];
    if (disk->ops->read(disk, blk, 1, blk_index) < 0)
    {
        return 0;
    }

    size_t len_read, len_bak = len;
    int blk_num, blk_offset;
    for (blk_num = offset / indirect_level1_sz, blk_offset = offset % indirect_level1_sz;
         blk_num < num_entry_in_blk && len_bak > 0;
         blk_num++)
    {
        if (blk_offset + len_bak > indirect_level1_sz)
        {
            len_read = indirect_level1_sz - blk_offset;
        }
        else
        {
            len_read = len_bak;
        }

        len_read = fs_read_indir1(blk_index[blk_num], blk_offset, len_read, buf);
        buf += len_read;
        len_bak -= len_read;
        blk_offset = 0;
    }
    return len - len_bak;
}

/*
*   read data from a block at offset
*/
static int fs_read_block(int blk_num, off_t offset, int len, char *buf)
{
    char tmp[BLOCK_SIZE];
    bzero(tmp, BLOCK_SIZE);
    if (disk->ops->read(disk, blk_num, 1, tmp) < 0)
        exit(1);
    memcpy(buf, tmp + offset, len);
    return len;
}

/* write - write data to a file
 * It should return exactly the number of bytes requested, except on
 * error.
 * Errors - path resolution, ENOENT, EISDIR
 *  return EINVAL if 'offset' is greater than current file length.
 *  (POSIX semantics support the creation of files with "holes" in them, 
 *   but we don't)
 */
static int fs_write_direct(size_t inode_index, off_t offset, size_t len, const char *buf);
static int fs_write_indir1(size_t blk, off_t offset, int len, const char *buf);
static int fs_write_indir2(size_t blk, off_t offset, int len, const char *buf);

static int fs_write(const char *path, const char *buf, size_t len,
                    off_t offset, struct fuse_file_info *fi)
{
    char _path[strlen(path) + 1];
    strcpy(_path, path);
    int inode_index = lookup(_path);
    if (inode_index < 0)
    {
        return inode_index;
    }
    struct fs_inode *inode = &inodes[inode_index];
    if (!S_ISREG(inode->mode))
    {
        return -EISDIR;
    }

    if (offset > inode->size)
    {
        return 0;
    }

    size_t len_bak = len;
    size_t len_write;

    if (len_bak > 0 && offset < direct_sz)
    {
        len_write = fs_write_direct(inode_index, offset, len_bak, buf);
        offset += len_write;
        len_bak -= len_write;
        buf += len_write;
    }

    if (len_bak > 0 && offset < direct_sz + indirect_level1_sz)
    {
        if (!inode->indir_1)
        {
            int available_blk = search_available_blk();
            if (available_blk < 0)
            {
                return len_bak - len;
            }
            inode->indir_1 = available_blk;
            set_inode(inode_index);
            FD_SET(available_blk, block_map);
            set_map();
        }

        // write to indir 1
        len_write = fs_write_indir1(inode->indir_1, offset - direct_sz, len_bak, buf);
        offset += len_write;
        len_bak -= len_write;
        buf += len_write;
    }

    // write indirect 2 blocks
    if (len_bak > 0 && offset < direct_sz + indirect_level1_sz + indirect_level2_sz)
    {
        // allocate indir2
        if (!inode->indir_2)
        {
            int available_blk = search_available_blk();
            if (available_blk < 0)
                return len_bak - len;
            inode->indir_1 = available_blk;
            set_inode(inode_index);
            FD_SET(available_blk, block_map);
            set_map();
        }

        len_write = fs_write_indir2(inode->indir_2, offset - direct_sz - indirect_level1_sz, len_bak, buf);
        offset += len_write;
        len_bak -= len_write;
        buf += len_write;
    }

    if (offset > inode->size)
    {
        inode->size = offset;
        set_inode(inode_index);
    }

    return len - len_bak;
}

static int fs_write_direct(size_t inode_index, off_t offset, size_t len, const char *buf)
{
    struct fs_inode *inode = &inodes[inode_index];
    size_t len_write, len_bak = len;
    int blk_num, blk_offset;
    for (blk_num = offset / BLOCK_SIZE, blk_offset = offset % BLOCK_SIZE;
         blk_num < N_DIRECT && len_bak > 0;
         blk_num++)
    {
        if (blk_offset + len_bak > BLOCK_SIZE)
        {
            len_write = BLOCK_SIZE - blk_offset;
        }
        else
        {
            len_write = len_bak;
        }
        len_bak -= len_write;

        if (!inode->direct[blk_num])
        {
            int available_blk = search_available_blk();
            if (available_blk < 0)
            {
                return len - len_bak;
            }
            inode->direct[blk_num] = available_blk;
            set_inode(inode_index);
            FD_SET(available_blk, block_map);
            set_map();
        }

        char tmp[BLOCK_SIZE];
        if (disk->ops->read(disk, inode->direct[blk_num], 1, tmp) < 0)
        {
            exit(1);
        }
        memcpy(tmp + blk_offset, buf, len_write);
        if (disk->ops->write(disk, inode->direct[blk_num], 1, tmp) < 0)
        {
            exit(1);
        }
        buf += len_write;
        blk_offset = 0;
    }
    return len - len_bak;
}

static int fs_write_indir1(size_t blk, off_t offset, int len, const char *buf)
{
    int blk_index[num_entry_in_blk];
    if (disk->ops->read(disk, blk, 1, blk_index) < 0)
    {
        exit(1);
    }

    size_t len_write, len_bak = len;
    int blk_num, blk_offset;
    for (blk_num = offset / BLOCK_SIZE, blk_offset = offset % BLOCK_SIZE;
         blk_num < num_entry_in_blk && len_bak > 0;
         blk_num++)
    {
        // calculate length to read
        len_write = blk_offset + len_bak > BLOCK_SIZE ? BLOCK_SIZE - blk_offset : len_bak;
        len_bak -= len_write;

        // allocate block if not exists
        if (!blk_index[blk_num])
        {
            int available_blk = search_available_blk();
            if (available_blk < 0)
            {
                return len - len_bak;
            }
            blk_index[blk_num] = available_blk;

            if (disk->ops->write(disk, blk, 1, blk_index))
            {
                exit(1);
            }
            FD_SET(available_blk, block_map);
            set_map();
        }

        char tmp[BLOCK_SIZE];
        if (disk->ops->read(disk, blk_index[blk_num], 1, tmp) < 0)
        {
            exit(1);
        }
        memcpy(tmp + blk_offset, buf, len_write);
        if (disk->ops->write(disk, blk_index[blk_num], 1, tmp) < 0)
        {
            exit(1);
        }

        buf += len_write;
        blk_offset = 0;
    }
    return len - len_bak;
}

static int fs_write_indir2(size_t blk, off_t offset, int len, const char *buf)
{
    int blk_index[num_entry_in_blk];
    if (disk->ops->read(disk, blk, 1, blk_index) < 0)
    {
        exit(1);
    }

    size_t len_write, len_bak = len;
    int blk_num, blk_offset;
    for (blk_num = offset / BLOCK_SIZE, blk_offset = offset % BLOCK_SIZE;
         blk_num < num_entry_in_blk && len_bak > 0;
         blk_num++)
    {
        if (blk_offset + len_bak > indirect_level1_sz)
        {
            len_write = indirect_level1_sz - blk_offset;
        }
        else
        {
            len_write = len_bak;
        }
        len_bak -= len_write;

        if (!blk_index[blk_num])
        {
            int available_blk = search_available_blk();
            if (available_blk < 0)
            {
                return len - len_bak;
            }
            blk_index[blk_num] = available_blk;

            if (disk->ops->write(disk, blk, 1, blk_index))
            {
                exit(1);
            }
            FD_SET(available_blk, block_map);
            set_map();
        }

        len_write = fs_write_indir1(blk_index[blk_num], blk_offset, len_write, buf);
        if (len_write == 0)
        {
            return len - len_bak;
        }

        buf += len_write;
        blk_offset = 0;
    }
    return len - len_bak;
}

static int fs_open(const char *path, struct fuse_file_info *fi)
{
    if (is_file(path))
    {
        fi->fh = lookup(path);
        return SUCCESS;
    }
    else
    {
        return EISDIR;
    }
}

static int fs_release(const char *path, struct fuse_file_info *fi)
{
    int val;
    if ((val = is_file(path)) == SUCCESS)
    {
        fi->fh = -1;
    }
    return val;
}

/* statfs - get file system statistics
 * see 'man 2 statfs' for description of 'struct statvfs'.
 * Errors - none. 
 */
static int fs_statfs(const char *path, struct statvfs *st)
{
    /* needs to return the following fields (set others to zero):
     *   f_bsize = BLOCK_SIZE
     *   f_blocks = total image - metadata
     *   f_bfree = f_blocks - blocks used
     *   f_bavail = f_bfree
     *   f_namelen = <whatever your max namelength is>
     *
     * this should work fine, but you may want to add code to
     * calculate the correct values later.
     */
    st->f_bsize = FS_BLOCK_SIZE;
    st->f_blocks = 0; /* probably want to */
    st->f_bfree = 0;  /* change these */
    st->f_bavail = 0; /* values */
    st->f_namemax = 27;

    return 0;
}

/* operations vector. Please don't rename it, as the skeleton code in
 * misc.c assumes it is named 'fs_ops'.
 */
struct fuse_operations fs_ops = {
    .init = fs_init,
    .getattr = fs_getattr,
    .opendir = fs_opendir,
    .readdir = fs_readdir,
    .releasedir = fs_releasedir,
    .mknod = fs_mknod,
    .mkdir = fs_mkdir,
    .unlink = fs_unlink,
    .rmdir = fs_rmdir,
    .rename = fs_rename,
    .chmod = fs_chmod,
    .utime = fs_utime,
    .truncate = fs_truncate,
    .open = fs_open,
    .read = fs_read,
    .write = fs_write,
    .release = fs_release,
    .statfs = fs_statfs,
};
