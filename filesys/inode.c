#include "filesys/inode.h"
#include <list.h>
#include <debug.h>
#include <round.h>
#include <string.h>
#include "filesys/filesys.h"
#include "filesys/free-map.h"
#include "threads/malloc.h"
#define DLINKS 123

/* Identifies an inode. */
#define INODE_MAGIC 0x494e4f44


struct indirect_inode
  {
    int directs[128];
  };

struct doubly_indirect_inode
  {
    int indirects[128];
  };

/* On-disk inode.
   Must be exactly BLOCK_SECTOR_SIZE bytes long. */
struct inode_disk
  {
    off_t length;                       /* File size in bytes. */
    unsigned magic;                     /* Magic number. */
    int directs[DLINKS];                /* Direct links. */
    int indirect;                       /* An indirect link. */
    int dindirect;                      /* A doubly-indirect link. */
    int isDirectory;                    /* 0 if File, 1 if Directory */
  };

/* Returns the number of sectors to allocate for an inode SIZE
   bytes long. */
static inline size_t
bytes_to_sectors (off_t size)
{
  if (size <= DLINKS * BLOCK_SECTOR_SIZE)
    return DIV_ROUND_UP (size, BLOCK_SECTOR_SIZE);
  else if (size <= (DLINKS + 128) * BLOCK_SECTOR_SIZE)
    return 1 + DIV_ROUND_UP (size, BLOCK_SECTOR_SIZE);
  else {
    int newSize = size - ((DLINKS + 128) * BLOCK_SECTOR_SIZE);
    return DLINKS + 130 +  DIV_ROUND_UP (newSize, BLOCK_SECTOR_SIZE * 128) + DIV_ROUND_UP (newSize, BLOCK_SECTOR_SIZE);
  }
}

/* In-memory inode. */
struct inode 
  {
    struct list_elem elem;              /* Element in inode list. */
    block_sector_t sector;              /* Sector number of disk location. */
    int open_cnt;                       /* Number of openers. */
    bool removed;                       /* True if deleted, false otherwise. */
    int deny_write_cnt;                 /* 0: writes ok, >0: deny writes. */
    struct inode_disk data;             /* Inode content. */
  };
int
get_isDir (struct inode *inode) 
{
  return inode->data.isDirectory;
}

/* Returns the block device sector that contains byte offset POS
   within INODE.
   Returns -1 if INODE does not contain data for a byte at offset
   POS. */
static block_sector_t
byte_to_sector (const struct inode *inode, off_t pos) 
{
  ASSERT (inode != NULL);
  if (pos < inode->data.length){
    // case in which pos is within a direct index.
    if(pos < DLINKS * BLOCK_SECTOR_SIZE) {
      return inode->data.directs[pos / BLOCK_SECTOR_SIZE]; 
    }
    // case in which pos is within the single indirect index.
    else if(pos < (DLINKS + 128) * BLOCK_SECTOR_SIZE){
      int newPos = pos - (DLINKS * BLOCK_SECTOR_SIZE);
      struct indirect_inode buff;
      block_read(fs_device, inode->data.indirect, &buff);
      return buff.directs[newPos/BLOCK_SECTOR_SIZE];
    }
    // case in which pos is within the doubly indirect index
    else{
      int newestPos = pos - ((DLINKS + 128) * BLOCK_SECTOR_SIZE);
      int indirectIndex = newestPos/(BLOCK_SECTOR_SIZE * 128);
      int directIndex = (newestPos/BLOCK_SECTOR_SIZE)%128;
      struct doubly_indirect_inode buffer;
      struct indirect_inode buffer2;
      block_read(fs_device, inode->data.dindirect, &buffer);
      block_read(fs_device, buffer.indirects[indirectIndex], &buffer2);
      return buffer2.directs[directIndex];
    }
  }
  else {
    return -1;
  }
}

/* List of open inodes, so that opening a single inode twice
   returns the same `struct inode'. */
static struct list open_inodes;

/* Initializes the inode module. */
void
inode_init (void) 
{
  list_init (&open_inodes);
}

/* Called When an allocation on free map fails. */
void
allocation_fail_3 (int indirect_index, int direct_index, struct inode_disk* disk_inode, 
struct indirect_inode* indirect_disk_inode, struct doubly_indirect_inode* didi)
{
  allocation_fail_2(128, indirect_disk_inode);
  free_map_release(disk_inode->indirect, 1);
  free_map_release(disk_inode->dindirect, 1);
  allocation_fail(DLINKS, disk_inode);
  int indir, dir;
  for (indir = 0; indir <= indirect_index; indir++) {
    int end = (indir == indirect_index) ? direct_index : 128;
    struct indirect_inode d_indirect_disk_inode;
    block_read (fs_device, didi->indirects[indir], &d_indirect_disk_inode);
    for (dir = 0; dir < end; dir++) {
      free_map_release(d_indirect_disk_inode.directs[dir], 1);  
    }
  }
  free (didi);
  free (indirect_disk_inode);
  free (disk_inode);
}


/* Called When an allocation on free map fails. */
void
allocation_fail (int i, struct inode_disk* disk_inode) 
{
  int j;
  for (j = 0; j < i; j++) {
    free_map_release (disk_inode->directs[j], 1); 
  }
}

/* Called When an allocation on free map fails. */
void
allocation_fail_2 (int i, struct indirect_inode* indirect_disk_inode) 
{
  int j;
  for (j = 0; j < i; j++) {
    free_map_release (indirect_disk_inode->directs[j], 1); 
  }
}

/* A helper function for inode_create and inode_grow. */
bool inode_process (block_sector_t sector, off_t length, struct inode_disk* arg_disk_inode) 
{
  static char zeros[BLOCK_SECTOR_SIZE];
  struct inode_disk *disk_inode = NULL;
  ASSERT (length >= 0);
  int added_sectors;

  /* If this assertion fails, the inode structure is not exactly
     one sector in size, and you should fix that. */
  ASSERT (sizeof *disk_inode == BLOCK_SECTOR_SIZE);
  disk_inode = calloc (1, sizeof *disk_inode);

  if (disk_inode != NULL)
    {
      int sectors = bytes_to_sectors (length); // get total sectors needed, including those for indices.
      int i;

      if (arg_disk_inode != NULL) {
        if (length < arg_disk_inode->length) {
          free (disk_inode);
          return;
        }
        block_read (fs_device, sector, disk_inode);
        disk_inode->length = length; 
      }

      if (arg_disk_inode == NULL) {
        disk_inode->length = length;
        disk_inode->magic = INODE_MAGIC;
        disk_inode->indirect = -1;
        disk_inode->dindirect = -1;
        size_t direct_index;
        for (direct_index = 0; direct_index < DLINKS; direct_index++) 
          disk_inode->directs[direct_index] = -1;
      }

      block_write (fs_device, sector, disk_inode);
      for (i = 0; i < sectors; i++) {

        // 1st case: data pointed to by direct links. 
        if (i < DLINKS) {
          if (disk_inode->directs[i] != -1) continue;
          if (free_map_allocate (1, &disk_inode->directs[i])){
            block_write (fs_device, disk_inode->directs[i], zeros);
          }
          else {
            allocation_fail(i, disk_inode);
            free (disk_inode);
            return false;
          }
        }

        // 2nd case: indirect link block.
        else if (i == DLINKS) {
          if (disk_inode->indirect != -1) continue;
          if (free_map_allocate (1, &disk_inode->indirect)) {
            struct indirect_inode *indirect_disk_inode = NULL;
            indirect_disk_inode = calloc (1, sizeof *indirect_disk_inode);
            if (indirect_disk_inode == NULL) {
              free_map_release (disk_inode->indirect, 1); 
              allocation_fail(i, disk_inode);
              free (disk_inode);
              return false;
            }
            else {
              size_t indirect_index;
              for (indirect_index = 0; indirect_index < 128; indirect_index++) 
                indirect_disk_inode->directs[indirect_index] = -1;
              block_write (fs_device, disk_inode->indirect, indirect_disk_inode);
              free (indirect_disk_inode);
            }
          }
          else {
            allocation_fail(i, disk_inode);
            free (disk_inode);
            return false;
          }
        }

        else break;
      }

      // 3rd case: data pointed to by direct links contained in the indirect link block. 
      int end = sectors - DLINKS - 1 < 128 ? sectors - DLINKS - 1 : 128;
      struct indirect_inode* indirect_disk_inode = NULL;
      if (end <= 0) {
        block_write (fs_device, sector, disk_inode);
        if (arg_disk_inode != NULL) {
          block_read (fs_device, sector, arg_disk_inode); 
        }
        free (disk_inode);
        return true;
      }
      if (end > 0) {
        indirect_disk_inode = calloc (1, sizeof *indirect_disk_inode);
        if (indirect_disk_inode == NULL) {
          free_map_release(disk_inode->indirect, 1);
          allocation_fail(DLINKS, disk_inode);
          free (disk_inode);
          return false;
        }
        else {
          block_read (fs_device, disk_inode->indirect, indirect_disk_inode);
        }
      }

      for (i = 0; i < end; i++) {
        if (indirect_disk_inode->directs[i] != -1) continue;
        if (free_map_allocate (1, &indirect_disk_inode->directs[i]))
          block_write (fs_device, indirect_disk_inode->directs[i], zeros);
        else {
          allocation_fail_2(i, indirect_disk_inode);
          free_map_release(disk_inode->indirect, 1);
          allocation_fail(DLINKS, disk_inode);
          free (indirect_disk_inode);
          free (disk_inode);
          return false;
        }
      }
      block_write (fs_device, disk_inode->indirect, indirect_disk_inode);
      block_write (fs_device, sector, disk_inode); 
      if (arg_disk_inode != NULL) {
        block_read (fs_device, sector, arg_disk_inode); 
      }

      if (sectors > DLINKS + 1 + 128) {

        // 4th case: doubly-indirect block.
        struct doubly_indirect_inode* didi = NULL;
        didi = calloc (1, sizeof *didi);
        if (didi == NULL) {
            allocation_fail_2(128, indirect_disk_inode);
            free_map_release(disk_inode->indirect, 1);
            allocation_fail(DLINKS, disk_inode);
            free (indirect_disk_inode);
            free (disk_inode);
            return false;
        }
        else {
          size_t dindirect_index;
          for (dindirect_index = 0; dindirect_index < 128; dindirect_index++) 
            didi->indirects[dindirect_index] = -1;
          if (disk_inode->dindirect != -1) {
            block_read (fs_device, disk_inode->dindirect, didi);
          }
          else {
            if (free_map_allocate (1, &disk_inode->dindirect)) {
              block_write (fs_device, disk_inode->dindirect, didi);
            }
            else {
              allocation_fail_2(128, indirect_disk_inode);
              free_map_release(disk_inode->indirect, 1);
              allocation_fail(DLINKS, disk_inode);
              free (didi);
              free (indirect_disk_inode);
              free (disk_inode);
              return false;
            }
          }
        }

        // 5th case: data and indirect blocks in doubly-indirect block.
        int n_indirect = DIV_ROUND_UP((sectors - DLINKS - 130), 129);    // Number of indirect blocks needed. 
        int last_direct_n = (sectors - DLINKS - 130 - n_indirect) % 128; // Number of direct blocks in the last indirect block.
        size_t indirect_index, direct_index;

        for (indirect_index = 0; indirect_index < n_indirect ; indirect_index++) {
          struct indirect_inode* d_indirect_disk_inode = NULL;
          d_indirect_disk_inode = calloc (1, sizeof *d_indirect_disk_inode);
          if (d_indirect_disk_inode == NULL) {
            allocation_fail_3(indirect_index, 0, disk_inode, indirect_disk_inode, didi);
            return false;
          }
          else {
            size_t ind_index; 
            for (ind_index = 0; ind_index < 128; ind_index++) 
              d_indirect_disk_inode->directs[ind_index] = -1;
            if (didi->indirects[indirect_index] == -1) {
              if (!free_map_allocate (1, &didi->indirects[indirect_index])) {
                allocation_fail_3(indirect_index, 0, disk_inode, indirect_disk_inode, didi);
                free(d_indirect_disk_inode);
                return false;
              } 
            }
            else {
              block_read (fs_device, didi->indirects[indirect_index], d_indirect_disk_inode);
            }
          }

          /*** One indirect block done, next comes data of this block. ***/

          size_t last_direct_index = (indirect_index == n_indirect - 1) ? last_direct_n : 128;
          for (direct_index = 0; direct_index < last_direct_index; direct_index++) {
            if (d_indirect_disk_inode->directs[direct_index] != -1) continue;
            if (free_map_allocate (1, &d_indirect_disk_inode->directs[direct_index])) {
              block_write (fs_device, d_indirect_disk_inode->directs[direct_index], zeros);
            }
            else {
              allocation_fail_3(indirect_index, direct_index, disk_inode, indirect_disk_inode, didi);
              return false;
            }
          }

          /*** Update this indirect block. ***/

          block_write (fs_device, didi->indirects[indirect_index], d_indirect_disk_inode);
          free(d_indirect_disk_inode);
        }

        /*** Update doubly-indirect block, inode creation completes. ***/
 
        block_write (fs_device, disk_inode->dindirect, didi);
        free (didi);
      }

      block_write (fs_device, sector, disk_inode); // update disk_inode on the disk.
      if (arg_disk_inode != NULL) {
        block_read (fs_device, sector, arg_disk_inode); 
      }
      free (indirect_disk_inode);
      free (disk_inode);
      return true;
    }
  return false;
}


/* Initializes an inode with LENGTH bytes of data and
   writes the new inode to sector SECTOR on the file system
   device.
   Returns true if successful.
   Returns false if memory or disk allocation fails. */
bool
inode_create (block_sector_t sector, off_t length)
{
  return inode_process (sector, length, NULL);
}


/* A function that grows the size of an inode. 
   It has no effect if the length of the inode is bigger than new_size. */
void
inode_grow (block_sector_t sector, off_t new_size, struct inode_disk* disk_inode) {
  inode_process (sector, new_size, disk_inode);
}

/* Reads an inode from SECTOR
   and returns a `struct inode' that contains it.
   Returns a null pointer if memory allocation fails. */
struct inode *
inode_open (block_sector_t sector)
{
  struct list_elem *e;
  struct inode *inode;

  /* Check whether this inode is already open. */
  for (e = list_begin (&open_inodes); e != list_end (&open_inodes);
       e = list_next (e)) 
    {
      inode = list_entry (e, struct inode, elem);
      if (inode->sector == sector) 
        {
          inode_reopen (inode);
          return inode; 
        }
    }

  /* Allocate memory. */
  inode = malloc (sizeof *inode);
  if (inode == NULL)
    return NULL;

  /* Initialize. */
  list_push_front (&open_inodes, &inode->elem);
  inode->sector = sector;
  inode->open_cnt = 1;
  inode->deny_write_cnt = 0;
  inode->removed = false;
  block_read (fs_device, inode->sector, &inode->data);
  return inode;
}

/* Reopens and returns INODE. */
struct inode *
inode_reopen (struct inode *inode)
{
  if (inode != NULL)
    inode->open_cnt++;
  return inode;
}

/* Returns INODE's inode number. */
block_sector_t
inode_get_inumber (const struct inode *inode)
{
  return inode->sector;
}

/* Closes INODE and writes it to disk.
   If this was the last reference to INODE, frees its memory.
   If INODE was also a removed inode, frees its blocks. */
void
inode_close (struct inode *inode) 
{
  /* Ignore null pointer. */
  if (inode == NULL)
    return;

  /* Release resources if this was the last opener. */
  if (--inode->open_cnt == 0)
    {
      /* Remove from inode list and release lock. */
      list_remove (&inode->elem);
 
      /* Deallocate blocks if removed. */
      if (inode->removed) 
        {
          free_map_release (inode->sector, 1);
          free_map_release (inode->data.directs[0],
                            bytes_to_sectors (inode->data.length)); 
        }

      free (inode); 
    }
}

/* Marks INODE to be deleted when it is closed by the last caller who
   has it open. */
void
inode_remove (struct inode *inode) 
{
  ASSERT (inode != NULL);
  inode->removed = true;
}

/* Reads SIZE bytes from INODE into BUFFER, starting at position OFFSET.
   Returns the number of bytes actually read, which may be less
   than SIZE if an error occurs or end of file is reached. */
off_t
inode_read_at (struct inode *inode, void *buffer_, off_t size, off_t offset) 
{
  uint8_t *buffer = buffer_;
  off_t bytes_read = 0;
  uint8_t *bounce = NULL;

  while (size > 0) 
    {
      /* Disk sector to read, starting byte offset within sector. */
      block_sector_t sector_idx = byte_to_sector (inode, offset);
      int sector_ofs = offset % BLOCK_SECTOR_SIZE;

      /* Bytes left in inode, bytes left in sector, lesser of the two. */
      off_t inode_left = inode_length (inode) - offset;
      int sector_left = BLOCK_SECTOR_SIZE - sector_ofs;
      int min_left = inode_left < sector_left ? inode_left : sector_left;

      /* Number of bytes to actually copy out of this sector. */
      int chunk_size = size < min_left ? size : min_left;
      if (chunk_size <= 0)
        break;

      if (sector_ofs == 0 && chunk_size == BLOCK_SECTOR_SIZE)
        {
          /* Read full sector directly into caller's buffer. */
          block_read (fs_device, sector_idx, buffer + bytes_read);
        }
      else 
        {
          /* Read sector into bounce buffer, then partially copy
             into caller's buffer. */
          if (bounce == NULL) 
            {
              bounce = malloc (BLOCK_SECTOR_SIZE);
              if (bounce == NULL)
                break;
            }
          block_read (fs_device, sector_idx, bounce);
          memcpy (buffer + bytes_read, bounce + sector_ofs, chunk_size);
        }
      
      /* Advance. */
      size -= chunk_size;
      offset += chunk_size;
      bytes_read += chunk_size;
    }
  free (bounce);

  return bytes_read;
}

/* Writes SIZE bytes from BUFFER into INODE, starting at OFFSET.
   Returns the number of bytes actually written, which may be
   less than SIZE if end of file is reached or an error occurs.
   (Normally a write at end of file would extend the inode, but
   growth is not yet implemented.) */
off_t
inode_write_at (struct inode *inode, const void *buffer_, off_t size,
                off_t offset) 
{
  const uint8_t *buffer = buffer_;
  off_t bytes_written = 0;
  uint8_t *bounce = NULL;

  if (inode->deny_write_cnt)
    return 0;

  inode_grow (inode->sector, offset + size, &inode->data);

  while (size > 0) 
    {
      /* Sector to write, starting byte offset within sector. */
      block_sector_t sector_idx = byte_to_sector (inode, offset);
      int sector_ofs = offset % BLOCK_SECTOR_SIZE;

      /* Bytes left in inode, bytes left in sector, lesser of the two. */
      off_t inode_left = inode_length (inode) - offset;
      int sector_left = BLOCK_SECTOR_SIZE - sector_ofs;
      int min_left = inode_left < sector_left ? inode_left : sector_left;

      /* Number of bytes to actually write into this sector. */
      int chunk_size = size < min_left ? size : min_left;
      if (chunk_size <= 0)
        break;

      if (sector_ofs == 0 && chunk_size == BLOCK_SECTOR_SIZE)
        {
          /* Write full sector directly to disk. */
          block_write (fs_device, sector_idx, buffer + bytes_written);
        }
      else 
        {
          /* We need a bounce buffer. */
          if (bounce == NULL) 
            {
              bounce = malloc (BLOCK_SECTOR_SIZE);
              if (bounce == NULL)
                break;
            }

          /* If the sector contains data before or after the chunk
             we're writing, then we need to read in the sector
             first.  Otherwise we start with a sector of all zeros. */
          if (sector_ofs > 0 || chunk_size < sector_left) 
            block_read (fs_device, sector_idx, bounce);
          else
            memset (bounce, 0, BLOCK_SECTOR_SIZE);
          memcpy (bounce + sector_ofs, buffer + bytes_written, chunk_size);
          block_write (fs_device, sector_idx, bounce);
        }

      /* Advance. */
      size -= chunk_size;
      offset += chunk_size;
      bytes_written += chunk_size;
    }
  free (bounce);

  return bytes_written;
}

/* Disables writes to INODE.
   May be called at most once per inode opener. */
void
inode_deny_write (struct inode *inode) 
{
  inode->deny_write_cnt++;
  ASSERT (inode->deny_write_cnt <= inode->open_cnt);
}

/* Re-enables writes to INODE.
   Must be called once by each inode opener who has called
   inode_deny_write() on the inode, before closing the inode. */
void
inode_allow_write (struct inode *inode) 
{
  ASSERT (inode->deny_write_cnt > 0);
  ASSERT (inode->deny_write_cnt <= inode->open_cnt);
  inode->deny_write_cnt--;
}

/* Returns the length, in bytes, of INODE's data. */
off_t
inode_length (const struct inode *inode)
{
  return inode->data.length;
}
