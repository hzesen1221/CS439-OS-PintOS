#include "userprog/syscall.h"
#include <stdio.h>
#include <syscall-nr.h>
#include "threads/interrupt.h"
#include "threads/thread.h"
#include "userprog/process.h"
#include "threads/synch.h"
#include "devices/shutdown.h"
#include "threads/vaddr.h"
#include "userprog/pagedir.h"
#include "filesys/filesys.h"
#include "filesys/file.h"

void halt (void){
  shutdown_power_off();
  return;
}

void close (int fd){
  struct thread* cur = thread_current();
  cur->file_descriptors[fd] = 0;
  return; 
}

void exit (int status){
  struct thread* cur = thread_current();
  cur->exit_status = status;
  cur->exit_called = true;
  int i = 2;
  for (; i < 130; i++) {
      cur->file_descriptors[i] = 0; 
  } 
  thread_exit();
}

pid_t exec (const char *cmd_line){
  return process_execute(cmd_line);
}

int wait (pid_t pid){
  return process_wait(pid);
}

bool create (const char *file, unsigned initial_size){
  return filesys_create(file, initial_size);
}

bool remove (const char *file){
  return filesys_remove(file);
}

int open (const char *file){
  struct file* f = filesys_open(file);
  if (!f) return -1;
  struct thread* cur = thread_current();
  int i = 2;
  for (; i < 130; i++) {
    if (!cur->file_descriptors[i]) {
      cur->file_descriptors[i] = f;
      return i;
    }
  } 
  return -1;
}

int filesize (int fd){
  struct thread* cur = thread_current();
  struct file *file = cur->file_descriptors[fd];
  return file_length(file);
}

int read (int fd, void *buffer, unsigned size){
  if (fd == 1) return -1;
  else if (fd == 0){
    int i;
    for (i = 0; i < size; i++)
      input_getc();
  }
  else {
   // lock_acquire(&lock);
    struct thread* cur = thread_current();
    struct file *file = cur->file_descriptors[fd];
    return file_read (file, buffer, size);
   // lock_release(&lock);
  }
}

int write (int fd, const void *buffer, unsigned size){
  if (fd == 0) return -1;
  else if (fd == 1) {
    putbuf(buffer, size);
    return size;
  }
  else {
    
    struct thread* cur = thread_current();
    struct file *file = cur->file_descriptors[fd];
    //lock_acquire(&lock);
    return file_write (file, buffer, size);
    //lock_release(&lock); 
  }
}

void seek (int fd, unsigned position){
  struct thread* cur = thread_current();
  struct file *file = cur->file_descriptors[fd];
  if (filesize(fd) > position)
    file_seek(file, position);
}

unsigned tell (int fd){
  struct thread* cur = thread_current();
  struct file *file = cur->file_descriptors[fd];
  return file_tell(file);
}



static void
syscall_handler (struct intr_frame *f ) 
{
  struct thread* cur = thread_current();
  uint32_t *pd = cur->pagedir;
  int *arg_ptr = f->esp;	
  arg_ptr--;
  /* Check the return address. */  
  if (arg_ptr == NULL || !is_user_vaddr(arg_ptr) || pagedir_get_page(pd, arg_ptr) == NULL) {
    f->eax = -1;
    exit(-1);  
    return;
  }

  /* Increment esp to get SYSCALL_NUM, and then use a switch structure to handle different system functions. Validations are performed here. */
  arg_ptr++;
  switch ((*arg_ptr)) {

    case SYS_HALT: {
       halt();
       return; }

    case SYS_EXIT: { 
      if (!is_user_vaddr(arg_ptr+1)) {
        f->eax = -1;
        exit(-1);
        return;
      }
      exit(*(arg_ptr + 1));
      return; }

    case SYS_EXEC: { 
      arg_ptr++;
      const char* cmd_line = *arg_ptr;
      if (cmd_line == NULL || !is_user_vaddr(cmd_line) || pagedir_get_page(pd, cmd_line) == NULL || !is_user_vaddr(arg_ptr)) {
        f->eax = -1;
        exit(-1);
        return;
      }
      f->eax = exec(cmd_line);
      return; }

    case SYS_WAIT: {
      arg_ptr++;
      pid_t pid = *arg_ptr;
      if (!is_user_vaddr(arg_ptr)) {
        f->eax = -1;
        exit(-1);
        return;
      }
      f->eax = wait(pid);
      return; }

    case SYS_CREATE: { 
      arg_ptr++;
      const char* file = *arg_ptr;
      arg_ptr++;
      unsigned initial_size = *arg_ptr;
      if (file == NULL || !is_user_vaddr(file + initial_size - 1) || !is_user_vaddr(file) || pagedir_get_page(pd, file) == NULL || !is_user_vaddr(arg_ptr)) {
        f->eax = -1;
        exit(-1);
        return;
      }
      f->eax = create(file, initial_size);
      return; }

    case SYS_REMOVE: {
      arg_ptr++;
      const char* file = *arg_ptr;
      if (file == NULL || !is_user_vaddr(file) || pagedir_get_page(pd, file) == NULL || !is_user_vaddr(arg_ptr)) {
        f->eax = -1;
        exit(-1);
        return;
      }
      f->eax = remove(file);
      return; }

    case SYS_OPEN: {
      arg_ptr++;
      const char* file = *arg_ptr;
      if (file == NULL || !is_user_vaddr(file) || pagedir_get_page(pd, file) == NULL || !is_user_vaddr(arg_ptr)) {
        f->eax = -1;
        exit(-1);
        return;
      }
      f->eax = open(file);
      return; }

    case SYS_FILESIZE: {
      arg_ptr++;
      if (!is_user_vaddr(arg_ptr) || *arg_ptr < 2 || *arg_ptr > 129) {
        f->eax = -1;
        exit(-1);
        return; 
      }
      f->eax = filesize(*arg_ptr);
      return; } 
      
    case SYS_READ: { 
      arg_ptr++;
      int fd = *arg_ptr;
      arg_ptr++;
      void *buffer = *arg_ptr; 
      arg_ptr++;
      unsigned size = *arg_ptr;      
      if (buffer == NULL || !is_user_vaddr(buffer + size - 1) || !is_user_vaddr(buffer) || pagedir_get_page(pd, buffer) == NULL || !is_user_vaddr(arg_ptr) || fd < 0 || fd > 129) {
        f->eax = -1;
        exit(-1);  
        return;
      }
      f->eax = read(fd, buffer, size);  
      return; }

    case SYS_WRITE: { 
      arg_ptr++;
      int fd = *arg_ptr;
      arg_ptr++;
      void *buffer = *arg_ptr; 
      arg_ptr++;
      unsigned size = *arg_ptr;      
      if (buffer == NULL || !is_user_vaddr(buffer + size - 1) || !is_user_vaddr(buffer) || pagedir_get_page(pd, buffer) == NULL || !is_user_vaddr(arg_ptr) || fd < 0 || fd > 129) {
        f->eax = -1;
        exit(-1);  
        return;
      }
      f->eax = write(fd, buffer, size);  
      return; }

    case SYS_SEEK: {
      arg_ptr++;
      int fd = *arg_ptr;
      arg_ptr++;
      unsigned position = *arg_ptr;
      if (!is_user_vaddr(arg_ptr) || fd < 2 || fd > 129) {
        f->eax = -1;
        exit(-1);
        return; 
      }
      seek(fd, position);
      return; } 

    case SYS_TELL: {
      arg_ptr++;
      if (!is_user_vaddr(arg_ptr) || *arg_ptr < 2 || *arg_ptr > 129) {
        f->eax = -1;
        exit(-1);
        return; 
      }
      f->eax = tell(*arg_ptr);
      return; } 


    case SYS_CLOSE: {
      arg_ptr++;
      if (!is_user_vaddr(arg_ptr) || *arg_ptr < 2 || *arg_ptr > 129) {
        f->eax = -1;
        exit(-1);
        return;
      }
      close(*arg_ptr);
      return; }

    default: {
      f->eax = -1;
      exit(-1);
      return; }
  }	
}

void
syscall_init (void) 
{
  intr_register_int (0x30, 3, INTR_ON, syscall_handler, "syscall");
  lock_init(&lock);
}
