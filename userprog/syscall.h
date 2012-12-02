#ifndef USERPROG_SYSCALL_H
#define USERPROG_SYSCALL_H
typedef int pid_t;

struct lock lock;
void syscall_init (void);
void exit (int);


#endif /* userprog/syscall.h */
