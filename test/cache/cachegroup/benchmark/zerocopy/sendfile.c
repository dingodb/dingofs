/*=============================================================================
 * Date:           2017-08-02
 * Author:         wine93 <wine93.info@gmail.com>
 * ComplieFlags:   -g
 * Description:    A simple echo server
 * =============================================================================*/

#include <sys/types.h>
#define _GNU_SOURCE
#include <arpa/inet.h>    /* inet_ntoa */
#include <errno.h>        /* errno */
#include <netinet/in.h>   /* sockaddr_in */
#include <signal.h>       /* signal */
#include <stdarg.h>       /* va_start va_end */
#include <stdio.h>        /* perror */
#include <stdlib.h>       /* exit */
#include <string.h>       /* memset */
#include <sys/epoll.h>    /* epoll_create epoll_ctl epoll_wait */
#include <sys/socket.h>   /* socket */
#include <unistd.h>       /* close */
#include <sys/sendfile.h> /* sendfile */
#include <fcntl.h>
#include <stddef.h>
#include <time.h>
#include <sys/mman.h>
#include <sys/stat.h>

#define dd(...)                \
  fprintf(stderr, "[DEBUG] "); \
  fprintf(stderr, __VA_ARGS__);

#define logerr(format)                                   \
  fprintf(stderr, "[ERR] (%s:%d) ", __func__, __LINE__); \
  perror(format)

#define echo_signal_helper(n) SIG##n
#define echo_signal_value(n) echo_signal_helper(n)

#define SOCKADDRLEN sizeof(struct sockaddr)

#define MAX_BACKLOG 8192
#define MAX_BUFFER 4096
#define MAX_EVENTS 1024

#define READ_EVENT 1
#define WRITE_EVENT 2

#define ECHO_BROKENPIPE_SIGNAL PIPE

typedef struct signal_s signal_t;
typedef struct event_s event_t;
typedef struct connection_s connection_t;

typedef void (*event_handler_pt)(event_t* ev);

struct signal_s {
  int signo;
  char* signame;
  void (*handler)(int signo);
};

struct event_s {
  void* data;
  int active;
  event_handler_pt handler;
};

struct connection_s {
  int fd;
  struct sockaddr* sockaddr;
  socklen_t socklen;
  const char* addr_text;
  short port;
  void* buffer;
  u_char* pos;
  u_char* last;
  event_t* rev;
  event_t* wev;
};

static void* pcalloc(size_t size);

static void init_signal();
static void signal_handler(int signo);

static connection_t* create_connection();
static int free_connection(connection_t* c);
static int check_broken_connection(connection_t* c);

static int open_listening_socket(const char* host, short port);

static int epoll_init();
static int epoll_add_connection(connection_t* c);
static int epoll_del_connection(connection_t* c);
static int epoll_add_event(event_t* ev, int event);
static int epoll_process_events();

static void echo_accept(event_t* ev);
static void echo_recv(event_t* ev);
static void echo_send(event_t* ev);

static int epfd = -1;
static struct epoll_event event_list[MAX_EVENTS + 5];

static signal_t signals[] = {
    /*{ SIGPIPE, "SIGPIPE", SIG_IGN },*/
    {SIGPIPE, "SIGPIPE", signal_handler},
    {SIGTERM, "SIGTERM", signal_handler},
    {0, NULL, NULL}};

static void* palloc(size_t size) {
  void* p;

  p = malloc(size);
  if (p == NULL) {
    return NULL;
  }

  memset(p, 0, size);

  return p;
}

static connection_t* create_connection() {
  connection_t* c;

  c = (connection_t*)palloc(sizeof(connection_t));
  if (c == NULL) {
    return NULL;
  }

  c->rev = (event_t*)palloc(sizeof(event_t));
  if (c->rev == NULL) {
    return NULL;
  }

  c->wev = (event_t*)palloc(sizeof(event_t));
  if (c->wev == NULL) {
    return NULL;
  }

  return c;
}

static int free_connection(connection_t* c) {
  int rc;

  epoll_del_connection(c);

  rc = close(c->fd);
  if (rc == -1) {
    logerr("close() failed");
    return -1;
  }

  return 0;
}

static int check_broken_connection(connection_t* c) {
  int rc;
  char buf[1];

  rc = recv(c->fd, buf, 1, MSG_PEEK);
  if (rc == 0 || (rc == -1 && errno != EAGAIN)) {
    return 0;
  }

  return 1;
}

static int open_listening_socket(const char* host, short port) {
  int s, rc, opt;
  connection_t* lc;
  struct sockaddr_in* sa;

  s = socket(AF_INET, SOCK_STREAM | SOCK_NONBLOCK, 0);
  if (s == -1) {
    logerr("socket() failed");
    return -1;
  }

  opt = 1;
  rc = setsockopt(s, SOL_SOCKET, SO_REUSEADDR, &opt, sizeof(int));
  if (rc == -1) {
    logerr("setsockopt() failed");
    return -1;
  }

  opt = 1;
  rc = setsockopt(s, SOL_SOCKET, SO_KEEPALIVE, &opt, sizeof(int));
  if (rc == -1) {
    logerr("setsockopt() failed");
    return -1;
  }

  sa = (struct sockaddr_in*)palloc(sizeof(struct sockaddr_in));
  sa->sin_family = AF_INET;
  inet_pton(AF_INET, host, &(sa->sin_addr));
  sa->sin_port = htons(port);
  rc = bind(s, (struct sockaddr*)sa, sizeof(struct sockaddr));
  if (rc == -1) {
    logerr("bind() failed");
    return -1;
  }

  rc = listen(s, MAX_BACKLOG);
  if (rc == -1) {
    logerr("listen() failed");
    return -1;
  }

  lc = create_connection();
  if (lc == NULL) {
    return -1;
  }

  lc->fd = s;
  lc->sockaddr = (struct sockaddr*)sa;
  lc->socklen = SOCKADDRLEN;
  lc->addr_text = host;
  lc->port = port;

  lc->rev->data = (void*)lc;
  lc->rev->handler = echo_accept;
  epoll_add_event(lc->rev, READ_EVENT);

  return 0;
}

static int epoll_init() {
  epfd = epoll_create(1);
  if (epfd == -1) {
    logerr("epoll_create() failed");
  }

  return epfd;
}

static int epoll_add_connection(connection_t* c) {
  int rc;
  struct epoll_event ee;

  ee.events = EPOLLET | EPOLLIN | EPOLLOUT | EPOLLRDHUP;
  ee.data.ptr = (void*)c;

  rc = epoll_ctl(epfd, EPOLL_CTL_ADD, c->fd, &ee);
  if (rc == -1) {
    logerr("epoll_ctl() failed");
    return -1;
  }

  c->rev->active = 1;
  c->wev->active = 1;

  return rc;
}

static int epoll_del_connection(connection_t* c) {
  int rc;
  struct epoll_event ee;

  ee.events = 0;
  ee.data.ptr = NULL;

  rc = epoll_ctl(epfd, EPOLL_CTL_MOD, c->fd, &ee);
  if (rc == -1) {
    logerr("epoll_ctl() failed");
    return -1;
  }

  c->rev->active = 0;
  c->wev->active = 0;

  return rc;
}

static int epoll_add_event(event_t* ev, int event) {
  int rc;
  connection_t* c;
  struct epoll_event ee;

  c = (connection_t*)ev->data;

  ee.data.ptr = (void*)c;

  if (event & READ_EVENT) {
    ee.events = EPOLLIN | EPOLLET;
  } else if (event & WRITE_EVENT) {
    ee.events = EPOLLOUT | EPOLLET;
  }

  rc = epoll_ctl(epfd, EPOLL_CTL_ADD, c->fd, &ee);
  if (rc == -1) {
    logerr("epoll_ctl() failed");
    return -1;
  }

  ev->active = 1;

  return rc;
}

static int epoll_process_events() {
  int i, events, revents;
  connection_t* c;
  event_t *rev, *wev;
  struct epoll_event ee;

  events = epoll_wait(epfd, event_list, MAX_EVENTS, -1);
  if (events == -1) {
    logerr("epoll_wait() failed");
    return -1;
  }

  for (i = 0; i < events; i++) {
    c = (connection_t*)event_list[i].data.ptr;

    revents = event_list[i].events;

    rev = c->rev;
    if ((revents & EPOLLIN) && rev->active) {
      rev->handler(rev);
    }

    wev = c->wev;
    if ((revents & EPOLLOUT) && wev->active) {
      wev->handler(wev);
    }
  }

  return 0;
}

static void echo_accept(event_t* ev) {
  int s;
  struct sockaddr_in sa;
  socklen_t socklen;
  connection_t *lc, *c;

  lc = (connection_t*)ev->data;

  socklen = SOCKADDRLEN;
  s = accept4(lc->fd, (struct sockaddr*)&sa, &socklen, SOCK_NONBLOCK);

  if (s == -1) {
    logerr("accept4() failed");
    return;
  }

  c = create_connection();
  if (c == NULL) {
    return;
  }

  c->fd = s;
  c->sockaddr = (struct sockaddr*)palloc(sizeof(struct sockaddr));
  memcpy((void*)c->sockaddr, (void*)&sa, SOCKADDRLEN);
  c->socklen = socklen;
  c->buffer = malloc(MAX_BUFFER + 5);
  c->pos = (u_char*)c->buffer;
  c->last = (u_char*)c->buffer;
  c->addr_text = inet_ntoa(sa.sin_addr);
  c->port = ntohs(sa.sin_port);

  c->rev->data = (void*)c;
  c->rev->handler = echo_recv;

  c->wev->data = (void*)c;
  c->wev->handler = echo_send;

  epoll_add_connection(c);

  dd("%s:%hu -> %s:%hu\n", c->addr_text, c->port, lc->addr_text, lc->port);
}

static void echo_recv(event_t* ev) {
  size_t n, len;
  connection_t* c;

  c = (connection_t*)ev->data;

  for (;;) {
    if (c->last == (u_char*)c->buffer + MAX_BUFFER) {
      echo_send(c->wev);
    }

    len = MAX_BUFFER - (c->last - (u_char*)c->buffer);
    if (len == 0) {
      break;
    }

    n = recv(c->fd, c->last, len, 0);

    /*dd("recv %d\n", n);*/

    if (n == (size_t)-1) {
      if (errno != EAGAIN) {
        logerr("recv() failed");
      }
      break;
    } else if (n == 0) {
      echo_send(c->wev);
      free_connection(c);
      dd("client prematurely closed connection\n");
      break;
    }

    c->last += n;
  }
}

static int64_t start = 0;
static int64_t stop = 0;

static int64_t cpuwide_time_ns() {
    struct timespec now;
    clock_gettime(CLOCK_MONOTONIC, &now);
    return now.tv_sec * 1000000000L + now.tv_nsec;
}

static void start_timer() {
    start = cpuwide_time_ns();
}

static void stop_timer() {
    stop = cpuwide_time_ns();
}

static int64_t u_elapsed() {
    return (stop - start) / 1000;
}

static void normal_file(int out_fd) {
    int flags = O_RDONLY ;
    int in_fd = open("f1", flags);
    if (in_fd <= 0) {
        logerr("open() failed");
    }

    char* data = malloc(4 * 1024 * 1024);

    off_t pos = 0;
    off_t last = 4 * 1024 * 1024;
    while (pos < last) {

        size_t count = 128 * 1024;
        ssize_t n = read(in_fd, data, count);
        if (n == (ssize_t)-1) {
          logerr("read() failed");
          break;
        }

        n = send(out_fd, data, count, 0);
        if (n == (size_t)-1) {
          logerr("send() failed");
          if (errno != EAGAIN) {
            //free_connection(c);
          }
          break;
        }

        pos += n;
        dd("send(%d): %d %d\n", count, n, pos);
    }
}

static void mmap_file(int out_fd) {
    int flags = O_RDONLY ;
    int in_fd = open("f1", flags);
    if (in_fd <= 0) {
        logerr("open() failed");
    }

    char* data = (char*)mmap(NULL, 4 * 1024 * 1024, PROT_READ, MAP_PRIVATE, in_fd, 0);

    off_t pos = 0;
    off_t last = 4 * 1024 * 1024;
    while (pos < last) {
        size_t count = last - pos;
        ssize_t n = send(out_fd, data, count, 0);
        if (n == (size_t)-1) {
          logerr("send() failed");
          if (errno != EAGAIN) {
            //free_connection(c);
          }
          break;
        }

        pos += n;
        dd("send(%d): %d %d\n", count, n, pos);
    }
}

static void send_file(int out_fd) {
    int flags = O_RDONLY ;
    int in_fd = open("f1", flags);
    if (in_fd <= 0) {
        logerr("open() failed");
    }

    off_t pos = 0;
    off_t last = 4 * 1024 * 1024;
    while (pos < last) {
        size_t count = last - pos;
        ssize_t n = sendfile(out_fd, in_fd, &pos, count);
        if (n == (size_t)-1) {
          logerr("send() failed");
          if (errno != EAGAIN) {
            //free_connection(c);
          }
          break;
        }

        dd("sendfile(%d): %d %d\n", count, n, pos);
    }
}

static void echo_send(event_t* ev) {
  ssize_t n;
  connection_t* c;

  c = (connection_t*)ev->data;




  if (c->pos < c->last) {
    start_timer();
    normal_file(c->fd);
    //mmap_file(c->fd);
    //send_file(c->fd);
    //dd("send_file(): %.6lf\n", u_elapsed() * 1.0 / 1e6);
    stop_timer();
    dd("normal_file(): %.6lf\n", u_elapsed() * 1.0 / 1e6);

    /*sleep(3);*/
    c->pos = c->last;
  }

  if (c->pos == c->last) {
    c->pos = (u_char*)c->buffer;
    c->last = (u_char*)c->buffer;
  }
}

static void init_signal() {
  int rc;
  signal_t* sig;
  struct sigaction sa;

  for (sig = signals; sig->signo != 0; sig++) {
    memset(&sa, 0, sizeof(struct sigaction));
    sa.sa_handler = signal_handler;
    sigemptyset(&sa.sa_mask);
    // sigaddset(&sa.sa_mask, sig->signo);
    rc = sigaction(sig->signo, &sa, NULL);
    if (rc == -1) {
      logerr("sigaction() failed");
    }
  }
}

static void signal_handler(int signo) {
  switch (signo) {
    case SIGPIPE:
      dd("RECV SIGPIPE\n");
      break;

    case SIGTERM:
      dd("RECV SIGTERM\n");
      break;

    default:
      dd("RECV UNKNOWN_SIGNAL\n");
      break;
  }
}

int main() {
  int rc;

  /*signal(SIGPIPE, signal_handler);*/
  /*signal(SIGTERM, signal_handler);*/
  init_signal();

  epoll_init();

  rc = open_listening_socket("127.0.0.1", 8000);
  if (rc == -1) {
    return 1;
  }

  rc = open_listening_socket("127.0.0.1", 8001);
  if (rc == -1) {
    return 1;
  }

  for (;;) {
    epoll_process_events();
  }

  return 0;
}

/*
 * -- TEST CASE 1: BROKEN PIPE
 * - hack
 *   - echo_send: sleep(3)
 * - request
 *   - telnet 127.0.0.1 8000
 *       - 012345678901234567890
 *       - quit
 * - request
 *   - echo '0000000000111111111122222222223' | nc 127.0.0.1 8000


 * -- TEST CASE 2: WIPE OUT CLOSE_WAIT
 * - request
 *   - echo '012345678' | nc 127.0.0.1 8000


 * -- TEST CASE 3: RECV LENGTH
 * - request
 *   - echo '0123456789' | nc 127.0.0.1 8000


 * -- TEST CASE 4: BIG FILE
 * - request
 *   - seq 1 100000000 > bf
 *   - cat bf | nc 127.0.0.1 8000 > bf2
 *   - md5(bf) == md5(bf2)
 */