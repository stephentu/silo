#include "macros.h"
#include "thread.h"

#include <iostream>

using namespace std;
using namespace util;

ndb_thread::~ndb_thread()
{
}

void
ndb_thread::start()
{
  pthread_attr_t attr;
  ALWAYS_ASSERT(pthread_attr_init(&attr) == 0);
  if (daemon)
    ALWAYS_ASSERT(pthread_attr_setdetachstate(&attr, PTHREAD_CREATE_DETACHED) == 0);
  ALWAYS_ASSERT(pthread_create(&p, &attr, pthread_bootstrap, (void *) this) == 0);
  ALWAYS_ASSERT(pthread_attr_destroy(&attr) == 0);
}

void
ndb_thread::join()
{
  ALWAYS_ASSERT(!daemon);
  ALWAYS_ASSERT(pthread_join(p, NULL) == 0);
}

void
ndb_thread::run()
{
  ALWAYS_ASSERT(body);
  body();
}

bool
ndb_thread::register_completion_callback(callback_t callback)
{
  completion_callbacks().push_back(callback);
  return true;
}

vector<ndb_thread::callback_t> &
ndb_thread::completion_callbacks()
{
  static vector<callback_t> *callbacks = NULL;
  if (!callbacks)
    callbacks = new vector<callback_t>;
  return *callbacks;
}

void
ndb_thread::on_complete()
{
  for (vector<callback_t>::iterator it = completion_callbacks().begin();
       it != completion_callbacks().end(); ++it)
    (*it)(this);
}

void *
ndb_thread::pthread_bootstrap(void *p)
{
  ndb_thread *self = static_cast<ndb_thread *>(p);
  try {
    self->run();
  } catch (...) {
    cerr << "[Thread " << self->p << " (" << self->name << ")] - "
         << "terminating due to uncaught exception" << endl;
    self->on_complete();
    throw;
  }
  self->on_complete();
  return NULL;
}
