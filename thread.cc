#include "macros.h"
#include "thread.h"

using namespace std;

ndb_thread::~ndb_thread()
{
}

void
ndb_thread::start()
{
  thd_ = std::move(thread(&ndb_thread::run, this));
  if (daemon_)
    thd_.detach();
}

void
ndb_thread::join()
{
  ALWAYS_ASSERT(!daemon_);
  thd_.join();
}

// can be overloaded by subclasses
void
ndb_thread::run()
{
  ALWAYS_ASSERT(body_);
  body_();
}
