/*
 * Canonical location:
 *   git+ssh://amsterdam.csail.mit.edu/home/am1/prof/proftools.git
 *   under spmc/lib/scopedperf.hh
 *
 * Modified by stephentu to disable for non C++11 builds
 */

#ifndef _SCOPED_PERF_H_
#define _SCOPED_PERF_H_

#ifdef USE_PERF_CTRS

#if !defined(XV6)
#include <iostream>
#include <iomanip>
#include <sstream>
#include <assert.h>
#include <string.h>
#include <stdint.h>
#include <sys/time.h>
#endif

namespace scopedperf {

#if defined(XV6)
typedef u32 uint;
typedef u64 uint64_t;
#endif

/*
 * statically enable/disable most of the generated code for profiling.
 */
class default_enabler {
 public:
  bool enabled() const { return true; }
};

class always_enabled {
 public:
  bool enabled() const { return true; }
};

class always_disabled {
 public:
  bool enabled() const { return false; }
};

/*
 * get CPU id function type
 */
typedef int(*getcpu_fn)(void);

/*
 * spinlock: mostly to avoid pthread mutex sleeping.
 */
#if !defined(XV6_KERNEL)
class spinlock {
 public:
  spinlock() : x(0) {}

  void acquire() {
    while (!__sync_bool_compare_and_swap(&x, 0, 1))
      ;
  }

  void release() {
    x = 0;
  }

 private:
  volatile uint x;
};
#endif

#if defined(XV6_KERNEL)
using ::spinlock;

static inline int sched_getcpu() {
  return mycpu()->id;
}
#endif

class scoped_spinlock {
 public:
  scoped_spinlock(spinlock *larg) : l(larg) {
    l->acquire();
    held = true;
  }

  void release() {
    if (held)
      l->release();
    held = false;
  }

  ~scoped_spinlock() { release(); }

 private:
  spinlock *const l;
  bool held;
};


/*
 * vector & pair: for portability.
 */
template<class A, class B>
struct pair {
  A first;
  B second;
};

template<class A, class B>
pair<A, B>
make_pair(const A &a, const B &b)
{
  pair<A, B> p;
  p.first = a;
  p.second = b;
  return p;
}

template<class T>
struct vector {
  T _buf[128];
  uint _cnt;

  vector() : _cnt(0) {}
  void insert_front(T e) {
    assert(_cnt < sizeof(_buf) / sizeof(T));
    memmove(&_buf[1], &_buf[0], _cnt * sizeof(T));
    _buf[0] = e;
    _cnt++;
  }
  void push_back(T e) {
    assert(_cnt < sizeof(_buf) / sizeof(T));
    _buf[_cnt] = e;
    _cnt++;
  }
};

template<class T>
struct viter {
  const vector<T> *_v;
  int _pos;

  viter(const vector<T> *v, int pos) : _v(v), _pos(pos) {}
  bool operator!=(const viter &other) const { return _pos != other._pos; }
  void operator++() { _pos++; }
  T operator*() { return _v->_buf[_pos]; }
};

template<class T>
viter<T>
begin(const vector<T> &v)
{
  return viter<T>(&v, 0);
}

template<class T>
viter<T>
end(const vector<T> &v)
{
  return viter<T>(&v, v._cnt);
}


/*
 * fast log-base-2, for histograms.
 */
static const uint8_t log2table[256] = {
#define R2(x)   x,      x
#define R4(x)   R2(x),  R2(x)
#define R8(x)   R4(x),  R4(x)
#define R16(x)  R8(x),  R8(x)
#define R32(x)  R16(x), R16(x)
#define R64(x)  R32(x), R32(x)
#define R128(x) R64(x), R64(x)
  0, 1, R2(2), R4(3), R8(4), R16(5), R32(6), R64(7), R128(8)
#undef R2
#undef R4
#undef R8
#undef R16
#undef R32
#undef R64
#undef R128
};

template<class T, int Nbits>
inline uintptr_t
log2r(T v)
{
  if (Nbits == 8) {
    return log2table[v];
  } else {
    T hi = v >> (Nbits/2);
    if (hi)
      return Nbits/2 + log2r<T, Nbits/2>(hi);
    else
      return log2r<T, Nbits/2>(v);
  }
}

template<class T>
uintptr_t
log2(T v)
{
  return log2r<T, sizeof(T)*8>(v);
}


/*
 * ctrgroup: a group of performance counters.
 */

template<typename... Counters>
class ctrgroup_chain;

template<>
class ctrgroup_chain<> {
 public:
  ctrgroup_chain() {}
  static const uint cg_nctr = 0;
  void cg_get_samples(uint64_t *v) const {}
  void cg_get_delta(uint64_t *delta, uint64_t *prev) const {}
  vector<const char*> get_names() const { return {}; }
};

template<typename One, typename... Others>
class ctrgroup_chain<One, Others...> : ctrgroup_chain<Others...> {
 public:
  ctrgroup_chain(One *x, Others*... y)
    : ctrgroup_chain<Others...>(y...), ctr(x)
  {
    x->setup();
  }

  static const uint cg_nctr = 1 + ctrgroup_chain<Others...>::cg_nctr;

  void cg_get_samples(uint64_t *v) const {
    v[0] = ctr->sample();
    ctrgroup_chain<Others...>::cg_get_samples(v+1);
  }

  void cg_get_delta(uint64_t *delta, uint64_t *prev) const {
    uint64_t x = ctr->sample();
    *delta = (x - *prev) & ctr->mask;
    *prev = x;
    ctrgroup_chain<Others...>::cg_get_delta(delta+1, prev+1);
  }

  vector<const char*> get_names() const {
    vector<const char*> v = ctrgroup_chain<Others...>::get_names();
    v.insert_front(ctr->name);
    return v;
  }

 private:
  const One *const ctr;
};

template<typename... Counters>
ctrgroup_chain<Counters...>
ctrgroup(Counters*... args)
{
  return ctrgroup_chain<Counters...>(args...);
}


/*
 * perfsum: aggregating counter deltas across multiple CPUs.
 */
class perfsum_base {
 public:
  enum display_opt { show, hide };

  perfsum_base(const char *n, display_opt d) : name(n), disp(d) {
    scoped_spinlock x(get_sums_lock());
    get_sums()->push_back(this);
  }

  static void printall(int w0 = 17, int w = 13) {
    scoped_spinlock x(get_sums_lock());
    auto sums = get_sums();
    for (perfsum_base *ps: *sums)
      if (ps->disp == show)
        ps->print(w0, w);
  }

  static void resetall() {
    scoped_spinlock x(get_sums_lock());
    for (perfsum_base *ps: *get_sums())
      ps->reset();
  }

  virtual void print(int w0, int w) const = 0;
  virtual void reset() = 0;

 protected:
  template<class Row, class Callback>
  static void print_row(const char *rowname, const Row &r,
			int w0, int w, Callback f)
  {
    std::cout << std::left << std::setw(w0) << rowname;
    for (const auto &elem: r)
      std::cout << std::left << std::setw(w) << f(elem) << " ";
    std::cout << std::endl;
  }

  const char *name;
  const display_opt disp;

 private:
  static vector<perfsum_base*> *get_sums() {
    static vector<perfsum_base*> v;
    return &v;
  }

  static spinlock *get_sums_lock() {
    static spinlock l;
    return &l;
  }
};

static inline void
compiler_barrier()
{
  /* Avoid compile-time reordering across performance counter reads */
  __asm __volatile("" ::: "memory");
}

template<typename Enabler, typename... Counters>
class perfsum_tmpl : public perfsum_base, public Enabler {
 public:
  perfsum_tmpl(const ctrgroup_chain<Counters...> *c,
               const char *n, perfsum_base::display_opt d)
    : perfsum_base(n, d), cg(c)
  {
  }

  static const uint ps_nctr = ctrgroup_chain<Counters...>::cg_nctr;

 protected:
  const struct ctrgroup_chain<Counters...> *const cg;
  enum { maxcpu = 256 };

  template<class Stats, class T>
  static uint64_t addcpus(const Stats stat[], T f) {
    uint64_t tot = 0;
    for (uint i = 0; i < maxcpu; i++)
      tot += f(&stat[i]);
    return tot;
  }
};

/*
 * perfsum_ctr: aggregate counts of performance events.
 */
template<typename Enabler, typename... Counters>
class perfsum_ctr : public perfsum_tmpl<Enabler, Counters...> {
 public:
  perfsum_ctr(const ctrgroup_chain<Counters...> *c,
	      const char *n, perfsum_base::display_opt d)
    : perfsum_tmpl<Enabler, Counters...>(c, n, d), base(0)
  {
    reset();
  }

  perfsum_ctr(const char *n,
	      const perfsum_ctr<Enabler, Counters...> *basesum,
              perfsum_base::display_opt d)
    : perfsum_tmpl<Enabler, Counters...>(basesum->cg, n, d), base(basesum)
  {
    reset();
  }

  void get_samples(uint64_t *s) const {
    compiler_barrier();
    perfsum_tmpl<Enabler, Counters...>::cg->cg_get_samples(s);
    compiler_barrier();
  }

  void record(uint cpuid, uint64_t *s) {
    uint64_t delta[perfsum_tmpl<Enabler, Counters...>::ps_nctr];

    compiler_barrier();
    perfsum_tmpl<Enabler, Counters...>::cg->cg_get_delta(delta, s);
    compiler_barrier();

    for (uint i = 0; i < perfsum_tmpl<Enabler, Counters...>::ps_nctr; i++)
      stat[cpuid].sum[i] += delta[i];
    stat[cpuid].count++;
  }

  void print(int w0, int w) const /* override */ {
    if (!Enabler::enabled())
      return;

    auto &cg = perfsum_tmpl<Enabler, Counters...>::cg;
    vector<pair<uint64_t, uint64_t> > p;
    for (uint i = 0; i < cg->cg_nctr; i++) {
      uint64_t b =
	base ? this->addcpus(base->stat, [&](const stats *s) { return s->sum[i]; })
	     : this->addcpus(stat,       [&](const stats *s) { return s->count; });
      p.push_back(make_pair(b,
	this->addcpus(stat, [i](const stats *s) { return s->sum[i]; })));
    }

    this->print_row(perfsum_base::name, cg->get_names(), w0, w, [](const char *name)
	      { return name; });
    this->print_row("  avg",   p, w0, w, [](const pair<uint64_t, uint64_t> &e)
#if !defined(XV6)
	      { return ((double) e.second) / (double) e.first; }
#else
	      { return e.second / e.first; }
#endif
              );
    this->print_row("  total", p, w0, w, [](const pair<uint64_t, uint64_t> &e)
	      { return e.second; });
    this->print_row("  count", p, w0, w, [](const pair<uint64_t, uint64_t> &e)
	      { return e.first; });
  }

  void reset() /* override */ {
    memset(stat, 0, sizeof(stat));
  }

 private:
  struct stats {
    uint64_t count;
    uint64_t sum[perfsum_tmpl<Enabler, Counters...>::ps_nctr];
  } __attribute__((aligned (64)));

  struct stats stat[perfsum_tmpl<Enabler, Counters...>::maxcpu];
  const struct perfsum_ctr<Enabler, Counters...> *const base;
};

template<typename Enabler, typename... Counters>
class perfsum_ctr_inlinegroup :
  public ctrgroup_chain<Counters...>,
  public perfsum_ctr<Enabler, Counters...>
{
 public:
  perfsum_ctr_inlinegroup(const char *n, perfsum_base::display_opt d,
                          Counters*... ctrs)
    : ctrgroup_chain<Counters...>(ctrs...),
      perfsum_ctr<Enabler, Counters...>(this, n, d) {}
};

template<typename Enabler = default_enabler, typename... Counters>
perfsum_ctr<Enabler, Counters...>
perfsum(const char *name, const ctrgroup_chain<Counters...> *c,
	const perfsum_base::display_opt d = perfsum_base::show)
{
  return perfsum_ctr<Enabler, Counters...>(c, name, d);
}

template<typename Enabler = default_enabler, typename... Counters>
perfsum_ctr_inlinegroup<Enabler, Counters...>
perfsum_group(const char *name, Counters*... c)
{
  return perfsum_ctr_inlinegroup<Enabler, Counters...>(name, perfsum_base::show, c...);
}

template<typename Enabler, typename... Counters>
perfsum_ctr<Enabler, Counters...>
perfsum_frac(const char *name,
	     const perfsum_ctr<Enabler, Counters...> *base)
{
  return perfsum_ctr<Enabler, Counters...>(name, base, perfsum_base::show);
}

/*
 * perfsum_hist: histogram-based aggregates.
 */
template<typename Enabler, typename... Counters>
class perfsum_hist_tmpl : public perfsum_tmpl<Enabler, Counters...> {
 public:
  perfsum_hist_tmpl(const ctrgroup_chain<Counters...> *c,
	            const char *n, perfsum_base::display_opt d)
    : perfsum_tmpl<Enabler, Counters...>(c, n, d)
  {
    reset();
  }

  void get_samples(uint64_t *s) const {
    compiler_barrier();
    perfsum_tmpl<Enabler, Counters...>::cg->cg_get_samples(s);
    compiler_barrier();
  }

  void record(uint cpuid, uint64_t *s) {
    uint64_t delta[perfsum_tmpl<Enabler, Counters...>::ps_nctr];

    compiler_barrier();
    perfsum_tmpl<Enabler, Counters...>::cg->cg_get_delta(delta, s);
    compiler_barrier();

    for (uint i = 0; i < perfsum_tmpl<Enabler, Counters...>::ps_nctr; i++)
      stat[cpuid].hist[i].count[log2(delta[i])]++;
  }

  void print(int w0, int w) const /* override */ {
    if (!Enabler::enabled())
      return;

    uint first = nbuckets, last = 0;

    auto &cg = perfsum_tmpl<Enabler, Counters...>::cg;
    vector<buckets> p;
    for (uint i = 0; i < cg->cg_nctr; i++) {
      buckets v;
      for (uint j = 0; j < nbuckets; j++) {
        v.count[j] = this->addcpus(stat, [&](const stats *s) { return s->hist[i].count[j]; });
        if (v.count[j]) {
          if (j < first) first = j;
          if (j > last)  last = j;
        }
      }
      p.push_back(v);
    }

    this->print_row(perfsum_base::name, cg->get_names(), w0, w, [](const char *name)
	      { return name; });
    for (uint i = first; i <= last; i++) {
      char n[64];
      snprintf(n, sizeof(n), "  < 2^%d", i);
      this->print_row(n, p, w0, w, [&](const buckets &b) { return b.count[i]; });
    }
    this->print_row("  total", p, w0, w, [](const buckets &b)
                    { uint64_t s = 0; for (auto x: b.count) s += x; return s; });
  }

  void reset() /* override */ {
    memset(stat, 0, sizeof(stat));
  }

 private:
  enum { nbuckets = sizeof(uint64_t)*8 + 1 };

  struct buckets {
    uint64_t count[nbuckets];
  };

  struct stats {
    struct buckets hist[perfsum_tmpl<Enabler, Counters...>::ps_nctr];
  } __attribute__((aligned (64)));

  struct stats stat[perfsum_tmpl<Enabler, Counters...>::maxcpu];
};

template<typename Enabler = default_enabler, typename... Counters>
perfsum_hist_tmpl<Enabler, Counters...>
perfsum_hist(const char *name, const ctrgroup_chain<Counters...> *c,
	     const perfsum_base::display_opt d = perfsum_base::show)
{
  return perfsum_hist_tmpl<Enabler, Counters...>(c, name, d);
}


/*
 * namedctr &c: actual counter implementations.
 */
template<uint64_t CounterWidth>
class namedctr {
 public:
  namedctr(const char *n) : name(n) {}
  void setup() {}
  const char *name;
  static const uint64_t mask =
    ((1ULL << (CounterWidth - 1)) - 1) << 1 | 1;
};

class tsc_ctr : public namedctr<64> {
 public:
  tsc_ctr() : namedctr("tsc") {}
  static uint64_t sample() {
    uint64_t a, d;
    __asm __volatile("rdtsc" : "=a" (a), "=d" (d));
    return a | (d << 32);
  }
};

class tscp_ctr : public namedctr<64> {
 public:
  tscp_ctr() : namedctr("tscp") {}
  static uint64_t sample() {
    uint64_t a, d, c;
    __asm __volatile("rdtscp" : "=a" (a), "=d" (d), "=c" (c));
    return a | (d << 32);
  }
};

template<uint64_t CounterWidth>
class pmc_ctr : public namedctr<CounterWidth> {
 public:
  pmc_ctr(int n) : namedctr<CounterWidth>(mkname(n)), cn(n) {}
  pmc_ctr(const char *nm) : namedctr<CounterWidth>(nm), cn(-1) {}

  uint64_t sample() const {
    uint64_t a, d;
    __asm __volatile("rdpmc" : "=a" (a), "=d" (d) : "c" (cn));
    return a | (d << 32);
  }

  int cn;

 private:
  static const char* mkname(int n) {
    char *buf = new char[32];
    snprintf(buf, 32, "pmc%d", n);
    return buf;
  }
};

template<uint64_t CounterWidth = 64>
class pmc_setup : public pmc_ctr<CounterWidth> {
 public:
  pmc_setup(uint64_t v, const char *nm)
    : pmc_ctr<CounterWidth>(nm), pmc_v(v) {}

  void setup() {
    if (pmc_ctr<CounterWidth>::cn >= 0)
      return;

    /*
     * XXX detect how many counters the hardware has
     */
    static bool pmcuse[4];
    static spinlock pmcuselock;

    int n = 0;
    scoped_spinlock x(&pmcuselock);
    while (n < 4 && pmcuse[n])
      n++;
    assert(n < 4);
    pmcuse[n] = true;
    x.release();

#if !defined(XV6)
    // ugly but effective
    std::stringstream ss;
    ss << "for f in /sys/kernel/spmc/cpu*/" << n << "; do "
       << "echo " << std::hex << pmc_v << " > $f; done";
    assert(0 == system(ss.str().c_str()));
#endif

    pmc_ctr<CounterWidth>::cn = n;
  }

 private:
  uint64_t pmc_v;
};

#if !defined(XV6)
class tod_ctr : public namedctr<64> {
 public:
  tod_ctr() : namedctr("tod-usec") {}
  uint64_t sample() const {
    struct timeval tv;
    gettimeofday(&tv, 0);
    return ((uint64_t) tv.tv_usec) + ((uint64_t) tv.tv_sec) * 1000000;
  }
};
#endif

class zero_ctr : public namedctr<64> {
 public:
  zero_ctr() : namedctr("zero") {}
  uint64_t sample() const { return 0; }
};


/*
 * scoped performance-counting regions, which record samples into a perfsum.
 */
template<typename Perfsum>
class base_perf_region {
 public:
  base_perf_region(Perfsum *psarg, getcpu_fn getcpu)
    : ps(psarg), enabled(ps->enabled()), cpuid(enabled ? getcpu() : 0)
  {
    if (enabled)
      ps->get_samples(s);
  }

  // invoke lap multiple times to precisely measure iterations
  // (use same measurement for end of one & start of next round)
  void lap() {
    if (enabled)
      ps->record(cpuid, s);
  }

 private:
  Perfsum *const ps;
  const bool enabled;
  const uint cpuid;
  uint64_t s[Perfsum::ps_nctr];
};

template<typename Perfsum>
class scoped_perf_region : public base_perf_region<Perfsum> {
 public:
  scoped_perf_region(Perfsum *psarg, getcpu_fn getcpu)
    : base_perf_region<Perfsum>(psarg, getcpu) {}
  ~scoped_perf_region() { base_perf_region<Perfsum>::lap(); }
};

template<typename Perfsum>
class killable_perf_region : public base_perf_region<Perfsum> {
 public:
  killable_perf_region(Perfsum *psarg, getcpu_fn getcpu)
    : base_perf_region<Perfsum>(psarg, getcpu), active(true) {}
  ~killable_perf_region() { stop(); }

  // perform a final measurement, if needed before destructor
  void stop() {
    if (active)
      base_perf_region<Perfsum>::lap();
    active = false;
  }

  // prevent destructor from performing a measurement
  void kill() { active = false; }

 private:
  bool active;
};

template<typename Perfsum>
scoped_perf_region<Perfsum>
perf_region(Perfsum *ps, getcpu_fn getcpu = sched_getcpu)
{
  return scoped_perf_region<Perfsum>(ps, getcpu);
}

template<typename Perfsum>
killable_perf_region<Perfsum>
killable_region(Perfsum *ps, getcpu_fn getcpu = sched_getcpu)
{
  return killable_perf_region<Perfsum>(ps, getcpu);
}


/*
 * macros for the common case of putting in a scoped perf-counting region.
 */
#define __PERF_CONCAT2(a, b)  a ## b
#define __PERF_CONCAT(a, b)   __PERF_CONCAT2(a, b)
#define __PERF_ANON	      __PERF_CONCAT(__anon_id_, __COUNTER__)

#define __PERF_REGION(region_var, sum_var, region_type, text, group)	       \
  static auto __PERF_CONCAT(sum_var, _sum) = scopedperf::perfsum(text, group); \
  auto region_var = region_type(&__PERF_CONCAT(sum_var, _sum));

#define ANON_REGION(text, group) \
  __PERF_REGION(__PERF_ANON, __PERF_ANON, scopedperf::perf_region, text, group)
#define PERF_REGION(var, text, group) \
  __PERF_REGION(var, __PERF_ANON, scopedperf::perf_region, text, group)
#define KILLABLE_REGION(var, text, group) \
  __PERF_REGION(var, __PERF_ANON, scopedperf::killable_region, text, group)

#define STATIC_COUNTER_DECL(ctrtype, ctrname, groupname) \
  static ctrtype ctrname; \
  static ::scopedperf::ctrgroup_chain< ctrtype > groupname(&ctrname);
#define PERF_EXPR(expr) expr
#define PERF_DECL(decl) decl

#define CLASS_STATIC_COUNTER_DECL(ctrtype, ctrname, groupname) \
  static ctrtype ctrname; \
  static ::scopedperf::ctrgroup_chain< ctrtype > groupname;
#define CLASS_STATIC_COUNTER_IMPL(clsname, ctrtype, ctrname, groupname) \
  ctrtype clsname::ctrname; \
  ::scopedperf::ctrgroup_chain< ctrtype > clsname::groupname(&ctrname) ;

} /* namespace scopedperf */

#else /* !USE_PERF_CTRS */

#define ANON_REGION(text, group) ((void)0)
#define PERF_REGION(var, text, group) ((void)0)
#define KILLABLE_REGION(var, text, group) ((void)0)

#define STATIC_COUNTER_DECL(ctrtype, ctrname, groupname)
#define PERF_EXPR(expr) ((void)0)
#define PERF_DECL(decl)

#define CLASS_STATIC_COUNTER_DECL(ctrtype, ctrname, groupname)
#define CLASS_STATIC_COUNTER_IMPL(clsname, ctrtype, ctrname, groupname)

#endif /* USE_PERF_CTRS */

#endif /* _SCOPED_PERF_H_ */
