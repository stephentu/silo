#pragma once

#if NDB_MASSTREE
#include "masstree_btree.h"
#else
#include "btree.h"
#include "btree_impl.h"
#endif
