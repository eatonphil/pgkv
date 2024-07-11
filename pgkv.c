#include <string.h>

#include "postgres.h"
#include "access/table.h"
#include "access/heapam.h"
#include "catalog/indexing.h"
#include "catalog/namespace.h"
#include "fmgr.h"
#include "utils/builtins.h"
#include "utils/fmgroids.h"
#include "utils/lsyscache.h"
#include "utils/rel.h"
#include "utils/snapmgr.h"

PG_MODULE_MAGIC;

PG_FUNCTION_INFO_V1(pgkv_set);

static Oid
pgkv_get_store_table_oid()
{
  Oid namespace_oid, tablename_oid;
  namespace_oid = get_namespace_oid("pgkv", false);
  Assert(OidIsValid(namespace_oid));
  tablename_oid = get_relname_relid("store", namespace_oid);
  Assert(OidIsValid(tablename_oid));
  return tablename_oid;
}

Datum
pgkv_set(PG_FUNCTION_ARGS)
{
  TupleDesc tupDesc;
  Datum values[2] = {PG_GETARG_DATUM(0), PG_GETARG_DATUM(1)};
  /* Neither key nor value may be NULL. */
  bool nulls[2] = {false, false};
  HeapTuple tup;
  Relation rel;

  if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
    elog(ERROR, "key and value must not be NULL");

  rel = table_open(pgkv_get_store_table_oid(), RowExclusiveLock);
  tupDesc = RelationGetDescr(rel);
  tup = heap_form_tuple(tupDesc, values, nulls);
  CatalogTupleInsert(rel, tup);

  heap_freetuple(tup);
  table_close(rel, RowExclusiveLock);

  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pgkv_get);

Datum
pgkv_get(PG_FUNCTION_ARGS)
{
  /* TODO */
  PG_RETURN_TEXT_P(cstring_to_text("OK"));
}

PG_FUNCTION_INFO_V1(pgkv_del);

Datum
pgkv_del(PG_FUNCTION_ARGS)
{
  /* TODO */
  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pgkv_list);

Datum
pgkv_list(PG_FUNCTION_ARGS)
{
  TableScanDesc scan;
  HeapTuple tup;
  TupleDesc tupDesc;
  Relation rel;
  StringInfoData res;
  ScanKeyData key[1];
  char *prefix = "";
  text *prefix_text = NULL;

  if (!PG_ARGISNULL(0))
    prefix = TextDatumGetCString(PG_GETARG_DATUM(0));
  prefix_text = cstring_to_text(prefix);
  
  ScanKeyInit(&key[0],
	      1, /* 1-indexed column number. i.e. the key */
	      BTGreaterEqualStrategyNumber, F_TEXT_GE,
	      PointerGetDatum(prefix_text));

  initStringInfo(&res);
  appendStringInfoString(&res, "[");

  rel = table_open(pgkv_get_store_table_oid(), AccessShareLock);
  tupDesc = RelationGetDescr(rel);

  scan = heap_beginscan(rel, &SnapshotSelfData, 1, key, NULL, 0);
  while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
  {
    bool isnull;
    char *key, *val;

    // The attribute to get is 1-indexed.
    key = TextDatumGetCString(heap_getattr(tup, 1, tupDesc, &isnull));
    val = TextDatumGetCString(heap_getattr(tup, 2, tupDesc, &isnull));

    // We control the end of the scan.
    if (strncmp(key, prefix, strlen(prefix)) > 0)
    {
      pfree(key);
      pfree(val);
      break;
    }

    if (res.len > 1)
      appendStringInfoString(&res, ", ");
    
    appendStringInfo(&res, "%s = %s", key, val);

    pfree(key);
    pfree(val);
  }
  appendStringInfoString(&res, "]");

  if (*prefix)
    pfree(prefix);
  pfree(prefix_text);
  heap_endscan(scan);
  table_close(rel, AccessShareLock);

  PG_RETURN_TEXT_P(cstring_to_text(res.data));
}
