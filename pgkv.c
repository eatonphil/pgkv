/*
 * This extension implements a key-value API on top of a single
 * Postgres table (pgkv.store). The key column is the PRIMARY KEY for
 * pgkv.store.
 *
 * It provides the following methods:
 *
 * - pgkv.set(key, value): Stores they key-value mapping in pgkv.store.
 * - pgkv.get(key): Returns the value for the key stored in pgkv.store.
 * - pgkv.del(key): Removes the row containing the key from pgkv.store.
 * - pgkv.list(keyPrefix): Returns a string formatting the key-value
 *   pair for all keys that start with keyPrefix within pgkv.store.
 */

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

/* Table columns and their offsets, 1-indexed. */
#define Anum_pgkv_store_key 1
#define Anum_pgkv_store_value 2

/* Return the Oid for the pgkv.store table. */
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

/*
 * Takes two arguments, a key and a value and store the key and value
 * as a row in the pkgkv.store table.
 *
 * e.g. `SELECT pgkv.set('name.1', 'Julia');` sets the key 'name.1' to
 * 'Julia'.
 */
Datum
pgkv_set(PG_FUNCTION_ARGS)
{
  Relation rel;
  TupleTableSlot *slot;

  if (PG_ARGISNULL(0) || PG_ARGISNULL(1))
    elog(ERROR, "key and value must not be NULL");

  rel = table_open(pgkv_get_store_table_oid(), RowExclusiveLock);

  slot = table_slot_create(rel, NULL);
  ExecClearTuple(slot);
  slot->tts_values[Anum_pgkv_store_key - 1] = PG_GETARG_DATUM(0);
  slot->tts_values[Anum_pgkv_store_value - 1] = PG_GETARG_DATUM(1);
  memset(slot->tts_isnull, 0, 2);
  ExecStoreVirtualTuple(slot);

  simple_table_tuple_insert(rel, slot);

  ExecDropSingleTupleTableSlot(slot);
  table_close(rel, RowExclusiveLock);

  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pgkv_get);

/*
 * Takes a single argument (a key) and scans pgkv.store's on the
 * primary key column to find the value.
 *
 * e.g. `SELECT pgkv.get('name.1');` returns the value for the key
 * 'name.1'.
 */
Datum
pgkv_get(PG_FUNCTION_ARGS)
{
  TableScanDesc scan;
  HeapTuple tup;
  TupleDesc tupDesc;
  Relation rel;
  ScanKeyData key[1];
  bool isnull;
  char *val;

  if (PG_ARGISNULL(0))
    elog(ERROR, "key must not be NULL");

  ScanKeyInit(&key[0],
	      Anum_pgkv_store_key,
	      BTEqualStrategyNumber, F_TEXTEQ,
	      PG_GETARG_DATUM(0));

  rel = table_open(pgkv_get_store_table_oid(), AccessShareLock);
  tupDesc = RelationGetDescr(rel);

  scan = heap_beginscan(rel, GetActiveSnapshot(), sizeof key / sizeof key[0], key, NULL, 0);
  tup = heap_getnext(scan, ForwardScanDirection);
  if (!HeapTupleIsValid(tup))
    elog(ERROR, "key does not exist");

  // Copy the value so we own it.
  val = TextDatumGetCString(heap_getattr(tup, Anum_pgkv_store_value, tupDesc, &isnull));

  heap_endscan(scan);
  table_close(rel, AccessShareLock);

  PG_RETURN_TEXT_P(cstring_to_text(val));
}

PG_FUNCTION_INFO_V1(pgkv_del);

/*
 * Deletes a single key-value pair from pgkv.store.
 *
 * e.g. `SELECT pgkv.del('name.1');` deletes the row where the key is
 * 'name.1'.
 */
Datum
pgkv_del(PG_FUNCTION_ARGS)
{
  TableScanDesc scan;
  HeapTuple tup;
  Relation rel;
  ScanKeyData key[1];

  if (PG_ARGISNULL(0))
    elog(ERROR, "key must not be NULL");
  
  ScanKeyInit(&key[0],
	      Anum_pgkv_store_key,
	      BTEqualStrategyNumber, F_TEXTEQ,
	      PG_GETARG_DATUM(0));

  rel = table_open(pgkv_get_store_table_oid(), RowExclusiveLock);

  scan = heap_beginscan(rel, GetActiveSnapshot(), sizeof key / sizeof key[0], key, NULL, 0);
  tup = heap_getnext(scan, ForwardScanDirection);
  if (!HeapTupleIsValid(tup))
    elog(ERROR, "key does not exist");

  simple_table_tuple_delete(rel, &tup->t_self, GetActiveSnapshot());

  heap_endscan(scan);
  table_close(rel, RowExclusiveLock);

  PG_RETURN_VOID();
}

PG_FUNCTION_INFO_V1(pgkv_list);

/*
 * Returns a formatted list (as a string) of all key-value pairs that
 * match the keyPrefix. Does a scan on `pkgv.store` using the primary
 * key column.
 *
 * e.g. `SELECT pgkv_list('name.');` returns all key-value pairs
 * where the key starts with 'name.'.
 */
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
	      Anum_pgkv_store_key,
	      BTGreaterEqualStrategyNumber, F_TEXT_GE,
	      PointerGetDatum(prefix_text));

  initStringInfo(&res);
  appendStringInfoString(&res, "[");

  rel = table_open(pgkv_get_store_table_oid(), AccessShareLock);
  tupDesc = RelationGetDescr(rel);

  scan = heap_beginscan(rel, GetActiveSnapshot(), sizeof key / sizeof key[0], key, NULL, 0);
  while (HeapTupleIsValid(tup = heap_getnext(scan, ForwardScanDirection)))
  {
    bool isnull;
    char *key, *val;

    // The attribute to get is 1-indexed.
    key = TextDatumGetCString(heap_getattr(tup, Anum_pgkv_store_key, tupDesc, &isnull));
    val = TextDatumGetCString(heap_getattr(tup, Anum_pgkv_store_value, tupDesc, &isnull));

    // Postgres heap table rows are not ordered by the PRIMARY KEY, so
    // the best we can do is just skip keys that don't meet the prefix
    // filter.
    if (strncmp(key, prefix, strlen(prefix)) > 0)
      continue;

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
