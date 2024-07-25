DROP EXTENSION pgkv;
CREATE EXTENSION pgkv;
SELECT pgkv.set('name.1', 'Tara');
SELECT pgkv.set('id.1', '10238');
SELECT pgkv.set('name.2', 'Minho');
SELECT pgkv.set('id.2', '299');
SELECT pgkv.list('name.');
SELECT pgkv.list('id.');
SELECT pgkv.list();
SELECT pgkv.list('name.1');
SELECT pgkv.get('name.1');
SELECT pgkv.del('id.1');
SELECT pgkv.del('name.1');
SELECT pgkv.list('name.');
SELECT pgkv.list('id.');
SELECT pgkv.set('id.2', '300');
SELECT pgkv.get('id.2');
