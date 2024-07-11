# pgkv: A key-value API for learning Postgres C

```console
$ make
$ make install
$ psql postgres --echo-all -f test.sql
DROP EXTENSION pgkv;
DROP EXTENSION
CREATE EXTENSION pgkv;
CREATE EXTENSION
SELECT pgkv.set('name.1', 'Tara');
 set
-----

(1 row)

SELECT pgkv.set('id.1', '10238');
 set
-----

(1 row)

SELECT pgkv.set('name.2', 'Minho');
 set
-----

(1 row)

SELECT pgkv.set('id.2', '299');
 set
-----

(1 row)

SELECT pgkv.list('name.');
              list
---------------------------------
 [name.1 = Tara, name.2 = Minho]
(1 row)

SELECT pgkv.list('id.');
            list
----------------------------
 [id.1 = 10238, id.2 = 299]
(1 row)

SELECT pgkv.list();
                           list
-----------------------------------------------------------
 [name.1 = Tara, id.1 = 10238, name.2 = Minho, id.2 = 299]
(1 row)

SELECT pgkv.list('name.1');
      list
-----------------
 [name.1 = Tara]
(1 row)

SELECT pgkv.get('name.1');
 get
------
 Tara
(1 row)

SELECT pgkv.del('id.1');
 del
-----

(1 row)

SELECT pgkv.del('name.1');
 del
-----

(1 row)

SELECT pgkv.list('name.');
       list
------------------
 [name.2 = Minho]
(1 row)

SELECT pgkv.list('id.');
     list
--------------
 [id.2 = 299]
(1 row)
```