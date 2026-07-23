\set iid random(100001, 200000)
DELETE FROM bmsql_item WHERE i_id = :iid;
