\set iid random(1, 100000)
SELECT i_price, i_name, i_data FROM bmsql_item WHERE i_id = :iid;
