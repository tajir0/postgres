\set wid random(1, 10)
\set r random_zipfian(1, 100000, 1.01)
\set iid 1 + permute(:r - 1, 100000)
SELECT s_quantity, s_data FROM bmsql_stock WHERE s_w_id = :wid AND s_i_id = :iid;
