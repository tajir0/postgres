\set wid random(1, 10)
\set r random_zipfian(1, 100000, 1.01)
\set iid 1 + permute(:r - 1, 100000)
UPDATE bmsql_stock SET s_ytd = s_ytd + 1 WHERE s_w_id = :wid AND s_i_id = :iid;
