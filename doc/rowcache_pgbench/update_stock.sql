\set wid random(1, 10)
\set iid random(1, 100000)
UPDATE bmsql_stock SET s_ytd = s_ytd + 1 WHERE s_w_id = :wid AND s_i_id = :iid;
