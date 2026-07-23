\set wid random(1, 10)
\set did random(1, 10)
\set cid random(1, 3000)
SELECT c_balance, c_first, c_last FROM bmsql_customer WHERE c_w_id = :wid AND c_d_id = :did AND c_id = :cid;
