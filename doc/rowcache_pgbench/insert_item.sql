\set iid random(100001, 200000)
INSERT INTO bmsql_item (i_id, i_im_id, i_name, i_price, i_data) VALUES (:iid, 1, 'x', 1.00, 'x') ON CONFLICT (i_id) DO NOTHING;
