delimiter //;
DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='BMSQL_CONFIG' ;
  IF (v1 = 1) then
    execute immediate 'drop table bmsql_config';
  END IF;
END//;

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='BMSQL_NEW_ORDER' ;
  IF (v1 = 1) then
    execute immediate 'drop table bmsql_new_order;';
  END IF;
END//;

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='BMSQL_ORDER_LINE' ;
  IF (v1 = 1) then
    execute immediate 'drop table bmsql_order_line;';
  END IF;
END//;

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='BMSQL_OORDER' ;
  IF (v1 = 1) then
    execute immediate 'drop table bmsql_oorder;';
  END IF;
END//;

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='BMSQL_WAREHOUSE' ;
  IF (v1 = 1) then
    execute immediate 'drop table bmsql_warehouse;';
  END IF;
END//;

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='BMSQL_DISTRICT' ;
  IF (v1 = 1) then
    execute immediate 'drop table bmsql_district;';
  END IF;
END//;

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='BMSQL_ITEM' ;
  IF (v1 = 1) then
    execute immediate 'drop table bmsql_item;';
  END IF;
END//;

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='BMSQL_STOCK' ;
  IF (v1 = 1) then
    execute immediate 'drop table bmsql_stock;';
  END IF;
END//;

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='BMSQL_CUSTOMER' ;
  IF (v1 = 1) then
    execute immediate 'drop table bmsql_customer;';
  END IF;
END//;

DECLARE v1 INT;
BEGIN
  select count(*) into v1 from user_tables where table_name='BMSQL_HISTORY' ;
  IF (v1 = 1) then
    execute immediate 'drop table bmsql_history;';
  END IF;
END//;

drop tablegroup tpcc_group;

drop sequence bmsql_hist_id_seq;