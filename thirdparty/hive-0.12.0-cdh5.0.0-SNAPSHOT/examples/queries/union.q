FROM (
  FROM src select src.key, src.value WHERE src.key < 100
  UNION ALL
  FROM src SELECT src.* WHERE src.key > 100
) unioninput
INSERT OVERWRITE DIRECTORY 'target/tmp/ql/test/data/warehouse/union.out' SELECT unioninput.*
