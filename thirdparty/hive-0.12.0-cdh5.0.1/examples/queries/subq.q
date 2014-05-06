FROM (
  FROM src select src.* WHERE src.key < 100
) unioninput
INSERT OVERWRITE DIRECTORY 'target/tmp/ql/test/data/warehouse/union.out' SELECT unioninput.*
