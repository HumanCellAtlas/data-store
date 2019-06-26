TEST_NAMES_POS = [('helloworl12345', "alpha numerica characters"),
                  ('hello@world.com', "email format"),
                  ('hello-world=_@.,ZDc', "special characters"),
                  ('HellOWoRLd', "different cases"),
                  ('ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789'
                   'ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789', "== 128 characters"),
                  ("1", "one character")]

TEST_NAMES_NEG = [
    ("&^#$Hello", "illegal characters 1"),
    ("! <>?world", "illegal characters 2"),
    ('ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789'
     'ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF01234567890', "> 128 characters"),
    ('', "empty")
]
