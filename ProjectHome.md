Sirenial aims:
  * to provide Haskell programmers with a moderately type-safe way to construct and execute SQL queries;
  * to use simple types, resulting in friendly type errors;
  * to generate simple, predictable SQL;
  * to transparently combine similar queries into one query.

The first two aims bite each other: more type-safety means more involved types, while more involved types result in more daunting type errors. Sirenial aims to find the perfect balance.