import 'dart:io';
import 'dart:async';
//TODO #import('package:postgresql/postgresql.dart');
import '../lib/postgresql.dart' as pg;

void main() {
  var s = new pg.Settings(host: 'localhost', port: 5432, username: 'testdb', database: 'testdb', password: 'password');
  pg.connect(s)
  .then((c) {
    print('connected...');
    runExampleQuery(c);
  })
  .catchError((e) {
    print('Exception: $e');
    return true;
  });
}

void runExampleQuery(pg.Connection c) {
  var sql = 'select 1 as one, \'sfdgsdfgdfgfd\' as two, 3.1 as three;';

  c.query(sql).one()
    .then((result) {
      print(result.one);
      print(result.two);
      print(result.three);
    })
    .catchError((err) {
      print(err);
      return true;
    });
}

void runExampleQueryBad(pg.Connection c) {
  //var sql = 'select 1 as one, \'2\' as two, 3.1 as three;';
  var sql = 'dsfsdfdsf';

  c.query(sql).one()
    .then((result) {
      print(result);
    })
    .catchError((err) {
      print(err);

      // Check that state is still OK by running another query.
      runExampleQuery(c);

      return true;
    });
}

void runExampleQueries(pg.Connection c) {

  var q1 = c.query('select 1 as one, \'2\' as two, 3.1 as three').one();
  var q2 = c.query('select 1 as one, \'2\' as two, 3.1 as three').one();
  var q3 = c.query('select 1 as one, \'2\' as two, 3.1 as three').one();

  Future.wait([q1, q2, q3])
  .then((result) {
    print(result);
    c.close();
  })
  .catchError((err) {
    print(err);
    return true;
  });

}

void runMultiQuery(pg.Connection c) {
  var sql = 'select 1 as one, \'2\' as two, 3.1 as three;'
      ' select 1 as one, \'2\' as two, 3.1 as three;';

  c.query(sql).all()
    .then((result) {
      print(result);
    })
    .catchError((err) {
      print(err);
      return true;
    });
}