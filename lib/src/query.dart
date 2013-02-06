part of postgresql;

class _Query implements Query {

  _Query(this.sql, this._resultMapper, this._resultReader)
    : _state = _CREATED;

  _QueryState _state;

  final String sql;
  _QueryState get state => _state;
  final Mapper _resultMapper;
  final _ResultReader _resultReader;
  final Streamer<dynamic> _streamer = new Streamer<dynamic>();

  void _log(String msg) => print(msg);

  void changeState(_QueryState state) {
    if (state == _state) {
      return;
    }
    _log('Query state change: $_state => $state.');
    _state = state;
  }

  void readResult() {
    _resultMapper.onData(_resultReader, _streamer);
  }

  // Delegate to stream impl.
  void onReceive(void receiver(dynamic value)) =>
      _streamer.stream.onReceive(receiver);
  Future<dynamic> one() => _streamer.stream.one();
  Future<List<dynamic>> all() => _streamer.stream.all();
 
  Future then(onValue(value), { onError(AsyncError asyncError) }) => _streamer.stream.then(onValue, onError: onError);
  Future catchError(onError(AsyncError asyncError),
                    {bool test(Object error)}) => _streamer.stream.catchError(onError, test: test);
  Future whenComplete(action()) => _streamer.stream.whenComplete(action);
  Stream asStream() => _streamer.stream.asStream();
  
  void complete([value]) => _streamer.complete(value);
  void completeError(Object exception, [Object stackTrace]) =>
      _streamer.completeError(exception, stackTrace);
}

