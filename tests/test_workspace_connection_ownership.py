"""Bound native workspace transaction handles without changing fencing or durability."""
import sqlite3
from concurrent.futures import ThreadPoolExecutor
from contextlib import closing
from types import SimpleNamespace
import pytest
from pa.config import Settings
from pa.repository.workspace import WorkspaceManager

@pytest.fixture
def tracked(tmp_path,monkeypatch):
 manager=WorkspaceManager(Settings(data_dir=tmp_path/'data',workspace_root=tmp_path/'workspaces',instance_id='fixture',instance_name='fixture'),SimpleNamespace())
 connections=[];original=manager._connect
 def connect():
  conn=original();connections.append(conn);return conn
 monkeypatch.setattr(manager,'_connect',connect)
 return manager,connections,original

def assert_closed(connections):
 assert connections
 for conn in connections:
  with pytest.raises(sqlite3.ProgrammingError,match='closed'):conn.execute('SELECT 1')

def test_read_and_return_inside_transaction_close_before_return(tracked):
 manager,connections,original=tracked
 assert manager.list()==[] and manager.metrics()=={}
 assert manager._next_fence('fixture')==1
 assert_closed(connections)
 with closing(original())as conn:assert conn.execute('SELECT next_token FROM workspace_fences WHERE repository_id=?',('fixture',)).fetchone()[0]==2

def test_failure_rolls_back_and_closes_without_consuming_fence(tracked):
 manager,connections,original=tracked
 with closing(original())as conn,conn:conn.execute("CREATE TRIGGER reject_fence AFTER INSERT ON workspace_fences BEGIN SELECT RAISE(ABORT,'fixture abort'); END")
 with pytest.raises(sqlite3.IntegrityError,match='fixture abort'):manager._next_fence('fixture')
 assert_closed(connections)
 with closing(original())as conn,conn:
  assert conn.execute('SELECT * FROM workspace_fences').fetchall()==[]
  conn.execute('DROP TRIGGER reject_fence')
 assert manager._next_fence('fixture')==1
 assert_closed(connections)

def test_contended_fences_remain_unique_durable_and_connections_close(tracked):
 import threading
 manager,connections,original=tracked
 owned=threading.local()
 def connect():
  conn=original();owned.connection=conn;return conn
 manager._connect=connect
 def one(_):
  token=manager._next_fence('fixture')
  assert_closed([owned.connection])
  return token
 with ThreadPoolExecutor(max_workers=6)as pool:tokens=list(pool.map(one,range(30)))
 assert sorted(tokens)==list(range(1,31))
 with closing(original())as conn:assert conn.execute('SELECT next_token FROM workspace_fences WHERE repository_id=?',('fixture',)).fetchone()[0]==31

from pa.core.sqlite_metrics import sqlite_connection_scope

def closed(conn):
 with pytest.raises(sqlite3.ProgrammingError,match='closed'):conn.execute('SELECT 1')

def test_worker_scope_reuses_connection_with_native_durable_transactions(tracked):
 manager,connections,original=tracked
 with sqlite_connection_scope():
  assert manager._next_fence('fixture')==1
  manager._increment_metric('fixture',2)
  assert manager.metrics()=={'fixture':2} and manager.list()==[]
  assert len(connections)==1
  # A separate native reader sees commits before the worker releases ownership.
  with closing(original())as check:
   assert check.execute('SELECT value FROM workspace_metrics WHERE key=?',('fixture',)).fetchone()[0]==2
 closed(connections[0])

def test_error_discards_handle_rolls_back_and_reopens_within_same_scope(tracked):
 manager,connections,original=tracked
 with closing(original())as conn,conn:conn.execute("CREATE TRIGGER reject_fence AFTER INSERT ON workspace_fences BEGIN SELECT RAISE(ABORT,'fixture abort'); END")
 with sqlite_connection_scope():
  with pytest.raises(sqlite3.IntegrityError,match='fixture abort'):manager._next_fence('fixture')
  closed(connections[0])
  with closing(original())as conn,conn:
   assert conn.execute('SELECT * FROM workspace_fences').fetchall()==[]
   conn.execute('DROP TRIGGER reject_fence')
  assert manager._next_fence('fixture')==1
  assert len(connections)==2 and connections[1] is not connections[0]
 for conn in connections:closed(conn)

def test_nested_scope_preserves_owner_until_outer_exit(tracked):
 manager,connections,_=tracked
 with sqlite_connection_scope():
  manager.metrics()
  with sqlite_connection_scope():assert manager.list()==[]
  assert len(connections)==1 and connections[0].execute('SELECT 1').fetchone()[0]==1
 closed(connections[0])

@pytest.mark.asyncio
async def test_actual_worker_reuses_and_closes_before_completion(tracked,monkeypatch):
 from pa.core.async_runtime import AsyncRuntime
 from pa.core.sqlite_metrics import ObservedConnection
 manager,connections,_=tracked
 completed_closes=[];native_close=ObservedConnection.close
 def close(conn):
  native_close(conn)
  completed_closes.append(id(conn))
 monkeypatch.setattr(ObservedConnection,'close',close)
 runtime=AsyncRuntime(max_workers=1)
 def work():
  manager._increment_metric('fixture')
  assert manager.list()==[]
  return manager.metrics()
 try:
  assert await runtime.run_blocking('workspace.collect_garbage',work)=={'fixture':1}
  assert len(connections)==1
  assert completed_closes==[id(connections[0])]
  operation=runtime.snapshot()['operations']['workspace.collect_garbage']
  assert operation['completed']==1 and operation['failed']==operation['timed_out']==0
 finally:await runtime.close()
