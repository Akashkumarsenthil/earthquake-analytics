"""Credential-free regressions for the real ingestion functions and dbt predicate."""
import ast
from pathlib import Path
import logging
import sqlite3
import unittest
from unittest.mock import Mock
from datetime import datetime

ROOT = Path(__file__).resolve().parents[1]

def function(name, **dependencies):
    tree = ast.parse((ROOT / 'dags/earthquake_ingestion.py').read_text())
    node = next(n for n in tree.body if isinstance(n, ast.FunctionDef) and n.name == name)
    scope = dict(logging=logging, datetime=datetime, DATABASE='TEST_DB', SCHEMA_RAW='RAW',
                 SNOWFLAKE_CONN_ID='test', USGS_HISTORICAL_URL='https://example.test', **dependencies)
    exec(compile(ast.Module(body=[node], type_ignores=[]), '<actual ingestion function>', 'exec'), scope)
    return scope[name]

class Regressions(unittest.TestCase):
    def test_historical_loader_reads_correct_xcom_and_batches(self):
        ti = Mock(); ti.xcom_pull.return_value = [{'event_id':'event-1'}]
        cursor=Mock(); conn=Mock(); conn.cursor.return_value=cursor
        context=Mock(); context.__enter__=Mock(return_value=conn); context.__exit__=Mock(return_value=False)
        hook=Mock(); hook.get_conn.return_value=context
        loader=function('load_to_snowflake', SnowflakeHook=Mock(return_value=hook))
        self.assertEqual(loader(source_task_id='fetch_historical_earthquakes', ti=ti),1)
        ti.xcom_pull.assert_called_once_with(key='earthquake_records',task_ids='fetch_historical_earthquakes')
        cursor.executemany.assert_called_once()
        conn.commit.assert_called_once()
        tree=ast.parse((ROOT/'dags/earthquake_ingestion.py').read_text())
        task=next(n.value for n in ast.walk(tree) if isinstance(n,ast.Assign) and any(isinstance(t,ast.Name) and t.id=='load_historical_task' for t in n.targets))
        self.assertEqual(ast.literal_eval(next(k.value for k in task.keywords if k.arg=='op_kwargs')),{'source_task_id':'fetch_historical_earthquakes'})

    def test_empty_load_never_connects(self):
        ti=Mock();ti.xcom_pull.return_value=[];hook=Mock()
        self.assertEqual(function('load_to_snowflake',SnowflakeHook=hook)(ti=ti),0)
        hook.assert_not_called()

    def test_backfill_fetches_second_page(self):
        event={'id':'one','properties':{'time':1},'geometry':{'coordinates':[1,2,3]}}
        request=Mock();pages=iter([{'features':[event]*20000},{'features':[dict(event,id='two')]}]);offsets=[]
        def get(url,params,timeout):
            offsets.append(params.get('offset',1)); r=Mock();r.json.return_value=next(pages);return r
        request.get.side_effect=get;ti=Mock()
        total=function('fetch_historical_earthquakes',requests=request)(ti=ti,dag_run=None)
        self.assertEqual(total,20001);self.assertEqual(offsets,[1,20001])

    def test_incremental_revision_uses_each_event_not_global_max(self):
        sql=(ROOT/'dbt/earthquake_analytics/models/marts/fct_earthquakes.sql').read_text()
        predicate=sql.split('{% if is_incremental() %}')[1].split('{% endif %}')[0].replace('{{ this }}','target')
        c=sqlite3.connect(':memory:')
        c.executescript('CREATE TABLE staged(event_id TEXT,updated_timestamp INT,event_timestamp INT); CREATE TABLE target(event_id TEXT,updated_timestamp INT,event_timestamp INT);')
        c.executemany('INSERT INTO target VALUES(?,?,?)',[('old',10,1),('newer',100,2),('same',50,3)])
        c.executemany('INSERT INTO staged VALUES(?,?,?)',[('old',20,1),('newer',100,2),('same',50,3),('unseen',5,4)])
        result=c.execute('SELECT s.event_id FROM staged s '+predicate).fetchall()
        self.assertEqual(set(result),{('old',),('unseen',)})

if __name__=='__main__':unittest.main(verbosity=2)
