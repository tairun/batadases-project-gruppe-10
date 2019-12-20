- ThreadPoolExecutor starts too many Threads and insert method throws error.

```bash
PS D:\_projects\batadases-project-gruppe-10> python -m app
Press 'Enter' to start the integration process ...
Press 'Enter' to start download and extraction process ...
Parsing links ...: 100%|█████████████████████████████████████████████████████████| 2568/2568 [00:00<00:00, 23087.57it/s]
1814 links found in date range '2015-01-01 00:00:00 - 2019-12-31 00:00:00'!
Downloading and integrating GDELT ...: 0it [00:00, ?it/s]This is here ...

Inserting ./raw_data/gdelt\20191219.export.CSV ...:   0%|                                    | 0/166834 [00:00<?, ?it/s]

Inserting ./raw_data/gdelt\20191214.export.CSV ...:   0%|                                     | 0/97333 [00:03<?, ?it/s]


Inserting ./raw_data/gdelt\20191215.export.CSV ...:   0%|                                     | 0/86495 [00:00<?, ?it/s]T             raceback (most recent call last):
  File "d:\tools\python-3.7.4\Lib\runpy.py", line 193, in _run_module_as_main
    "__main__", mod_spec)
  File "d:\tools\python-3.7.4\Lib\runpy.py", line 85, in _run_code
    exec(code, run_globals)
  File "D:\_projects\batadases-project-gruppe-10\app.py", line 56, in <module>
    main()
  File "D:\_projects\batadases-project-gruppe-10\app.py", line 40, in main
    wait(results, return_when=ALL_COMPLETED)
  File "d:\tools\python-3.7.4\Lib\concurrent\futures\_base.py", line 284, in wait
    with _AcquireFutures(fs):
  File "d:\tools\python-3.7.4\Lib\concurrent\futures\_base.py", line 142, in __init__
    self.futures = sorted(futures, key=id)
  File "C:\Users\lucku\.virtualenvs\batadases-project-gruppe-10-Ht-MA7oa\lib\site-packages\tqdm\std.py", line 1099, in __             iter__
    for obj in iterable:
  File "d:\tools\python-3.7.4\Lib\concurrent\futures\_base.py", line 598, in result_iterator
    yield fs.pop().result()
  File "d:\tools\python-3.7.4\Lib\concurrent\futures\_base.py", line 435, in result
    return self.__get_result()
  File "d:\tools\python-3.7.4\Lib\concurrent\futures\_base.py", line 384, in __get_result
    raise self._exception
  File "d:\tools\python-3.7.4\Lib\concurrent\futures\thread.py", line 57, in run
    result = self.fn(*self.args, **self.kwargs)
  File "D:\_projects\batadases-project-gruppe-10\src\GdeltIntegrator.py", line 129, in gdelt_wrapper
    self.insert_wrapper(csv_file, self.headers)
  File "D:\_projects\batadases-project-gruppe-10\src\DataIntegrator.py", line 181, in insert_wrapper
    self.insert_data(conn, cur, row, table_name)
  File "D:\_projects\batadases-project-gruppe-10\src\DataIntegrator.py", line 95, in insert_data
    raise e
  File "D:\_projects\batadases-project-gruppe-10\src\DataIntegrator.py", line 91, in insert_data
    columns = self.tables[table_name]["headers"]
KeyError: 'headers'
```