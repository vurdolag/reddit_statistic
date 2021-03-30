import time
from datetime import datetime
from tzlocal import get_localzone
import sqlite3
import sys
import traceback
import asyncio
import re
from typing import List, Union, Tuple, Dict, Coroutine, Optional


TIME_ZONE = 10800


def get_local_time_offset():
    tz = get_localzone()
    d = datetime.now(tz)
    utc_offset = d.utcoffset().total_seconds()
    if utc_offset != tz:
        return TIME_ZONE - utc_offset
    else:
        return TIME_ZONE


TIME_OFFSET = get_local_time_offset()


class Logger:
    __slots__ = 'level'

    def __init__(self, _level=None):
        self.level: int = _level

    def __getattr__(self, item):
        return lambda string='', p=False: self._logs(string, item + '.txt', p)

    def __call__(self, *args, **kwargs):
        self._logs(*args, **kwargs)

    def _logs(self, strings: str = '', name: str = 'logs_error.txt', p=False):
        """
        Логер и отлов ошибок, печатает полный трейсбек
        :param strings:
        :param name:
        """
        if p:
            print(strings)

        with open(f'logs/{name}', 'a', encoding='utf-8') as f:
            t = time.strftime("%y-%m-%d %H:%M:%S", time.localtime(time.time() + TIME_OFFSET))
            if strings:
                l = f'{t} => {strings}'
                f.write(l)
                f.write('\n> ')

            else:
                l = f'{t} ERROR => '
                a = sys.exc_info()
                if a and a[0] is KeyboardInterrupt or a[0] is asyncio.CancelledError:
                    print('STOP')
                else:
                    f.write(l)
                    traceback.print_exception(*a)
                    traceback.print_exception(*a, file=f)
                    f.write('\n> ')

    def log(self, string, p=False):
        self._logs(string, 'log.txt', p)


logs = Logger()


async def fetch_async(conn, query):
    loop = asyncio.get_event_loop()
    return await loop.run_in_executor(
        None, lambda: conn.cursor().execute(query).fetchall())


def cover_bd(func):
    def wrapper(self, *args, **kwargs):
        self.connection = sqlite3.connect(self.path)
        result = False
        try:
            result = func(self, *args, **kwargs)
            if isinstance(result, list) and not self.return_obj is None:
                result = [self.return_obj(*i) for i in result]

        except sqlite3.IntegrityError as ex:
            print(ex, func.__name__, *args)
        except:
            logs.bd()
        self.connection.close()
        return result

    return wrapper


re_q = re.compile(r'"')
re_q2 = re.compile(r',{2,}')


class sqlbd:
    __slots__ = 'tabs', 'path', 'connection', 'return_obj'

    def __init__(self, tabs='userdata', return_obj=None):
        self.connection = None
        self.tabs = tabs
        self.return_obj = return_obj
        self.path = 'bd/stat.db'

        if self.check(self.tabs):
            self.create_tabs()

    def __contains__(self, item):
        ans = self.get(item)
        if ans:
            a = ans[0][0]
            if isinstance(a, str) and not isinstance(item, str):
                item = str(item)
            if not isinstance(a, str) and isinstance(item, str):
                a = str(a)

            return item == a
        else:
            return False

    @cover_bd
    def check(self, tabs):
        crsr = self.connection.cursor()
        try:
            crsr.execute(f'SELECT * FROM {tabs}')
        except:
            return True
        return False

    @cover_bd
    def create_tabs(self):
        cmd = {
            "post": 'CREATE TABLE "post" ("id" VARCHAR(12) UNIQUE, "title" TEXT, "score_now" INTEGER,'
                    '"score" VARCHAR(256), "content" TEXT, "time_post" INTEGER, "time_pars" INTEGER);',

            "comment": 'CREATE TABLE "comment" ("id" VARCHAR(12) UNIQUE, root_id VARCHAR(12), '
                       '"post_id" VARCHAR(12), "body" VARCHAR(256), "score_now" INTEGER, "score" VARCHAR(256), '
                       '"time_comment" INTEGER, "author" VARCHAR(12));',
        }

        c = cmd.get(self.tabs)

        if c:
            crsr = self.connection.cursor()
            crsr.execute(c)
            self.connection.commit()
            print('Create tab', self.tabs)
            return True

        raise Exception(f'Create tab ERROR {self.tabs}')

    @cover_bd
    def get(self, id, item=''):
        select = '*' if item == '' else item
        crsr = self.connection.cursor()
        crsr.execute(f'SELECT {select} FROM {self.tabs} where id = "{id}";')
        ans = crsr.fetchall()
        return ans

    @cover_bd
    def get_all(self, key='', val=''):
        crsr = self.connection.cursor()
        t = ''
        if key and val:
            t = f'where {key} = "{val}"'
        crsr.execute(f'SELECT * FROM {self.tabs} {t};')
        ans = crsr.fetchall()
        return ans

    @cover_bd
    def get_between(self, key='', val1='', val2=''):
        crsr = self.connection.cursor()
        t = f'where {key} between {val1} and {val2}'
        crsr.execute(f'SELECT * FROM {self.tabs} {t};')
        ans = crsr.fetchall()
        return ans

    @cover_bd
    def put(self, *args):
        x = '('
        for i in args:
            if isinstance(i, str):
                i = re_q.sub("'", i)
                x += f'"{i}", '
            else:
                x += f'{i}, '
        x = x[:-2] + ')'

        sql_command = f"INSERT INTO {self.tabs} VALUES {x};"
        crsr = self.connection.cursor()
        crsr.execute(sql_command)
        self.connection.commit()
        return True

    @cover_bd
    def up(self, id, param='', **kwargs):
        if param and isinstance(param, dict):
            kwargs = param
        kwargs = ''.join(' {} = "{}",'.format(key, re_q.sub("'", str(val))) for key, val in kwargs.items())
        crsr = self.connection.cursor()
        sql_command = f'UPDATE {self.tabs} SET{kwargs[0:len(kwargs) - 1]} where id = "{id}"'
        crsr.execute(sql_command)
        self.connection.commit()
        return True

    @cover_bd
    def delete(self, command):
        crsr = self.connection.cursor()
        command = f'DELETE FROM {self.tabs} WHERE {command};'
        crsr.execute(command)
        self.connection.commit()
        return crsr.rowcount

    @cover_bd
    def castom(self, code):
        crsr = self.connection.cursor()
        crsr.execute(code)
        self.connection.commit()
        return True


class PostBD:
    __slots__ = 'id', 'title', 'score_now', 'score', 'content', 'time_post', 'time_pars'

    def __init__(self, _id, title, score_now, score, content, time_post, time_pars):
        self.id = _id
        self.title = title
        self.score_now = score_now
        self.score = score
        self.content = content
        self.time_post = time_post
        self.time_pars = time_pars

    def up(self, **kwargs):
        BD.post.up(self.id, **kwargs)


class CommentBD:
    __slots__ = ('id', 'root_id', 'post_id', 'body', 'score_now', 'score', 'time', 'author')

    def __init__(self, _id, root_id, post_id, body, score_now, score, _time, author):
        self.id = _id
        self.root_id = root_id
        self.post_id = post_id
        self.body = body
        self.score = score
        self.score_now = score_now
        self.time = _time
        self.author = author


class _BD:
    __slots__ = 'post', 'comment'

    def __init__(self):
        self.post = sqlbd('post', PostBD)
        self.comment = sqlbd('comment', CommentBD)


BD = _BD()


class Loop:
    __slots__ = '_tasks', 'tasks_list', '_start'

    def __init__(self):
        self._tasks: List[Tuple[Coroutine, Union[int, float]]] = []
        self.tasks_list: list = []
        self._start: bool = False

    def add(self, func: Coroutine, time_wait: Union[int, float] = 0):

        if not isinstance(func, Coroutine):
            raise ValueError(f'got type "{type(func)}" need "Coroutine"')

        if time_wait <= 0:
            time_wait = time.time()
        else:
            time_wait += time.time()

        self._tasks.append((func, time_wait))

        return self

    async def worker(self) -> None:
        while True:
            t1 = time.time()
            for task in self._tasks:
                if task[1] <= t1:
                    self.tasks_list.append(asyncio.create_task(task[0]))
                    self._tasks.remove(task)

            [self.tasks_list.remove(task) for task in self.tasks_list if task.done()]

            await asyncio.sleep(1)

    def start(self):
        if not self._start:
            self._start = True
            self.tasks_list.append(asyncio.create_task(self.worker()))

        return asyncio.gather(*self.tasks_list)


loop = Loop()




