# encoding=utf-8
"""
Python client for ssdb
"""

import socket
from itertools import izip, chain
import os

update_cmd = ['set', 'setx', 'zset', 'hset', 'del', 'zdel', 'hdel', 'multi_set', 'multi_del', 'multi_hset', 'multi_hdel',
              'multi_zset', 'multi_zdel']

single_get_cmd = ['get', 'hget']

incr_cmd = ['incr', 'decr', 'zincr', 'zdecr', 'hincr', 'hdecr', 'hsize', 'zsize', 'zget', 'zrank', 'zrrank']

key_cmd = ['keys', 'zkeys', 'hkeys', 'hlist', 'zlist']

scan_key = ['scan', 'rscan', 'hscan', 'hrscan', 'multi_get', 'multi_hget']

zscan_key = ['zscan', 'zrscan', 'zrange', 'zrrange', 'multi_zget']


class SSDBResponse(object):
    """
    A response from ssdb server.

    parameters:
        code:result code
        data_or_message:data if code is 'ok';other message

    """

    def __init__(self, code='', data_or_message=None):
        self.code = code
        self.data = None
        self.message = None

        if code == 'ok':
            self.data = data_or_message
        else:
            self.message = data_or_message

    def __repr__(self):
        return ((str(self.code) + ' ') + str(self.message)) + ' ' + str(self.data)

    def ok(self):
        return self.code == 'ok'

    def not_found(self):
        return self.code == 'not_found'


class SSDB(object):
    """
    A client for ssdb

    parameters:
        host:host to connect
        port:port to connect
        socket_timeout:socket_timeout to set
        max_connections:connection pool's max connection count
    """

    def __init__(self, host='127.0.0.1', port=8888, socket_timeout=None, max_connections=1):
        self.host = host
        self.port = port
        self.socket_timeout = socket_timeout
        self.max_connections = max_connections
        self.connection_pool = ConnectionPool(self.host, self.port, self.socket_timeout, self.max_connections)

    def set(self, key, value, ttl=None):
        """
        Set key's value.

        parameters:
            ttl: time to live of key's value

        return:
            'ok' code if success,other code failed.
        """
        if not ttl or int(ttl) == -1:
            return self.request("set", [key, value])
        return self.request("setx", [key, value, int(ttl)])

    def get(self, key):
        """
        Get key's value.

        return:
            'ok' code if success,'data' contain value;
            'not_found' code if key not exist;
            other code failed
        """
        return self.request("get", [key])

    def delete(self, key):
        """
        delete key's value

        return:
            'ok' code if success,other code failed.
        """
        return self.request("del", [key])

    def incr(self, key, increment):
        """
        increment key's value

        return:
            'ok' code if success,'data' is the updated value,other code failed
        """
        return self.request("incr", [key, increment])

    def decr(self, key, decrement):
        """
        decrement key's value

        return:
            'ok' code if success,'data' is the updated value,other code failed
        """
        return self.request("decr", [key, decrement])

    def keys(self, key_lower, key_upper, limit):
        """
        list keys in range (key_lower,key_upper],('',''] means no range limit

        parameters:
            key_lower:lower bound(not include),empty string means no limit
            key_upper:upper bound(include),empty string means no limit
            limit:up to that records will be returned
        return:
            'ok' code if success,'data' is a keys list
            other code failed
        """
        return self.request("keys", [key_lower, key_upper, limit])

    def scan(self, key_lower, key_upper, limit):
        """
        list key-value pairs in key range (key_lower,key_upper]

        parameters:
            key_lower:lower bound(not include),empty string means no limit
            key_upper:upper bound(include),empty string means no limit
            limit:up to that records will be returned
        return:
            'ok',code if success,'data["index"]' is a keys list,'data["items"]' is key-value dict
            other code failed
        """
        return self.request("scan", [key_lower, key_upper, limit])

    def scan_iterator(self, key_lower):
        """
        return a key-value tuple iterator.
        """
        start_key = key_lower
        while start_key is not None:
            response = self.scan(start_key, "", 1000)
            index, items = response.data['index'], response.data['items']
            start_key = index[-1] if index else None
            for key in index:
                yield (key, items[key])


    def rscan(self, key_upper, key_lower, limit):
        """
        list key-value pairs in key range (key_upper,key_lower] in reverse order

        parameters:
            key_upper:upper bound(not include),empty string means no limit
            key_lower:lower bound(include),empty string means no limit
            limit:up to that records will be returned
        return:
            'ok',code if success,'data["index"]' is a keys list,'data["items"]' is key-value dict
            other code failed
        """
        return self.request("rscan", [key_upper, key_lower, limit])

    def multi_set(self, key_value_map):
        """
        Set multiple key-value pairs

        parameters: a key-value dict

        return 'ok' code if success,other code failed

        """
        l = []
        for key, value in key_value_map.items():
            l = l + [key, value]
        return self.request("multi_set", l)

    def multi_get(self, keys):
        """
        Get those keys' values

        parameters:
            keys:keys list

        return:
            return 'ok' code if success,'data["index"]' is a keys list,'data["items"]' is key-value dict
            other code failed
        """
        return self.request("multi_get", keys)

    def multi_del(self, keys):
        """
        Delete those keys' values

        parameters:
            keys:keys list

        return:
            return 'ok' code if success,other code failed
        """
        return self.request("multi_del", keys)

    def hset(self, name, key, value):
        """
        Set a key's value of a hashmap

        parameters:
            name:hashmap's name
            key:key
            value:value

        return:
            'ok' code if success,other code failed.
        """
        return self.request("hset", [name, key, value])

    def hget(self, name, key):
        """
        Get a key's value of a hashmap

        parameters:
            name:hashmap's name
            key:key

        return:
            'ok' code if success,'data' contain value;other code failed
        """
        return self.request("hget", [name, key])

    def hdel(self, name, key):
        """
        Delete a key's value of a hashmap

        parameters:
            name:hashmap's name
            key:key

        return:
            'ok' code if success,other code failed
        """
        return self.request("hdel", [name, key])

    def hincr(self, name, key, increment):
        """
        Increment a key's value of a hashmap

        parameters:
            name:hashmap's name
            key:key
            increment:increment value

        return:
            'ok' code if success,'date' contain updated value;other code failed
        """
        return self.request("hincr", [name, key, increment])

    def hdecr(self, name, key, decrement):
        """
        Decrement a key's value of a hashmap

        parameters:
            name:hashmap's name
            key:key
            decrement:decrement value
        return:
            'ok' code if success,'date' contain updated value;other code failed
        """
        return self.request("hdecr", [name, key, decrement])

    def hsize(self, name):
        """
        Get a hashmap's size

        parameters:
            name:hashmap's name

        return:
            'ok' code if success,'date' contain hashmap's size
        """
        return self.request("hsize", [name])

    def hlist(self, name_lower, name_upper, limit):
        """
        Get hashmap names in range (name_lower,name_upper]

        parameters:
            name_lower:lower bound(not include) of names to be return,empty string means no limit
            name_upper:upper bound(include) of names to be return,empty string means no limit
            limit:the limit of items to be return

        return:
            'ok' code if success,'data' is names list;other code failed
        """
        return self.request("hlist", [name_lower, name_upper, limit])

    def hkeys(self, name, key_lower, key_upper, limit):
        """
        Get keys in range (name_lower,name_upper] of a hashmap

        parameters:
            name:hashmap name
            key_lower:lower bound(not include) of keys to be return,empty string means no limit
            key_upper:upper bound(include) of keys to be return,empty string means no limit
            limit:the limit of items to be return

        return:
            'ok' code if success,'data' is keys list;other code failed
        """
        return self.request("hkeys", [name, key_lower, key_upper, limit])

    def hscan(self, name, key_lower, key_upper, limit):
        """
        list key-value pairs in key range (key_lower,key_upper] of a hashmap

        parameters:
            name:hashmap name
            key_lower:lower bound(not include),empty string means no limit
            key_upper:upper bound(include),empty string means no limit
            limit:up to that records will be returned
        return:
            'ok',code if success,'data["index"]' is a keys list,'data["items"]' is key-value dict
            other code failed
        """
        return self.request("hscan", [name, key_lower, key_upper, limit])

    def hrscan(self, name, key_end, key_start, limit):
        """
        list key-value pairs in key range (key_upper,key_lower] of a hashmap in reverse order

        parameters:
            name:hashmap name
            key_upper:upper bound(not include),empty string means no limit
            key_lower:lower bound(include),empty string means no limit
            limit:up to that records will be returned
        return:
            'ok',code if success,'data["index"]' is a keys list,'data["items"]' is key-value dict
            other code failed
        """
        return self.request("hrscan", [name, key_end, key_start, limit])

    def multi_hset(self, name, key_value_map):
        """
        Set multiple key-value pairs of a hashmap

        parameters:
            name:hashmap name
            key_value_map: key-value dict

        return 'ok' code if success,other code failed

        """
        l = [name]
        for key, value in key_value_map.items():
            l = l + [key, value]
        return self.request("multi_hset", l)

    def multi_hget(self, name, keys):
        """
        Get those keys' values of a hashmap

        parameters:
            name:hashmap name
            keys:keys list

        return:
            return 'ok' code if success,'data["index"]' is a keys list,'data["items"]' is key-value dict
            other code failed
        """
        return self.request("multi_hget", [name] + keys)

    def multi_hdel(self, name, keys):
        """
        Delete those keys' values of a hashmap

        parameters:
            name:hashmap name
            keys:keys list

        return:
            return 'ok' code if success,other code failed
        """
        return self.request("multi_hdel", [name] + keys)

    def zset(self, name, key, score):
        """
        Set a key's score of a zset

        parameters:
            name:zset name
            key:key
            score:key's score

        return:
            return 'ok' code if success,other code failed
        """
        return self.request("zset", [name, key, score])

    def zget(self, name, key):
        """
        Get a key's score of a zset

        parameters:
            name:zset name
            key:key

        return:
            return 'ok' code if success,'data' contain key's score;other code failed
        """
        return self.request("zget", [name, key])

    def zdel(self, name, key):
        """
        Delete a key's score of a zset

        parameters:
            name:zset name
            key:key

        return:
            return 'ok' code if success,other code failed
        """
        return self.request("zdel", [name, key])

    def zincr(self, name, key, increment):
        """
        Increment a key's score of a zset

        parameters:
            name: zset name
            key:key
            increment:score's increment

        return:
            return 'ok' code if success,'data' contain updated score;other code failed
        """
        return self.request("zincr", [name, key, increment])

    def zsize(self, name):
        """
        Get the size of a zset

        parameters:
            name:zset name

        return:
            'ok' code if success,'data' contain size;other code failed
        """
        return self.request("zsize", [name])

    def zlist(self, name_lower, name_upper, limit):
        """
        Get zset names in range (name_lower,name_upper]

        parameters:
            name_lower:lower bound(not include) of names to be return,empty string means no limit
            name_upper:upper bound(include) of names to be return,empty string means no limit
            limit:the limit of items to be return

        return:
            'ok' code if success,'data' is names list;other code failed
        """
        return self.request("zlist", [name_lower, name_upper, limit])

    def zkeys(self, name, key_lower, score_lower, score_upper, limit):
        """
        List keys of a zset in range (key_lower+score_lower, score_upper].
        return keys in (key.score == score_lower && key > key_lower || key.score > score_lower).
        ("", ""] means no range limit.

        parameters:
            name - zset name
            key_lower - The key related to score_lower, could be empty string.
            score_lower - The minimum score related to keys(may not be included, depend on key_lower), empty string means -inf(no limit).
            score_upper - The maximum score(include) related to keys, empty string means +inf(no limit).
            limit - Up to that many elements will be returned.

        return:
            'ok' code if success,'data' is keys list;other code failed
        """
        return self.request("zkeys", [name, key_lower, score_lower, score_upper, limit])

    def zscan(self, name, key_lower, score_lower, score_upper, limit):
        """
        List key-score pairs of a zset in range (key_lower+score_lower, score_upper].
        return keys in (key.score == score_lower && key > key_lower || key.score > score_lower).
        ("", ""] means no range limit.

        parameters:
            name - zset name
            key_lower - The key related to score_lower, could be empty string.
            score_lower - The minimum score related to keys(may not be included, depend on key_lower), empty string means -inf(no limit).
            score_upper - The maximum score(include) related to keys, empty string means +inf(no limit).
            limit - Up to that many elements will be returned.

        return:
            'ok' code if success,'data['index']' is keys list,'data[items] is key-score pairs';other code failed
        """
        return self.request("zscan", [name, key_lower, score_lower, score_upper, limit])

    def zrscan(self, name, key_upper, score_upper, score_lower, limit):
        """
        List key-score pairs of a zset in range (key_upper+score_upper, score_lower] in reverse order.
        return keys in (key.score == score_upper && key < key_upper || key.score < score_upper).
        ("", ""] means no range limit.

        parameters:
            name - zset name
            key_upper - The key related to score_upper, could be empty string.
            score_upper - The maximum score related to keys(may not be included, depend on key_upper), empty string means -inf(no limit).
            score_lower - The minimum score(include) related to keys, empty string means +inf(no limit).
            limit - Up to that many elements will be returned.

        return:
            'ok' code if success,'data['index']' is key list,'data[items] is key-score pairs';other code failed
        """
        return self.request("zrscan", [name, key_upper, score_upper, score_lower, limit])

    def multi_zset(self, name, key_value_map):
        """
        Set multiple key-score pairs of a zset in one method call.

        parameters:
            name: zset name
            key_value_map: key-score dict.

        return:
            return 'ok' code if success,other code failed
        """
        l = [name]
        for key, value in key_value_map.items():
            l = l + [key, value]
        return self.request("multi_zset", l)

    def multi_zget(self, name, keys):
        """
        Get keys' score of a zset

        parameters:
            name:zset name
            keys: key list

        return:
            return 'ok' code if success,'data["index"]' key list,'data["items"]' is key-score pairs of 'data["index"]'
            other code failed
        """
        return self.request("multi_zget", [name] + keys)

    def multi_zdel(self, name, keys):
        """
        Delete keys' score of a zset

        parameters:
            name:zset name
            keys:key list

        return:
            return 'ok' code if success,other code failed
        """
        return self.request("multi_zdel", [name] + keys)

    def request(self, cmd, params=[]):
        connection = self.connection_pool.get_connection()
        try:
            params = [cmd] + params
            connection.send_cmd(self.generate_cmd(params))
            resp = connection.read_response()

            return self.parse_response(cmd, resp)
        finally:
            self.connection_pool.release(connection)

    def generate_cmd(self, data):
        """
        Generate ssdb cmd
        """
        params = []
        [params.extend([str(len(str(item))), str(item)]) for item in data]
        cmd = ('\n'.join(params) + '\n\n')
        return cmd

    def parse_response(self, cmd, resp):
        """
        解析返回的数据
        """
        if not resp:
            return SSDBResponse('error', 'Unknown error')

        if len(resp) == 0:
            return SSDBResponse('disconnected', 'Connection closed')

        #
        if cmd in update_cmd:
            if len(resp) > 1:
                return SSDBResponse(resp[0], int(resp[1]))
            else:
                return SSDBResponse(resp[0], 1)

        if cmd in single_get_cmd:
            if resp[0] == 'ok':
                if len(resp) == 2:
                    return SSDBResponse('ok', resp[1])
                else:
                    return SSDBResponse('server_error', 'Invalid response')
            else:
                return SSDBResponse(resp[0])

        if cmd in incr_cmd:
            if resp[0] == 'ok':
                if len(resp) == 2:
                    try:
                        val = int(resp[1])
                        return SSDBResponse('ok', val)
                    except Exception, e:
                        return SSDBResponse('server_error', 'Invalid response')
                else:
                    return SSDBResponse('server_error', 'Invalid response')
            else:
                return SSDBResponse(resp[0])

        if cmd in key_cmd:
            if resp[0] == 'ok':
                return SSDBResponse(resp[0], [] + resp[1:])

        if cmd in scan_key:
            if resp[0] == 'ok':
                if len(resp) % 2 == 1:
                    #返回一个iterator
                    iterator = iter(resp[1:])
                    #生成items
                    iz = tuple(izip(iterator, iterator))
                    index = [item[0] for item in iz]
                    items = dict(iz)
                    data = {'index': index, 'items': items}
                    return SSDBResponse('ok', data)
                else:
                    return SSDBResponse('server_error', 'Invalid response')
            else:
                return SSDBResponse(resp[0])

        if cmd in zscan_key:
            if resp[0] == 'ok':
                if len(resp) % 2 == 1:
                    #将score int化
                    format_resp = map(lambda item: self.map_func(item), enumerate(resp[1:]))

                    #返回一个iterator
                    iterator = iter(format_resp)
                    #生成items
                    iz = tuple(izip(iterator, iterator))
                    index = [item[0] for item in iz]
                    items = dict(iz)
                    data = {'index': index, 'items': items}
                    return SSDBResponse('ok', data)
                else:
                    return SSDBResponse('server_error', 'Invalid response')
            else:
                return SSDBResponse(resp[0])
        return None

    def map_func(self, item):
        if item[0] % 2 == 1:
            return int(item[1])
        return item[1]


class ConnectionError(Exception):
    pass


class Connection(object):
    def __init__(self, host='127.0.0.1', port=8888, socket_timeout=None):
        self.host = host
        self.port = port
        self.socket_timeout = socket_timeout
        self.socket = None
        self.buf = ''
        self.pid = os.getpid()

    def connect(self):
        if self.socket:
            return
        try:
            self.socket = self._connect()
        except socket.error, e:
            raise ConnectionError(e)

    def _connect(self):
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.settimeout(self.socket_timeout)
        sock.connect((self.host, self.port))
        return sock

    def dis_connect(self):
        try:
            if self.socket:
                self.socket.close()
        except socket.error:
            pass
        finally:
            self.socket = None
            self.buf = ''

    def send_cmd(self, cmd):
        if not self.socket:
            #don't need close if failed
            self.socket = self._connect()
        try:
            self.socket.sendall(cmd)
        except Exception, e:
            self.dis_connect()
            raise ConnectionError("error when write to socket:%s" % e)

    def read_response(self):
        while True:
            ret = self.parse()

            if not ret:
                self._read_response()
            else:
                return ret

    def _read_response(self):
        try:
            data = self.socket.recv(1024 * 8)
        except Exception, e:
            self.dis_connect()
            raise ConnectionError("error when recv from socket:%s" % e)
        self.buf += data

    def parse(self):
        r"""
        读取返回的数据,ssdb协议的返回格式
        'len
        data
        len
        data
        ..
        \n(最后是空行)
         '
        """
        ret = []
        read_index = 0
        while True:
            index = self.buf.find('\n', read_index)

            if index == -1:
                break

            #读取一行，len，表示下一次读取多少字节
            line = self.buf[read_index: index]
            #设置下一次的读取位置
            read_index = index + 1

            #可能是处理完一次response?
            if line.strip() == '':
                #复制余下的buf
                self.buf = self.buf[read_index:]
                return ret
            try:
                #接下来读取的字节数目
                num = int(line)
            except Exception, e:
                return []
            read_index = (read_index + num)

            #数据没有读够，继续读，再来处理
            if read_index > len(self.buf):
                break
                #读取的data
            data = self.buf[index + 1: read_index]

            ret.append(data)

            #skip数据末尾的换行
            index = self.buf.find('\n', read_index)

            #继续搞
            if index == - 1:
                break
            read_index += 1

        return None


class ConnectionPool(object):
    def __init__(self, host='127.0.0.1', port=8888, socket_timeout=None, max_connections=None):
        self.pid = os.getpid()
        self.host = host
        self.port = port
        self.socket_timeout = socket_timeout
        self.max_connections = max_connections
        self._created_connections = 0
        self._available_connections = []
        self._in_use_connections = set()

    def get_connection(self):
        self._check_pid()
        try:
            connection = self._available_connections.pop()
        except IndexError:
            connection = self.new_connection();
        self._in_use_connections.add(connection)
        return connection

    def release(self, connection):
        self._check_pid()
        #use this check if the connection belong to this pool
        if self.pid == connection.pid:
            self._in_use_connections.remove(connection)
            self._available_connections.append(connection)


    def new_connection(self):
        if self._created_connections >= self.max_connections:
            raise ConnectionError("Too many connections")
        self._created_connections += 1
        connection = Connection(self.host, self.port, self.socket_timeout)
        connection.connect()
        return connection

    def _close_pool(self):
        all_connections = chain(self._available_connections, self._in_use_connections)

        for conn in all_connections:
            conn.dis_connect()

    def _check_pid(self):
        if self.pid != os.getpid():
            self._close_pool()
            self.__init__(self, self.host, self.port, self.socket_timeout, self.max_connections)