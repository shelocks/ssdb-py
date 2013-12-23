import unittest
import ssdb
import time


class SSDBTest(unittest.TestCase):
    def setUp(self):
        self.ssdb = ssdb.SSDB('127.0.0.1', 8888)

    def test_set_with_ttl(self):
        r = self.ssdb.set("key1_ttl", "value1_ttl", 1)
        self.assertEqual("ok", r.code)
        r = self.ssdb.get("key1_ttl")
        self.assertEqual("value1_ttl", r.data)
        time.sleep(1)
        r = self.ssdb.get("key1_ttl")
        self.assertEqual("not_found", r.code)

    def test_scan_iter(self):
        data = {}
        for i in range(0, 100):
            key = "_".join(["scan_iter_key", str(i)])
            value = "_".join(["scan_iter_value", str(i)])
            data[key] = value
            self.ssdb.set(key, value)
        for item in self.ssdb.scan_iterator(""):
            self.assertEqual(data[item[0]], item[1])

    def test_del(self):
        self.ssdb.set("key1", "value1")
        r = self.ssdb.delete("key1")
        self.assertEqual("ok", r.code)
        r = self.ssdb.get("key1")
        self.assertEqual("not_found", r.code)


    def test_set(self):
        r = self.ssdb.set("key1", "value1")
        self.assertEqual("ok", r.code)
        self.assertEqual(1, r.data)
        r = self.ssdb.get("key1")
        self.assertEqual("ok", r.code)
        self.assertEqual("value1", r.data)
        self.ssdb.delete("key1")

    def test_get(self):
        self.ssdb.delete("key1")
        r = self.ssdb.get("key1")
        self.assertEqual("not_found", r.code)
        self.ssdb.set("key1", "value1")
        r = self.ssdb.get("key1")
        self.assertEqual("value1", r.data)
        self.ssdb.delete("key1")

    def test_incr(self):
        self.ssdb.delete("incr_key")
        self.ssdb.set("incr_key", 1)
        r = self.ssdb.incr("incr_key", 1)

        self.assertEqual('ok', r.code)
        self.assertEqual(2, r.data)
        self.ssdb.delete("incr_key")

    def test_decr(self):
        self.ssdb.delete("decr_key")
        self.ssdb.set("decr_key", 1024)
        r = self.ssdb.decr("decr_key", 1)
        self.assertEqual(1023, r.data)
        self.ssdb.delete("decr_key")

    def test_keys(self):
        self.ssdb.delete("b")
        self.ssdb.delete("c")
        self.ssdb.delete("d")

        self.ssdb.set("c", "c_value")
        self.ssdb.set("d", "d_value")

        r = self.ssdb.keys('b', 'd', 10)

        self.assertTrue('c' in r.data)
        self.assertTrue('d' in r.data)

        self.ssdb.delete("c")
        self.ssdb.delete("d")

    def test_scan(self):
        self.ssdb.delete("b")
        self.ssdb.delete("c")
        self.ssdb.delete("d")

        self.ssdb.set("c", "c_value")
        self.ssdb.set("d", "d_value")

        r = self.ssdb.scan("b", "d", 10)
        self.assertEqual("c_value", r.data["items"]["c"])
        self.assertEqual("d_value", r.data["items"]["d"])

        self.assertEqual(['c', 'd'], r.data["index"])

        self.ssdb.delete("c")
        self.ssdb.delete("d")

    def test_rscan(self):
        self.ssdb.delete("b")
        self.ssdb.delete("c")
        self.ssdb.delete("d")

        self.ssdb.set("c", "c_value")
        self.ssdb.set("d", "d_value")
        r = self.ssdb.rscan("e", "c", 10)

        self.assertEqual("c_value", r.data["items"]["c"])
        self.assertEqual("d_value", r.data["items"]["d"])

        self.assertEqual(['d', 'c'], r.data["index"])

        self.ssdb.delete("c")
        self.ssdb.delete("d")

    def test_multi_set(self):
        data = {"c": "c_value", "d": "d_value"}
        r = self.ssdb.get("c")
        self.assertEqual("not_found", r.code)
        r = self.ssdb.get("d")
        self.assertEqual("not_found", r.code)
        r = self.ssdb.multi_set(data)

        r = self.ssdb.get("c")
        self.assertEqual("c_value", r.data)
        r = self.ssdb.get("d")
        self.assertEqual("d_value", r.data)
        self.ssdb.delete("c")
        self.ssdb.delete("d")

    def test_multi_get(self):
        self.ssdb.set("c", "c_value")
        self.ssdb.set("d", "d_value")

        r = self.ssdb.multi_get(["c", "d"])

        self.assertEqual("c_value", r.data["items"]["c"])
        self.assertEqual("d_value", r.data["items"]["d"])
        self.ssdb.delete("c")
        self.ssdb.delete("d")

    def test_multi_del(self):
        self.ssdb.set("c", "c_value")
        self.ssdb.set("d", "d_value")

        r = self.ssdb.get("c")
        self.assertEqual("ok", r.code)
        r = self.ssdb.get("d")
        self.assertEqual("ok", r.code)

        r = self.ssdb.multi_del(["c", "d"])
        self.assertEqual(2, r.data)

        r = self.ssdb.get("c")
        self.assertEqual("not_found", r.code)
        r = self.ssdb.get("d")
        self.assertEqual("not_found", r.code)

    def test_hset(self):
        r = self.ssdb.hset("hset_1", "key_1", "value_1")
        self.assertEqual("ok", r.code)

    def test_hget(self):
        r = self.ssdb.hset("hset_1", "key_1", "value_1")
        self.assertEqual("ok", r.code)
        r = self.ssdb.hget("hset_1", "key_1")
        self.assertEqual("value_1", r.data)

    def test_hdel(self):
        r = self.ssdb.hset("hset_1", "key_1", "value_1")
        self.assertEqual("ok", r.code)
        r = self.ssdb.hget("hset_1", "key_1")
        self.assertEqual("value_1", r.data)

        r = self.ssdb.hdel("hset_1", "key_1")
        self.assertEqual("ok", r.code)
        r = self.ssdb.hget("hset_1", "key_1")
        self.assertEqual("not_found", r.code)

    def test_hincr(self):
        r = self.ssdb.hset("hset_incr", "key", 10)
        self.assertEqual("ok", r.code)
        r = self.ssdb.hincr("hset_incr", "key", 2)
        self.assertEqual(12, r.data)

    def test_hdecr(self):
        r = self.ssdb.hset("hset_decr", "key", 10)
        self.assertEqual("ok", r.code)
        r = self.ssdb.hdecr("hset_decr", "key", 2)
        self.assertEqual(8, r.data)

    def test_hsize(self):
        self.ssdb.hset("hset_size", "key", "value")
        self.ssdb.hset("hset_size", "key1", "value1")

        r = self.ssdb.hsize("hset_size")
        self.assertEqual(2, r.data)
        self.ssdb.hdel("hset_size", "key")
        self.ssdb.hdel("hset_size", "key1")

    def test_hlist(self):
        self.ssdb.hset("hset_list_1", "a", "value")
        self.ssdb.hset("hset_list_2", "b", "value1")
        self.ssdb.hset("hset_list_3", "c", "value2")

        r = self.ssdb.hlist("hset_list", "hset_list_4", 10)
        self.assertEqual(3, len(r.data))
        r = self.ssdb.hlist("hset_list_1", "hset_list_4", 10)
        self.assertEqual(2, len(r.data))

    def test_hkeys(self):
        self.ssdb.hset("hset_hkeys", "a", "value")
        self.ssdb.hset("hset_hkeys", "b", "value1")
        self.ssdb.hset("hset_hkeys", "c", "value2")

        r = self.ssdb.hkeys("hset_hkeys", "", "", 10)
        self.assertEqual(3, len(r.data))

    def test_hscan(self):
        self.ssdb.hset("hset_hscan", "a", "value")
        self.ssdb.hset("hset_hscan", "b", "value1")
        self.ssdb.hset("hset_hscan", "c", "value2")

        r = self.ssdb.hscan("hset_hscan", "a", "c", 10)

        self.assertEqual(['b', 'c'], r.data['index'])
        self.assertEqual("value1", r.data['items']['b'])
        self.assertEqual("value2", r.data['items']['c'])

    def test_hrscan(self):
        self.ssdb.hset("hset_hrscan", "a", "value")
        self.ssdb.hset("hset_hrscan", "b", "value1")
        self.ssdb.hset("hset_hrscan", "c", "value2")

        r = self.ssdb.hrscan("hset_hrscan", "c", "a", 10)

        self.assertEqual(['b', 'a'], r.data['index'])
        self.assertEqual("value1", r.data['items']['b'])
        self.assertEqual("value", r.data['items']['a'])

    def test_multi_hset(self):
        self.ssdb.hdel("multi_hset", "a")
        self.ssdb.hdel("multi_hset", "b")
        self.ssdb.hdel("multi_hset", "c")
        m = {"a": "value", "b": "value1", "c": "value2"}
        r = self.ssdb.multi_hset("multi_hset", m)
        self.assertEqual(3, r.data)
        r = self.ssdb.hget("multi_hset", "a")
        self.assertEqual("value", r.data)

    def test_multi_hget(self):
        m = {"a": "value", "b": "value1", "c": "value2"}
        r = self.ssdb.multi_hset("multi_hget", m)
        r = self.ssdb.multi_hget("multi_hget", ['a', 'b', 'c'])

        self.assertEqual(3, len(r.data['index']))
        self.assertEqual(['a', 'b', 'c'], r.data['index'])
        self.assertEqual("value", r.data['items']['a'])
        self.assertEqual("value1", r.data['items']['b'])
        self.assertEqual("value2", r.data['items']['c'])

    def test_multi_hdel(self):
        m = {"a": "value", "b": "value1", "c": "value2"}
        r = self.ssdb.multi_hset("multi_hdel", m)
        r = self.ssdb.multi_hget("multi_hdel", ['a', 'b', 'c'])

        self.assertEqual(3, len(r.data['index']))
        self.assertEqual(['a', 'b', 'c'], r.data['index'])
        self.assertEqual("value", r.data['items']['a'])
        self.assertEqual("value1", r.data['items']['b'])
        self.assertEqual("value2", r.data['items']['c'])

        self.ssdb.multi_hdel("multi_hdel", ['a', 'b', 'c'])
        r = self.ssdb.multi_hget("multi_hdel", ['a', 'b', 'c'])

        self.assertEqual(0, len(r.data['index']))

    def test_zset(self):
        r = self.ssdb.zset("zset_key", "key", 100)

        self.assertEqual("ok", r.code)

    def test_zget(self):
        r = self.ssdb.zset("zget_key", "key", 100)
        self.assertEqual("ok", r.code)

        r = self.ssdb.zget("zget_key", "key")
        self.assertEqual(100, r.data)

    def test_zdel(self):
        r = self.ssdb.zset("zdel_key", "key", 100)
        self.assertEqual("ok", r.code)

        self.ssdb.zdel("zdel_key", "key")

        r = self.ssdb.zget("zdel_key", "key")
        self.assertEqual("not_found", r.code)

    def test_zincr(self):
        r = self.ssdb.zset("zincr_key", "key", 100)
        self.ssdb.zincr("zincr_key", "key", 1)
        r = self.ssdb.zget("zincr_key", "key")

        self.assertEqual(101, r.data)

    def test_zsize(self):
        self.ssdb.zset("zsize_key", "key", 100)
        self.ssdb.zset("zsize_key", "key1", 1)

        r = self.ssdb.zsize("zsize_key")
        self.assertEqual(2, r.data)

    def test_zlist(self):
        self.ssdb.zset("a", "key", 1)
        self.ssdb.zset("b", "key", 2)
        self.ssdb.zset("c", "key", 3)

        r = self.ssdb.zlist("a", "c", 10)
        self.assertEqual(2, len(r.data))
        self.assertEqual(['b', 'c'], r.data)

    def test_zkeys(self):
        self.ssdb.zset("zkeys_key", 'a', 100)
        self.ssdb.zset("zkeys_key", 'b', 1)
        self.ssdb.zset("zkeys_key", 'c', 40)
        self.ssdb.zset("zkeys_key", 'd', 1)

        r = self.ssdb.zkeys("zkeys_key", 'b', 1, 200, 10)
        self.assertEqual(['d', 'c', 'a'], r.data)
        r = self.ssdb.zkeys("zkeys_key", '', '', '', 10)
        self.assertEqual(['b', 'd', 'c', 'a'], r.data)

    def test_zscan(self):
        self.ssdb.zset("zscan_key", 'a', 100)
        self.ssdb.zset("zscan_key", 'b', 1)
        self.ssdb.zset("zscan_key", 'c', 40)
        self.ssdb.zset("zscan_key", 'd', 1)

        r = self.ssdb.zscan("zscan_key", 'b', 1, 200, 10)
        self.assertEqual(['d', 'c', 'a'], r.data['index'])

    def test_zrscan(self):
        self.ssdb.zset("zrscan_key", 'a', 100)
        self.ssdb.zset("zrscan_key", 'b', 1)
        self.ssdb.zset("zrscan_key", 'c', 40)
        self.ssdb.zset("zrscan_key", 'd', 1)
        self.ssdb.zset("zrscan_key", 'e', 100)

        r = self.ssdb.zrscan("zrscan_key", 'e', 100, 1, 100)
        self.assertEqual(['a', 'c', 'd', 'b'], r.data['index'])

    def test_multi_zset(self):
        self.ssdb.multi_zset("multi_zset", {'key1': 1, 'key2': 2})

        r = self.ssdb.zget("multi_zset", 'key1')
        self.assertEqual(1, r.data)
        r = self.ssdb.zget("multi_zset", 'key2')
        self.assertEqual(2, r.data)

    def test_multi_zget(self):
        self.ssdb.multi_zset("multi_zget", {'key1': 1, 'key2': 2})

        r = self.ssdb.multi_zget("multi_zget", ['key1', 'key2'])

        self.assertEqual(2, len(r.data['index']))
        self.assertEqual(1, r.data['items']['key1'])
        self.assertEqual(2, r.data['items']['key2'])

    def test_multi_zdel(self):
        self.ssdb.multi_zset("multi_zdel", {'key1': 1, 'key2': 2})
        r = self.ssdb.multi_zget("multi_zdel", ['key1', 'key2'])

        self.assertEqual(2, len(r.data['index']))

        r = self.ssdb.multi_zdel("multi_zdel", ['key1', 'key2'])

        self.assertEqual(2, r.data)

        r = self.ssdb.zget("multi_zdel", 'key1')
        self.assertEqual('not_found', r.code)
        r = self.ssdb.zget("multi_zdel", 'key2')
        self.assertEqual('not_found', r.code)


if __name__ == '__main__':
    unittest.main()
