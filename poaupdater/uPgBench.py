import time
import uSysDB

class PgBench:
    def __init__(self, con):
        self.con = con

    def _loop(self, timeout, query, chunk, docommit):
        if not chunk:
            chunk = 100
        begin = time.time()
        end = begin + timeout
        loops = 0

        cur = self.con.cursor()

        uSysDB.set_verbose(False)
        try:
            while time.time() < end:
                i = 0
                while i < chunk:
                    cur.execute(query)
                    if docommit:
                        self.con.commit()
                    i = i + 1
                loops += chunk
        finally:
            uSysDB.set_verbose(True)

        end = time.time()
        if end == begin:
            return 0
        return int(loops / (end - begin))

    def test(self, testcase, timeout=5, minscore=None, chunk=None):
        score, minscore, msg, metrics = getattr(self, testcase)(timeout, minscore, chunk)
        ret = "%-50s: %5d %s" % (msg, score, metrics)
        ret = "%65s - must be %d, " % (ret, minscore)
        if score < minscore / 4:
            return False, ret + "VERY SLOW"
        if score < minscore:
            return False, ret + "SLOW"
        if score > minscore * 2:
            return True, ret + "VERY GOOD"
        return True, ret + "GOOD"

    def sequentialSelect(self, timeout, minscore, chunk):
        if not minscore:
            minscore = 4000
        rate = self._loop(timeout, "SELECT 1", chunk, False)
        return rate, minscore, "sequential select test 'SELECT 1'", "selects/sec"

    def sequentialCommit(self, timeout, minscore, chunk):
        if not minscore:
            minscore = 1000
        cur = self.con.cursor()
        value = 'a' * 255

        table = "postgresql_commit_benchmark"
        cur.execute("DROP TABLE IF EXISTS %s" % table)
        cur.execute("CREATE TABLE %s (test_column varchar(256))" % table)
        self.con.commit()
        try:
            rate = self._loop(timeout, "INSERT INTO %s (test_column) VALUES ('%s')" % (table, value), chunk, True)
        finally:
            cur.execute("DROP TABLE IF EXISTS %s" % table)
            self.con.commit()
        return rate, minscore, "sequential commit test 'BEGIN; INSERT ...; COMMIT'", "commits/sec"

