package com.xingcloud.qm.service;

import static com.xingcloud.basic.Constants.SEPERATOR_CHAR_CACHE_CONTENT;
import static com.xingcloud.qm.utils.QueryMasterCommonUtils.printRelations;
import static com.xingcloud.qm.utils.QueryMasterConstant.COUNT;
import static com.xingcloud.qm.utils.QueryMasterConstant.G;
import static com.xingcloud.qm.utils.QueryMasterConstant.GROUP;
import static com.xingcloud.qm.utils.QueryMasterConstant.INNER_HASH_KEY;
import static com.xingcloud.qm.utils.QueryMasterConstant.META_KEY;
import static com.xingcloud.qm.utils.QueryMasterConstant.SIZE_KEY;
import static com.xingcloud.qm.utils.QueryMasterConstant.SUM;
import static com.xingcloud.qm.utils.QueryMasterConstant.USER_NUM;
import static com.xingcloud.qm.utils.RoleUtils.provideWorkers;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphabetic;
import static org.apache.commons.lang3.RandomStringUtils.randomAlphanumeric;
import static org.apache.commons.lang3.RandomStringUtils.randomNumeric;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Random;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;

import org.apache.commons.collections.CollectionUtils;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.hadoop.io.ArrayWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.thirdparty.guava.common.base.Strings;
import org.apache.log4j.Logger;

import com.xingcloud.cache.MappedXCache;
import com.xingcloud.qm.exceptions.XRemoteQueryException;
import com.xingcloud.qm.queue.QueryJob;
import com.xingcloud.qm.queue.QueueContainer;
import com.xingcloud.qm.redis.CachePutQueue;
import com.xingcloud.qm.thread.XQueryNodeExecutorServiceProvider;

public class QueryBroker implements Runnable {

    public final class RowEntry<K, V> implements Entry<K, V> {

        private final K k;

        private V v;

        public RowEntry(K k) {
            super();
            this.k = k;
        }

        public RowEntry(K k, V v) {
            super();
            this.k = k;
            this.v = v;
        }

        @Override
        public K getKey() {
            return this.k;
        }

        @Override
        public V getValue() {
            return this.v;
        }

        @Override
        public V setValue( V value ) {
            V v = this.v;
            this.v = value;
            return v;
        }

        private QueryBroker getOuterType() {
            return QueryBroker.this;
        }

        @Override
        public int hashCode() {
            final int prime = 31;
            int result = 1;
            result = prime * result + getOuterType().hashCode();
            result = prime * result + ((k == null) ? 0 : k.hashCode());
            return result;
        }

        @Override
        public boolean equals( Object obj ) {
            if (this == obj)
                return true;
            if (obj == null)
                return false;
            if (getClass() != obj.getClass())
                return false;
            RowEntry other = (RowEntry) obj;
            if (!getOuterType().equals(other.getOuterType()))
                return false;
            if (k == null) {
                if (other.k != null)
                    return false;
            } else if (!k.equals(other.k))
                return false;
            return true;
        }

        @Override
        public String toString() {
            return "(" + k + ":" + v.getClass().getName() + ")";
        }

    }

    private static final Logger LOGGER = Logger.getLogger(QueryBroker.class);

    private RowEntry<String, Object[]> toRowEntry( String[] row,
            int[] metaInfoArray ) {
        if (ArrayUtils.isEmpty(row)) {
            return null;
        }
        if (ArrayUtils.isEmpty(metaInfoArray)) {
            return null;
        }

        String k = null;
        StringBuilder sb = new StringBuilder();
        String cell = null;
        Object[] valueArray = null;

        for( int i = 0; i < row.length; i++ ) {
            cell = row[i];
            if (valueArray == null) {
                valueArray = new Object[metaInfoArray.length];
            }

            if (metaInfoArray[i] == GROUP) {
                sb.append(cell);
                valueArray[i] = cell;
            } else {
                valueArray[i] = Long.parseLong(cell);
            }
        }
        k = sb.toString();
        RowEntry<String, Object[]> re = new RowEntry<String, Object[]>(k,
                valueArray);
        return re;
    }

    private void mergeArray( Object[] a1, Object[] a2, int[] metaInfoArray ) {
        if (ArrayUtils.isEmpty(a2)) {
            return;
        }
        if (ArrayUtils.isEmpty(a1)) {
            a1 = a2;
        }
        for( int i = 0; i < metaInfoArray.length; i++ ) {
            if (metaInfoArray[i] == GROUP) {
                continue;
            }
            a1[i] = (Long) a1[i] + (Long) a2[i];
        }
    }

    @Deprecated
    /**
     * Not supported in current version.
     * @param parts
     * @return
     */
    private MapWritable unionProjection( List<MapWritable> parts ) {
        MapWritable union = new MapWritable();
        int allSize = 0;
        long cnt = 0;
        LongWritable sizeWritable = null;
        Writable metaWritable = null;
        Writable row = null;
        LongWritable longWritable = new LongWritable();
        long size = 0;
        for( MapWritable part: parts ) {
            sizeWritable = (LongWritable) part.get(SIZE_KEY);
            if (sizeWritable == null) {
                continue;
            }
            size = sizeWritable.get();
            if (size == 0) {
                continue;
            }

            allSize += size;
            for( long i = 0; i < size; i++ ) {
                longWritable = new LongWritable(i);
                row = part.get(longWritable);
                if (row == null) {
                    continue;
                }
                longWritable = new LongWritable(cnt);
                union.put(longWritable, row);
                ++cnt;
            }
        }
        union.put(SIZE_KEY, new LongWritable(allSize));
        return union;
    }

    private MapWritable unionAggr( List<MapWritable> parts ) {
        MapWritable union = new MapWritable();
        LongWritable sizeWritable = null;
        Writable metaWritable = null;
        ArrayWritable row = null;
        long size = 0;
        String[] stringArray = null;
        long[] l = null;
        for( MapWritable part: parts ) {
            sizeWritable = (LongWritable) part.get(SIZE_KEY);
            if (sizeWritable == null) {
                continue;
            }
            size = sizeWritable.get();
            if (size == 0) {
                continue;
            }

            row = (ArrayWritable) part.get(new LongWritable(0));
            stringArray = row.toStrings();
            if (l == null) {
                l = new long[stringArray.length];
            }
            for( int i = 0; i < stringArray.length; i++ ) {
                l[i] += Long.valueOf(stringArray[i]);
            }
        }
        stringArray = new String[l.length];
        for( int i = 0; i < l.length; i++ ) {
            stringArray[i] = String.valueOf(l[i]);
        }
        row = new ArrayWritable(stringArray);
        union.put(SIZE_KEY, new LongWritable(1));
        union.put(new LongWritable(0), row);
        return union;
    }

    private MapWritable unionProjectionAggr( List<MapWritable> parts,
            int[] metaInfoArray ) {
        MapWritable union = new MapWritable();

        LongWritable longWritable = null;

        long size = 0;
        Map<String, Object[]> map = new HashMap<String, Object[]>();
        ArrayWritable row = null;
        RowEntry<String, Object[]> re = null;
        String k = null;
        Object[] v = null;
        Writable metaWritable = null;

        for( MapWritable part: parts ) {
            longWritable = (LongWritable) part.get(SIZE_KEY);
            if (longWritable == null) {
                continue;
            }
            size = longWritable.get();
            if (size == 0) {
                continue;
            }
            for( int i = 0; i < size; i++ ) {
                row = (ArrayWritable) part.get(new LongWritable(i));
                re = toRowEntry(row.toStrings(), metaInfoArray);
                k = re.getKey();
                v = map.get(k);
                if (v == null) {
                    map.put(k, re.getValue());
                    continue;
                }
                mergeArray(v, re.getValue(), metaInfoArray);
            }
        }
        union.put(SIZE_KEY, new LongWritable(map.size()));

        long l = 0;
        String[] stringArray = null;
        for( Entry<String, Object[]> entry: map.entrySet() ) {
            v = entry.getValue();

            stringArray = new String[v.length];
            for( int i = 0; i < v.length; i++ ) {
                stringArray[i] = String.valueOf(v[i]);
            }
            row = new ArrayWritable(stringArray);
            union.put(new LongWritable(l), row);
            ++l;
        }
        return union;
    }

    /*
     * 
     * @param parts
     * @param infoArray
     * @return
     * 3 kinds
     *  1. Only relations select a, b, c from
     *  2. Only aggrs select count(1), sum(a) from
     *  3. Both relations and aggrs select a, count ( distinct b) from
     */
    private MapWritable union( List<MapWritable> parts, String sql,
            int[] metaInfoArray ) {
        if (Strings.isNullOrEmpty(sql)) {
            return null;
        }
        if (CollectionUtils.isEmpty(parts)) {
            return null;
        }
        int groupByIndex = sql.indexOf(G);
        boolean hasGroupBy = groupByIndex >= 0;
        MapWritable union = hasGroupBy ? unionProjectionAggr(parts,
                metaInfoArray) : unionAggr(parts);
        return union;
    }

    private static int[] parseSqlMeta( String sql, boolean hasGroupBy ) {
        String selectFrom = sql.substring(6, sql.indexOf("from"));
        String[] arr = selectFrom.split(",");
        int length = hasGroupBy ? arr.length + 1 : arr.length;
        int[] result = new int[length];
        int cnt = 0;

        for( String s: arr ) {
            s = s.trim();
            if (s.startsWith("count") && !s.contains("distinct")) {
                result[cnt] = COUNT;
            } else if (s.startsWith("sum")) {
                result[cnt] = SUM;
            } else if (s.startsWith("count") && s.contains("distinct")) {
                result[cnt] = USER_NUM;
            } else {
                continue;
            }
            ++cnt;
        }
        if (hasGroupBy) {
            result[length - 1] = GROUP;
        }

        return result;
    }

    private MapWritable query( String sql, int[] metaInfoArray )
            throws XRemoteQueryException {
        ExecutorService service = XQueryNodeExecutorServiceProvider
                .getService();
        List<QueryWorker> workers = provideWorkers(sql);
        List<Future<MapWritable>> workList = new ArrayList<Future<MapWritable>>(
                workers.size());
        Future<MapWritable> undoneWork = null;
        for( QueryWorker worker: workers ) {
            undoneWork = service.submit(worker);
            workList.add(undoneWork);
        }

        MapWritable mw = null;

        long before = 0;
        long after = 0;

        long eclapsed = 0;
        long wait = 60 * 1000;
        long remain = wait;
        List<MapWritable> rpList = null;

        try {
            for( Future<MapWritable> f: workList ) {
                before = System.currentTimeMillis();
                mw = f.get(remain, TimeUnit.MILLISECONDS);
                after = System.currentTimeMillis();

                eclapsed += after - before;
                remain = wait - eclapsed;

                if (rpList == null) {
                    rpList = new ArrayList<MapWritable>();
                }
                rpList.add(mw);
            }
        } catch (TimeoutException e) {
            throw new XRemoteQueryException(e);
        } catch (Exception e) {
            throw new XRemoteQueryException(e);
        }
        MapWritable union = union(rpList, sql, metaInfoArray);
        LOGGER.info("[BROKER] - Sql job done - " + sql);
        return union;
    }

    @Override
    public void run() {
        QueueContainer qc = QueueContainer.getInstance();
        QueryJob job = null;
        MapWritable queryResult = null;
        MappedXCache mxc = null;
        int[] metaInfoArray = null;
        String cacheKey = null;
        String sql = null;
        boolean hasGroupBy = false;
        try {
            while (true) {
                job = qc.take();
                if (job == null) {
                    continue;
                }
                cacheKey = job.getCacheKey();
                sql = job.getSql();

                hasGroupBy = sql.indexOf(G) >= 0;
                metaInfoArray = parseSqlMeta(sql, hasGroupBy);
                try {
                    queryResult = query(sql, metaInfoArray);
                } catch (XRemoteQueryException e) {
                    e.printStackTrace();
                    continue;
                }
                mxc = converetCache(cacheKey, queryResult, metaInfoArray);
                CachePutQueue.getInstance().putQueue(mxc);
            }
        } catch (InterruptedException e) {
            e.printStackTrace();
        } finally {
            Thread.currentThread().interrupt();
        }
    }

    public static void testUnionProjectionAggr() {
        String sql = "select count(b), sum(d), count(distinct f) from test group by uid";
        int[] metaInfoArray = parseSqlMeta(sql, true);
        System.out.println(Arrays.toString(metaInfoArray));
        int shards = 3;
        List<MapWritable> parts = new ArrayList<MapWritable>();
        MapWritable mw = null;
        LongWritable lw = null;
        int randomCnt = 0;
        ArrayWritable aw = null;

        for( int i = 0; i < shards; i++ ) {
            mw = new MapWritable();
            randomCnt = 3;
            mw.put(SIZE_KEY, new LongWritable(randomCnt));

            for( int j = 0; j < randomCnt; j++ ) {
                lw = new LongWritable(j);
                aw = new ArrayWritable(
                        new String[] { randomNumeric(1), randomNumeric(1),
                                randomNumeric(1), randomAlphabetic(1) });
                mw.put(lw, aw);
            }
            parts.add(mw);
            printRelations(mw);
        }

        QueryBroker qb = new QueryBroker();
        MapWritable union = qb.unionProjectionAggr(parts, metaInfoArray);
        printRelations(union);
    }

    public static void testUnionAggr() {
        int shards = 3;
        List<MapWritable> parts = new ArrayList<MapWritable>();
        MapWritable mw = null;
        LongWritable lw = null;
        ArrayWritable aw = null;
        for( int i = 0; i < shards; i++ ) {
            mw = new MapWritable();
            mw.put(SIZE_KEY, new LongWritable(1));

            for( int j = 0; j < 1; j++ ) {
                lw = new LongWritable(j);
                aw = new ArrayWritable(new String[] { randomNumeric(2),
                        randomNumeric(3), randomNumeric(4) });
                mw.put(lw, aw);
            }
            parts.add(mw);
            printRelations(mw);
        }
        QueryBroker qb = new QueryBroker();
        MapWritable union = qb.unionAggr(parts);
        printRelations(union);
    }

    private static MappedXCache converetCache( String key, MapWritable mw,
            int[] metaInfoArray ) {

        Map<String, String> valueMap = new HashMap<String, String>(mw.size());
        mw.remove(SIZE_KEY);
        mw.remove(META_KEY);
        ArrayWritable aw = null;
        String[] valueArray = null;

        Long count = null;
        Long sum = null;
        Long userNum = null;
        String innerKey = null;
        StringBuilder sb = null;
        for( Entry<Writable, Writable> entry: mw.entrySet() ) {
            aw = (ArrayWritable) entry.getValue();
            valueArray = aw.toStrings();

            for( int i = 0; i < metaInfoArray.length; i++ ) {
                switch (metaInfoArray[i]) {
                case COUNT:
                    count = Long.valueOf(valueArray[i]);
                    break;
                case SUM:
                    sum = Long.valueOf(valueArray[i]);
                    break;
                case USER_NUM:
                    userNum = Long.valueOf(valueArray[i]);
                    break;
                case GROUP:
                    innerKey = valueArray[i];
                    break;
                default:
                    break;
                }
                if (Strings.isNullOrEmpty(innerKey)) {
                    innerKey = INNER_HASH_KEY;
                }
            }
            sb = new StringBuilder();
            if (count != null) {
                sb.append(count);
            }
            sb.append(SEPERATOR_CHAR_CACHE_CONTENT);
            if (sum != null) {
                sb.append(sum);
            }
            sb.append(SEPERATOR_CHAR_CACHE_CONTENT);
            if (userNum != null) {
                sb.append(userNum);
            }
            sb.append(SEPERATOR_CHAR_CACHE_CONTENT);
            sb.append(1.0d);
            valueMap.put(innerKey, sb.toString());
        }
        MappedXCache mxc = new MappedXCache(key, valueMap,
                System.currentTimeMillis());
        return mxc;
    }

    /**
     * @deprecated Not supported in current version
     */
    public static void testUnionProjection() {
        int shards = 3;
        List<MapWritable> parts = new ArrayList<MapWritable>();
        MapWritable mw = null;
        LongWritable lw = null;
        Random r = new Random();
        int randomCnt = 0;
        ArrayWritable aw = null;
        for( int i = 0; i < shards; i++ ) {
            mw = new MapWritable();
            randomCnt = r.nextInt(10);
            mw.put(SIZE_KEY, new LongWritable(randomCnt));

            for( int j = 0; j < randomCnt; j++ ) {
                lw = new LongWritable(j);
                aw = new ArrayWritable(new String[] { randomAlphanumeric(5),
                        randomAlphanumeric(5), randomAlphanumeric(5) });
                mw.put(lw, aw);
            }
            parts.add(mw);
            printRelations(mw);
        }
        QueryBroker qb = new QueryBroker();
        MapWritable union = qb.unionProjection(parts);
        printRelations(union);
    }

    private static void testSqlParse() {
        String sql = "select count(1) from a";
        System.out.println(Arrays.toString(parseSqlMeta(sql, false)));
        sql = "select count(1), sum(a.b) from a";
        System.out.println(Arrays.toString(parseSqlMeta(sql, false)));
        sql = "select count(1), sum(a.b), count(distinct a.c) from a";
        System.out.println(Arrays.toString(parseSqlMeta(sql, false)));
        sql = "select count(1), count(2), sum(a.b), count(distinct a.c) from a";
        System.out.println(Arrays.toString(parseSqlMeta(sql, false)));
        sql = "select count(1), count(2), sum(a.b), count(distinct a.c) from a group by uid";
        System.out.println(Arrays.toString(parseSqlMeta(sql, true)));
    }

    public static void main( String[] args ) {
        testUnionProjectionAggr();
    }
}
