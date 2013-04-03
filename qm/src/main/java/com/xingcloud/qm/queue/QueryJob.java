package com.xingcloud.qm.queue;

import java.text.SimpleDateFormat;
import java.util.Date;

public class QueryJob {

    private String cacheKey;

    private String sql;

    private long timestamp;

    public QueryJob(String sql, long timestamp) {
        super();
        this.sql = sql;
        this.timestamp = timestamp;
    }

    public QueryJob(String cacheKey, String sql, long timestamp) {
        super();
        this.cacheKey = cacheKey;
        this.sql = sql;
        this.timestamp = timestamp;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result
                + ((cacheKey == null) ? 0 : cacheKey.hashCode());
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
        QueryJob other = (QueryJob) obj;
        if (cacheKey == null) {
            if (other.cacheKey != null)
                return false;
        } else if (!cacheKey.equals(other.cacheKey))
            return false;
        return true;
    }

    public String getSql() {
        return sql;
    }

    public void setSql( String sql ) {
        this.sql = sql;
    }

    public long getTimestamp() {
        return timestamp;
    }

    public void setTimestamp( long timestamp ) {
        this.timestamp = timestamp;
    }

    public String getCacheKey() {
        return cacheKey;
    }

    public void setCacheKey( String cacheKey ) {
        this.cacheKey = cacheKey;
    }

    @Override
    public String toString() {
        SimpleDateFormat sdf = new SimpleDateFormat("yyyy-MM-dd HH:mm:ss.SSS");
        return "QJ(" + sdf.format(new Date(timestamp)) + ")." + sql;
    }

}
