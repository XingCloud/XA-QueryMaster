package com.xingcloud.qm.service;

public interface Submit {
    public boolean submit( String sql );
    
    public boolean submit(String sql, String cacheKey);
}
