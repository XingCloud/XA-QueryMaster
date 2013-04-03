package com.xingcloud.qm.service;

import org.apache.hadoop.io.MapWritable;
import org.apache.hadoop.ipc.VersionedProtocol;

public interface QuerySlaveProtocol extends VersionedProtocol {

    public MapWritable query( String sql ) throws Exception;

}
