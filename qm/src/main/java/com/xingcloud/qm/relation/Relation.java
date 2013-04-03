package com.xingcloud.qm.relation;

import java.io.Serializable;
import java.util.Collection;
import java.util.Iterator;

public interface Relation extends Serializable {

    public int size();

    public Collection<Row> rows();

    public Iterator<Row> iterator();
}
