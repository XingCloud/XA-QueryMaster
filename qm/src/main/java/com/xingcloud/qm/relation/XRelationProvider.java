package com.xingcloud.qm.relation;

public class XRelationProvider implements RelationProvider {

    private static final long serialVersionUID = -616500146796386940L;

    @Override
    public Relation getRelation() {
        return new XRelation();
    }

}
