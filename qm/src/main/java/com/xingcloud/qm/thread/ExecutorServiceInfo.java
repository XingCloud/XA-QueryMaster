package com.xingcloud.qm.thread;

public class ExecutorServiceInfo {
    private String id;

    private String name;

    private int threadCount;

    public ExecutorServiceInfo(String id, String name, int threadCount) {
        super();
        this.id = id;
        this.name = name;
        this.threadCount = threadCount;
    }

    public String getId() {
        return id;
    }

    public void setId( String id ) {
        this.id = id;
    }

    public String getName() {
        return name;
    }

    public void setName( String name ) {
        this.name = name;
    }

    public int getThreadCount() {
        return threadCount;
    }

    public void setThreadCount( int threadCount ) {
        this.threadCount = threadCount;
    }

    @Override
    public int hashCode() {
        final int prime = 31;
        int result = 1;
        result = prime * result + ((id == null) ? 0 : id.hashCode());
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
        ExecutorServiceInfo other = (ExecutorServiceInfo) obj;
        if (id == null) {
            if (other.id != null)
                return false;
        } else if (!id.equals(other.id))
            return false;
        return true;
    }

    @Override
    public String toString() {
        return "ThreadFactoryInfo(id=" + id + ", name=" + name
                + ", threadCount=" + threadCount + ")";
    }

}
