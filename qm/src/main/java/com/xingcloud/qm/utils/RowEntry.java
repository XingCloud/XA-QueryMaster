package com.xingcloud.qm.utils;

import java.util.Map.Entry;

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
  public V setValue(V value) {
    V v = this.v;
    this.v = value;
    return v;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + ((k == null) ? 0 : k.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    RowEntry other = (RowEntry) obj;
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
