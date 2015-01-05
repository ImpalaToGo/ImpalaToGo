package com.cloudera.impala.catalog;

import java.net.URI;

import org.apache.hadoop.fs.FileSystem;

// Wrapper around a FileSystem object to hash based on the underlying FileSystem's
// scheme and authority.
public class FsKey {
  FileSystem filesystem;

  public FsKey(FileSystem fs) { filesystem = fs; }

  @Override
  public int hashCode() { return filesystem.getUri().hashCode(); }

  @Override
  public boolean equals(Object o) {
    if (o == this) return true;
    if (o != null && o instanceof FsKey) {
      URI uri = filesystem.getUri();
      URI otherUri = ((FsKey)o).filesystem.getUri();
      return uri.equals(otherUri);
    }
    return false;
  }

  @Override
  public String toString() { return filesystem.getUri().toString(); }
}
