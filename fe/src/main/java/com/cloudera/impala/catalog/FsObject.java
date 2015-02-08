package com.cloudera.impala.catalog;

import java.util.concurrent.ConcurrentHashMap;

import org.apache.commons.lang.builder.EqualsBuilder;
import org.apache.commons.lang.builder.HashCodeBuilder;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.Path;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.cloudera.impala.util.FsKey;

/** Represents File System object.
 *  Designed to cache the unit of remote file system state
 */
public class FsObject {

 /** Describe the file system object state from cache perspective */
  public enum ObjectState{
    /** was not synchronized for state with origin */
    NA,
    /** exists on origin */
    EXISTS_ORIGIN,
    /** does not exist on origin */
    DOES_NOT_EXIST_ORIGIN,
    /** failure during sync on origin (retry is required) */
    SYNC_FAILURE,
    /** flag, indicates that current object was synchronized successfully */
    SYNC_OK,
  }

  /** cached object type. Mostly for logging */
  public enum ObjectType{
    FILE,
    DIRECTORY
  }

  /** Logging mechanism */
  private final static Logger LOG = LoggerFactory.getLogger(FsObject.class);

  /** represents the directory tree node */
  private final ConcurrentHashMap<String, FsObject> _children =
      new ConcurrentHashMap<String, FsObject>();

  /** represents Path metadata, for file or for directory */
  private FileStatus _metadata;

  /** file system object state - from cache perspective */
  private ObjectState _state;

  /** file system object type */
  private ObjectType _type;

  /** remote FileSystem associated with this object */
  private final FsKey _fileSystem;

  /** remote file system object path */
  private final Path _path;

  /** flag, indicates that remote object exists */
  private boolean _exists;

  /**
   * constructs the file system object from its path within the given filesystem, the object of known type
   *
   * @param filesystem - origin filesystem
   * @param path       - path within the filesystem
   * @param type       - object type, directory or file
   */
  public FsObject(FsKey filesystem, Path path, ObjectType type) {
    this(filesystem, path);
    _type = type;
  }

  /** constructs the file system object from its path within the given filesystem
   *
   * @param filesystem - origin filesystem
   * @param path       - path within the filesystem
   */
  public FsObject(FsKey filesystem, Path path){
    _fileSystem = filesystem;
    _path = path;
    // say state is "no acknowledge"
    _state = ObjectState.NA;
    _exists = false;
  }

  /**
   * Getter for file system object state
   * @return object state
   */
  public ObjectState getState(){
    return _state;
  }

  /**
   * Setter for file system object state
   * @param state - state to associate with the cached file system object
   */
  public void setState(ObjectState state){
    _state = state;
    if(state.equals(ObjectState.EXISTS_ORIGIN))
      _exists = true;
  }

  /**
   * Getter for file system object metadata
   * @return return file system object metadata
   */
  public FileStatus getMetadata(){
    return _metadata;
  }

  /**
   * Retrieves all children metadata, result of list operation
   *
   * @return directory's children files metadata if this is directory, null otherwise
   */
  public FileStatus[] getChildrenMetadata(){
    int size = _children.size();
    FileStatus[] statistics = size > 0 ? new FileStatus[_children.size()] : null;
    int i = 0;
    for (FsObject obj : _children.values()) {
         statistics[i++] = obj.getMetadata();
    }
    return statistics;
  }

  /**
   * get single child metadata using the path as a key to specify the child
   *
   * @param path - path to use to distinguish the child required
   * @return statistics for child whether one found, null otherwise
   */
  public FileStatus getChildMetadata(String path){
    if(_children.containsKey(path))
      return _children.get(path).getMetadata();
    return null;
  }

  /**
   * Setter for directory metadata (children metadata)
   * @param listStatus - result of list operation to associate with the cached DIRECTORY file system object
   */
  public void setMetadata(FileStatus[] listStatus){
    for(int i = 0; i < listStatus.length; i++){
      // assign own filesystem as origin for siblings
      FsObject obj = new FsObject(_fileSystem, listStatus[i].getPath(),
          listStatus[i].isDirectory() ? ObjectType.DIRECTORY : ObjectType.FILE);
      obj.setMetadata(listStatus[i]);
      obj.setState(ObjectState.SYNC_OK);

      obj = _children.putIfAbsent(obj.getPath().toString(), obj);

      // if object was present within the cache, it should be updated with the state and file status
      if(obj != null){
        LOG.info("Fs object with \"" + obj.getPath().toString() + "\" was present previously in the cache");
        // set those statistics that were provided:
        if(listStatus[i] != null)
          obj.setMetadata(listStatus[i]);
      }
      else{
        LOG.info("new Fs object with \"" + listStatus[i].getPath() + "\" is added among children of\"" + _path + "\"");
      }
    }
    _type = ObjectType.DIRECTORY;
  }

  /**
   * Setter for file system object metadata (file)
   * @param fileStatus - result of "get file status" operation to associate with the cached FILE file system object
   */
  public void setMetadata(FileStatus fileStatus){
    _metadata = fileStatus;
    _type = ObjectType.FILE;
  }

  /**
   * getter for file system object type
   * @return file system object type
   */
  public ObjectType getType(){
    return _type;
  }

  /**
   * Getter for file system object path (within origin)
   * @return file system object path
   */
  public Path getPath(){
    return _path;
  }

  /**
   * Getter for object origin filesystem
   * @return filesystem associated with the object
   */
  public FsKey getFilesystem(){
    return _fileSystem;
  }

  /**
   * Getter for "object exists" statistic
   * @return true if object was approved as existing on the remote part
   */
  public boolean getExistance(){
    return _exists;
  }

  @Override
  public boolean equals(Object object){
    if (!(object instanceof FsObject))
      return false;
    if (object == this)
      return true;

    FsObject rhs = (FsObject) object;
    return new EqualsBuilder().
      append(_path, rhs._path).
      append(_fileSystem, rhs._fileSystem).
      isEquals();
  }

  @Override
  public int hashCode() {
      return new HashCodeBuilder(17, 31). // two randomly chosen prime numbers
          append(_path.hashCode()).
          append(_fileSystem.hashCode()).
          toHashCode();
  }

  /** Triggers the object reload */
  public ObjectState reload(){
    return _state;
  }
}
