/**
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.cloudera.llama.am.cache;

import com.cloudera.llama.am.api.RMResource;
import com.cloudera.llama.am.api.Resource;
import com.cloudera.llama.util.ParamChecker;
import com.cloudera.llama.util.UUID;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.NavigableMap;
import java.util.TreeMap;

public class ResourceStore {
  private static final Logger LOG = LoggerFactory.getLogger(ResourceStore.class);

  private final NavigableMap<Key, List<Entry>> store;
  private final Map<UUID, Entry> idToEntryMap;

  public ResourceStore() {
    store = new TreeMap<Key, List<Entry>>();
    idToEntryMap = new HashMap<UUID, Entry>();
  }

  public synchronized void add(Entry entry) {
    entry.setValid(true);
    idToEntryMap.put(entry.getResourceId(), entry);
    Key key = new Key(entry);
    List<Entry> list = store.get(key);
    if (list == null) {
      list = new ArrayList<Entry>();
      store.put(key, list);
    }
    int idx = Collections.binarySearch(list, entry);
    if (idx >= 0) {
      list.add(idx, entry);
    } else {
      list.add(- (idx + 1), entry);
    }
  }

  private enum Mode {SAME_REF, STRICT_LOCATION, ANY_LOCATION}

  private Entry findAndRemove(Entry entry, Mode mode) {
    ParamChecker.notNull(entry, "entry");
    ParamChecker.notNull(mode, "mode");
    Entry found = null;
    Key key = new Key(entry);
    Map.Entry<Key, List<Entry>> cacheEntry = store.ceilingEntry(key);
    List<Entry> list = (cacheEntry != null) ? cacheEntry.getValue() : null;
    if (list != null) {
      int idx = Collections.binarySearch(list, entry);
      if (idx >= 0) {
        switch (mode) {
          case SAME_REF:
            for (int i = idx; i < list.size() && entry.compareTo(list.get(i)) == 0; i++) {
              if (entry == list.get(i)) {
                found = entry;
                list.remove(i);
                break;
              }
            }
            break;
          case STRICT_LOCATION:
          case ANY_LOCATION:
            found = list.remove(idx);
            break;
        }
      } else {
        switch (mode) {
          case SAME_REF:
            throw new RuntimeException("Inconsistent state");
          case STRICT_LOCATION:
            break;
          case ANY_LOCATION:
            if ( -(idx + 1) <= list.size()) {
              found = list.remove(- (idx + 1) - 1);
            }
            break;
        }
      }
      if (found != null) {
        idToEntryMap.remove(found.getResourceId());
        if (list.isEmpty()) {
          store.remove(key);
        }
        found.setValid(false);
      }
    }
    return found;
  }

  public synchronized Entry findAndRemove(RMResource resource) {
    Mode mode = (resource.getLocalityAsk() == Resource.Locality.MUST)
                ? Mode.STRICT_LOCATION : Mode.ANY_LOCATION;
    return findAndRemove(new Entry(resource.getLocationAsk(),
        resource.getCpuVCoresAsk(), resource.getMemoryMbsAsk()), mode);
  }

  public synchronized Entry findAndRemove(UUID storeId) {
    Entry found = idToEntryMap.remove(storeId);
    if (found != null && findAndRemove(found, Mode.SAME_REF) == null) {
      LOG.error("Inconsistency in for storeId '{}' rmResourceId '{}'",
          storeId, found.getRmResourceId());
    }
    return found;
  }

  protected synchronized List<Entry> getEntries() {
    return new ArrayList<Entry>(idToEntryMap.values());
  }

  public synchronized List<RMResource> emptyStore() {
    List<RMResource> list = new ArrayList<RMResource>(idToEntryMap.values());
    idToEntryMap.clear();
    return list;
  }

  public synchronized int getSize() {
    return idToEntryMap.size();
  }

}
