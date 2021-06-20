package indi.zhouxh.cache.table;

import lombok.Getter;

import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

/**
 * Create by zhouxinhai on 2021/1/31
 */
public class IndexInfo<T> {
    @Getter
    private Class<T> cla;
    @Getter
    private String indexName;
    private Map<T, Set<Integer>> indexTree = new HashMap<>();

    public IndexInfo(Class<T> cla,String indexName) {
        this.cla = cla;
        this.indexName = indexName;
    }


    public void clear() {
        indexTree.clear();
    }

    public Set<Integer> search(T indexObj) {
        return indexTree.get(indexObj);
    }


    public void insert(T indexObj, Integer offset) {
        Set<Integer> offsets = indexTree.computeIfAbsent(indexObj, k -> new HashSet<>());
        offsets.add(offset);
    }

    public void delete(T indexObj, Integer offset) {
        Set<Integer> offsets = indexTree.get(indexObj);
        if (offsets == null) {
            return;
        }
        offsets.remove(offset);
        if (offsets.isEmpty()) {
            indexTree.remove(indexObj);
        }
    }
}
