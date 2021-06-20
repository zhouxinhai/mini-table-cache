package indi.zhouxh.cache.table;

import lombok.Data;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Create by zhouxinhai on 2021/2/1
 */
@Data
public class ColumnDiveGroupInfo {

    public ColumnDiveGroupInfo() {
        indexInfos = new ArrayList<>();
        fields = new ArrayList<>();
        indexColumnVals = new ArrayList<>();
        commColumnVals = new ArrayList<>();
    }

    public ColumnDiveGroupInfo(int initialCapacity) {
        indexInfos = new ArrayList<>(initialCapacity);
        fields = new ArrayList<>(initialCapacity);
        indexColumnVals = new ArrayList<>(initialCapacity);
        commColumnVals = new ArrayList<>(initialCapacity);
    }

    //索引字段
    private List<IndexInfo> indexInfos;
    //非索引字段
    private List<Field> fields;
    //索引字段对应的值
    private List<Object> indexColumnVals;
    //非索引字段对应的值
    private List<Object> commColumnVals;
}
