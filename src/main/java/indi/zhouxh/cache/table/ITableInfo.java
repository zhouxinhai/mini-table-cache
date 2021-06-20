package indi.zhouxh.cache.table;

import java.lang.reflect.Field;
import java.util.List;
import java.util.Map;
import java.util.Queue;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Create by zhouxinhai on 2021/2/9
 */
public interface ITableInfo<R extends AbsTableColumnDef> {
    void init() throws Exception;

    int getRecordCnt() throws Exception;

    void defragment() throws Exception;

    void insert(R record) throws Exception;

    List<R> select(String indexes, Object[] indexVals, Predicate<R> condition) throws Exception;

    ResultSet select(String indexes, Object[] indexVals, Predicate<R> condition, String rsColumns) throws Exception;

    int delete(String indexes, Object[] indexVals, Predicate<R> condition) throws Exception;

    int update(String indexes, Object[] indexVals, Predicate<R> condition, Consumer<R> update) throws Exception;

    String getTableName();

    List<R> getRecords();

    Queue<Integer> getInvalidOffsets();

    Map<String, IndexInfo> getIndexTrees();

    List<Field> getIndexFields();

    Map<String, Field> getColumnFieldMap();
}
