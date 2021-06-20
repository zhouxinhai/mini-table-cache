package indi.zhouxh.cache.table.utils;

import indi.zhouxh.cache.table.AbsTableColumnDef;
import indi.zhouxh.cache.table.ColumnDiveGroupInfo;
import indi.zhouxh.cache.table.ITableInfo;
import indi.zhouxh.cache.table.IndexInfo;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Create by zhouxinhai on 2021/2/1
 */
public class TableModifyUtils {

    public static <R extends AbsTableColumnDef> int fullScanUpdate(ITableInfo<R> tableInfo,
                                                                   String[] columnNames,
                                                                   Object[] columnVals,
                                                                   Field[] fieldArry,
                                                                   Predicate<R> condition,
                                                                   Consumer<R> update) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, InstantiationException {

        int updateCnt = 0;
        for (int offset = 0; offset < tableInfo.getRecords().size(); offset++) {
            R record = tableInfo.getRecords().get(offset);
            if (record == null) {
                continue;
            }

            if ((columnNames == null || columnVals == null) && (condition == null || condition.test(record))) {
                removeIndexTrees(tableInfo, offset, record);
                if (update == null) {
                    tableInfo.getRecords().set(offset, null);
                    tableInfo.getInvalidOffsets().add(offset);
                } else {
                    update.accept(record);
                    addIndexTrees(tableInfo, offset, record);
                }
                updateCnt++;
                continue;
            }

            int i = 0;
            for (; i < fieldArry.length; i++) {
                Object val = fieldArry[i].get(record);

                if (!TableUtils.compareLeftAndRight(val, columnVals[i])) {
                    break;
                }
//                if (val != null) {
//                    if (!val.equals(columnVals[i])) {
//                        break;
//                    }
//                } else {
//                    if (columnVals[i] != null) {
//                        break;
//                    }
//                }
            }
            if (i == fieldArry.length) {
                if (condition == null || condition.test(record)) {

                    removeIndexTrees(tableInfo, offset, record);
                    if (update == null) {
                        tableInfo.getRecords().set(offset, null);
                        tableInfo.getInvalidOffsets().add(offset);
                    } else {
                        update.accept(record);
                        addIndexTrees(tableInfo, offset, record);
                    }
                    updateCnt++;
                }
            }
        }
        return updateCnt;
    }


    public static <R extends AbsTableColumnDef> int fastUpdate(ITableInfo<R> tableInfo,
                                                               ColumnDiveGroupInfo columnDiveGroupInfo,
                                                               Predicate<R> condition,
                                                               Consumer<R> update) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, InstantiationException {
        int updateCnt = 0;
        Set<Integer> offsetIntersectSet = null;

        //先找索引对应的数据
        List<IndexInfo> indexInfos = columnDiveGroupInfo.getIndexInfos();
        for (int i = 0; i < indexInfos.size(); i++) {
            IndexInfo indexInfo = indexInfos.get(i);
            Set<Integer> search = null;
            Object indexColumnVal = columnDiveGroupInfo.getIndexColumnVals().get(i);
            if (indexColumnVal!=null && indexColumnVal.getClass().isArray()) {
                Set<Integer> searchTmp = new HashSet<>();
                for (Object obj : (Object[]) indexColumnVal) {
                    search = indexInfo.search(obj);
                    if (search != null && !search.isEmpty()) {
                        searchTmp.addAll(search);
                    }
                }
                search = searchTmp;
            } else {
                search = indexInfo.search(indexColumnVal);
            }

            if (search == null || search.isEmpty()) {
                return updateCnt;
            }

            if (offsetIntersectSet == null) {
                offsetIntersectSet = new HashSet<>();
                offsetIntersectSet.addAll(search);
            } else {
                offsetIntersectSet.retainAll(search);
            }

            if (offsetIntersectSet.isEmpty()) {
                return updateCnt;
            }
        }

        //对非索引字段进行过滤
        for (Integer offset : offsetIntersectSet) {
            R record = tableInfo.getRecords().get(offset);
            if (record == null) {
                continue;
            }
            List<Field> fields = columnDiveGroupInfo.getFields();
            List<Object> commColumnVals = columnDiveGroupInfo.getCommColumnVals();
            int i = 0;
            for (; i < fields.size(); i++) {
                Field field = fields.get(i);
                Object val = field.get(record);

                if (!TableUtils.compareLeftAndRight(val, commColumnVals.get(i))) {
                    break;
                }

//                if (val != null) {
//                    if (!val.equals(commColumnVals.get(i))) {
//                        break;
//                    }
//                } else {
//                    if (commColumnVals.get(i) != null) {
//                        break;
//                    }
//                }
            }
            if (i == fields.size()) {
                if (condition == null || condition.test(record)) {
                    removeIndexTrees(tableInfo, offset, record);
                    if (update == null) {
                        tableInfo.getRecords().set(offset, null);
                        tableInfo.getInvalidOffsets().add(offset);
                    } else {
                        update.accept(record);
                        addIndexTrees(tableInfo, offset, record);
                    }
                    updateCnt++;
                }
            }
        }
        return updateCnt;
    }

    private static <R extends AbsTableColumnDef> void removeIndexTrees(ITableInfo<R> tableInfo,
                                                                       int recordOffset,
                                                                       R record) throws IllegalAccessException {
        if (tableInfo.getIndexTrees().isEmpty()) {
            //如果没有索引，无需处理
            return;
        }
        for (Field field : tableInfo.getIndexFields()) {
            Object indexVal = field.get(record);

            IndexInfo indexInfo = tableInfo.getIndexTrees().get(field.getName().toUpperCase());
            indexInfo.delete(indexVal, recordOffset);
        }
    }

    private static <R extends AbsTableColumnDef> void addIndexTrees(ITableInfo<R> tableInfo,
                                                                    int recordOffset,
                                                                    R record) throws IllegalAccessException {
        if (tableInfo.getIndexTrees().isEmpty()) {
            //如果没有索引，无需处理
            return;
        }
        for (Field field : tableInfo.getIndexFields()) {
            Object indexVal = field.get(record);

            IndexInfo indexInfo = tableInfo.getIndexTrees().get(field.getName().toUpperCase());
            indexInfo.insert(indexVal, recordOffset);
        }
    }
}
