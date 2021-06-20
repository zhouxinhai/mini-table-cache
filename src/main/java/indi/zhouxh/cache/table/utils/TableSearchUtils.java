package indi.zhouxh.cache.table.utils;


import indi.zhouxh.cache.table.*;
import lombok.SneakyThrows;
import org.apache.commons.beanutils.BeanUtils;

import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Create by zhouxinhai on 2021/2/1
 */
public class TableSearchUtils {

    @SneakyThrows
    private static <R extends AbsTableColumnDef> Object[] getResult(R record, Field[] rsFieldArry) {
        Object[] rsVals = new Object[rsFieldArry.length];
        for (int i = 0; i < rsFieldArry.length; i++) {
            Object val = rsFieldArry[i].get(record);
            rsVals[i] = val;
        }
        return rsVals;
    }

    private static <R extends AbsTableColumnDef> void fullScanSearchTraverseRecords(List<R> records,
                                                                                    String[] columnNames,
                                                                                    Object[] columnVals,
                                                                                    Field[] fieldArry,
                                                                                    Consumer<R> consumer) throws IllegalAccessException {
        for (R record : records) {
            if (record == null) {
                continue;
            }

            if (columnNames == null || columnVals == null) {
                consumer.accept(record);
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
                consumer.accept(record);
            }
        }
    }

    public static <R extends AbsTableColumnDef> ResultSet fullScanSearch(ITableInfo<R> tableInfo,
                                                                         String[] columnNames,
                                                                         Object[] columnVals,
                                                                         Field[] fieldArry,
                                                                         Predicate<R> condition,
                                                                         String[] rsColumnNames,
                                                                         Field[] rsFieldArry) throws IllegalAccessException {
        ResultSet rs = new ResultSet(rsColumnNames);

        fullScanSearchTraverseRecords(tableInfo.getRecords(), columnNames, columnVals, fieldArry, record -> {
            if (condition == null || condition.test(record)) {
                Object[] result = getResult(record, rsFieldArry);
                rs.getResults().add(result);
            }
        });
        return rs;
    }

    public static <R extends AbsTableColumnDef> List<R> fullScanSearch(ITableInfo<R> tableInfo,
                                                                       String[] columnNames,
                                                                       Object[] columnVals,
                                                                       Field[] fieldArry,
                                                                       Predicate<R> condition) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        List<R> resLst = new ArrayList<>();
        fullScanSearchTraverseRecords(tableInfo.getRecords(), columnNames, columnVals, fieldArry, new Consumer<R>() {
            @SneakyThrows
            @Override
            public void accept(R record) {
                if (condition == null || condition.test(record)) {
                    R clone = (R) (BeanUtils.cloneBean(record));
                    resLst.add(clone);
                }
            }
        });
        return resLst;
    }

    private static <R extends AbsTableColumnDef, RETURNTYPE> RETURNTYPE fastSearchTraverseRecords(RETURNTYPE returnObj,
                                                                                                  ColumnDiveGroupInfo columnDiveGroupInfo,
                                                                                                  List<R> records,
                                                                                                  Consumer<R> consumer) throws IllegalAccessException {

        //先找索引对应的数据
        Set<Integer> offsetIntersectSet = getOffsetIntersectSet(columnDiveGroupInfo);
        if (offsetIntersectSet == null) {
            return returnObj;
        }

        //对非索引字段进行过滤
        for (Integer offset : offsetIntersectSet) {
            R record = records.get(offset);
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
                consumer.accept(record);
            }
        }
        return returnObj;
    }

    public static <R extends AbsTableColumnDef> ResultSet fastSearch(ITableInfo<R> tableInfo,
                                                                     ColumnDiveGroupInfo columnDiveGroupInfo,
                                                                     Predicate<R> condition,
                                                                     String[] rsColumnNames,
                                                                     Field[] rsFieldArry) throws IllegalAccessException {
        ResultSet rs = new ResultSet(rsColumnNames);

        return fastSearchTraverseRecords(rs, columnDiveGroupInfo, tableInfo.getRecords(), record -> {
            if (condition == null || condition.test(record)) {
                Object[] result = getResult(record, rsFieldArry);
                rs.getResults().add(result);
            }
        });
    }

    public static <R extends AbsTableColumnDef> List<R> fastSearch(ITableInfo<R> tableInfo,
                                                                   ColumnDiveGroupInfo columnDiveGroupInfo,
                                                                   Predicate<R> condition) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, InstantiationException {
        List<R> resLst = new ArrayList<>();

        return fastSearchTraverseRecords(resLst, columnDiveGroupInfo, tableInfo.getRecords(), new Consumer<R>() {
            @SneakyThrows
            @Override
            public void accept(R record) {
                if (condition == null || condition.test(record)) {
                    R clone = (R) (BeanUtils.cloneBean(record));
                    resLst.add(clone);
                }
            }
        });
    }

    private static Set<Integer> getOffsetIntersectSet(ColumnDiveGroupInfo columnDiveGroupInfo) {
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
                return null;
            }

            if (offsetIntersectSet == null) {
                offsetIntersectSet = new HashSet<>();
                offsetIntersectSet.addAll(search);
            } else {
                offsetIntersectSet.retainAll(search);
            }

            if (offsetIntersectSet.isEmpty()) {
                return null;
            }
        }

        return offsetIntersectSet;
    }
}
