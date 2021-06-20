package indi.zhouxh.cache.table.utils;


import indi.zhouxh.cache.table.*;
import org.apache.commons.lang3.tuple.Pair;

import java.lang.reflect.Field;
import java.util.ArrayList;
import java.util.List;

/**
 * Create by zhouxinhai on 2021/1/31
 */
public class TableUtils {

    public static <R extends AbsTableColumnDef> Field[] getReflectField(ITableInfo<R> tableInfo, String[] columnNames) {
        Field[] fields = new Field[columnNames.length];
        int i = 0;
        for (String columnName : columnNames) {
            Field field = tableInfo.getColumnFieldMap().get(columnName.toUpperCase());
            if (field == null) {
                throw new RuntimeException("在表[" + tableInfo.getTableName() + "]中不存在[" + columnName + "]属性");
            }
            fields[i++] = field;
        }
        return fields;
    }

    public static <R extends AbsTableColumnDef> ColumnDiveGroupInfo columnsDivideGroup(ITableInfo<R> tableInfo,
                                                                                       Field[] fieldArry,
                                                                                       Object[] columnVals) {

        if (fieldArry == null) {
            return null;
        }
        if (tableInfo.getIndexTrees().isEmpty()) {
            return null;
        }

        ColumnDiveGroupInfo columnDiveGroupInfo = new ColumnDiveGroupInfo(fieldArry.length);
        int i = -1;
        for (Field field : fieldArry) {
            i++;
            IndexInfo indexInfo = tableInfo.getIndexTrees().get(field.getName().toUpperCase());
            if (indexInfo != null) {
                columnDiveGroupInfo.getIndexInfos().add(indexInfo);
                columnDiveGroupInfo.getIndexColumnVals().add(columnVals[i]);
            } else {
                columnDiveGroupInfo.getFields().add(field);
                columnDiveGroupInfo.getCommColumnVals().add(columnVals[i]);
            }
        }

        if (columnDiveGroupInfo.getIndexInfos().isEmpty()) {
            return null;
        }

        return columnDiveGroupInfo;
    }

    public static <R extends AbsTableColumnDef> void createIndexTrees(ITableInfo<R> tableInfo, String[] indexNames) {
        for (String indexName : indexNames) {
            Field field = tableInfo.getColumnFieldMap().get(indexName.toUpperCase());
            if (field == null) {
                throw new RuntimeException("建立索引失败，表[" + tableInfo.getTableName() + "]，中不存在字段[" + indexName + "]");
            }
            tableInfo.getIndexFields().add(field);
            tableInfo.getIndexTrees().put(indexName.toUpperCase(), new IndexInfo<>(field.getType(), indexName.toUpperCase()));
        }
    }

    public static <R extends AbsTableColumnDef> void refreshIndexTrees(ITableInfo<R> tableInfo) throws IllegalAccessException {
        List<IndexInfo<Object>> indexInfos = new ArrayList<>();
        for (Field field : tableInfo.getIndexFields()) {
            IndexInfo indexInfo = tableInfo.getIndexTrees().get(field.getName().toUpperCase());
            indexInfo.clear();
            indexInfos.add(indexInfo);
        }
        int offset = -1;
        for (R record : tableInfo.getRecords()) {
            if (record == null) {
                continue;
            }
            ++offset;

            for (int i = 0; i < tableInfo.getIndexFields().size(); i++) {
                Object indexObj = tableInfo.getIndexFields().get(i).get(record);
                indexInfos.get(i).insert(indexObj, offset);
            }
        }
    }

    public static <R extends AbsTableColumnDef> int[] getOffsets(ResultSet rs, String[] columnNames) {
        int[] offsets = new int[columnNames.length];
        String[] rsColumnNames = rs.getRsColumnNames();
        for (int i = 0; i < columnNames.length; i++) {
            int j = 0;
            for (; j < rsColumnNames.length; j++) {
                if (rsColumnNames[j].equalsIgnoreCase(columnNames[i])) {
                    offsets[i] = j;
                    break;
                }
            }
            if (j == rsColumnNames.length) {
                throw new RuntimeException("在ResultSet无法找到[" + columnNames[i] + "]字段");
            }
        }
        return offsets;
    }

    public static Pair<String[], String[]> convertColumnNames(String columns) {

        if (columns == null) {
            return Pair.of(null, null);
        }
        String[] split = columns.split(",");
        String[] columnNames = new String[split.length];
        String[] aliasNames = new String[split.length];
        for (int i = 0; i < split.length; i++) {
            String columnAndAlias = split[i].trim();
            String[] split1 = columnAndAlias.split("[Aa][Ss]");
            String[] split2 = columnAndAlias.split("\\s+");

            if (split1.length != 1) {
                columnNames[i] = split1[0].trim();
                aliasNames[i] = split1[1].trim();
            } else if (split2.length != 1) {
                columnNames[i] = split2[0].trim();
                aliasNames[i] = split2[1].trim();
            } else {
                columnNames[i] = split2[0].trim();
                aliasNames[i] = split2[0].trim();
            }
        }

        return Pair.of(columnNames, aliasNames);
    }

    /*
    左值	右值	是否相等
    -------------------------
    null	null	true
    null	数组	？
    null	单值	false
    非null	null	false
    非null	单值	？
    非null	数组	？
     */
    public static boolean compareLeftAndRight(Object left, Object right) {
        if (left == null && right == null) {
            return true;
        }
        if (left != null && right == null) {
            return false;
        }

        boolean rightIsArray = right.getClass().isArray();
        if (left == null && !rightIsArray) {
            return false;
        }
        if (!rightIsArray) {
            return right.equals(left);
        }

        for (Object subRight : (Object[]) right) {
            if (subRight != null) {
                if (subRight.equals(left)) {
                    return true;
                }
            } else {
                if (left == null) {
                    return true;
                }
            }
        }
        return false;
    }
}
