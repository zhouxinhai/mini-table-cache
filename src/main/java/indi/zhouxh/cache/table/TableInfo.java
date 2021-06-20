package indi.zhouxh.cache.table;


import indi.zhouxh.cache.table.utils.TableModifyUtils;
import indi.zhouxh.cache.table.utils.TableSearchUtils;
import indi.zhouxh.cache.table.utils.TableUtils;
import lombok.Getter;
import org.apache.commons.beanutils.BeanUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.io.Serializable;
import java.lang.reflect.Field;
import java.lang.reflect.InvocationTargetException;
import java.lang.reflect.Modifier;
import java.util.*;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantReadWriteLock;
import java.util.function.Consumer;
import java.util.function.Predicate;

/**
 * Create by zhouxinhai on 2021/1/31
 */
public class TableInfo<R extends AbsTableColumnDef> implements ITableInfo<R>, Serializable {
    private static final long serialVersionUID = 1;
    @Getter
    private Class<R> cla;
    @Getter
    private String[] indexNames;
    //records队列中，有一些被回收的空间
    @Getter
    private transient Queue<Integer> invalidOffsets;
    private transient ReentrantReadWriteLock rwl;
    private transient Lock readLock;
    private transient Lock writeLock;
    @Getter
    private String tableName;
    //表的字段名字和Field的对应关系
    @Getter
    private transient Map<String, Field> columnFieldMap;
    //表里面所有的数据的集合
    @Getter
    private List<R> records = new ArrayList<>();
    //索引森林
    @Getter
    private transient Map<String, IndexInfo> indexTrees;
    //索引字段对应的Field
    @Getter
    private transient List<Field> indexFields;

    @Override
    public void init() throws IllegalAccessException {
        //因为如果属性在定义阶段初始化，反序列化时候无法调用，所以定义阶段不进行属性的初始化操作
        columnFieldMap = new TreeMap<>();
        invalidOffsets = new LinkedList<>();
        rwl = new ReentrantReadWriteLock(true);
        readLock = rwl.readLock();
        writeLock = rwl.writeLock();
        indexTrees = new HashMap<>();
        indexFields = new ArrayList<>();

        Field[] declaredFields = cla.getDeclaredFields();
        for (Field field : declaredFields) {
            boolean isStatic = Modifier.isStatic(field.getModifiers());
            if (isStatic) {
                continue;
            }

            columnFieldMap.put(field.getName().toUpperCase(), field);
            field.setAccessible(true);
        }

        if (indexNames != null) {
            TableUtils.createIndexTrees(this, indexNames);
        }
        TableUtils.refreshIndexTrees(this);
    }

    public TableInfo(String tableName, Class<R> cla, String indexes) throws IllegalAccessException {
        this.cla = cla;
        this.tableName = tableName;

        String[] indexNames = TableUtils.convertColumnNames(indexes).getLeft();
        if (indexNames != null) {
            this.indexNames = new String[indexNames.length];
            System.arraycopy(indexNames, 0, this.indexNames, 0, indexNames.length);
        }

        init();
    }

    private int canRetrieveSize() {
        return invalidOffsets.size();
    }

    @Override
    public int getRecordCnt() {
        return records.size() - invalidOffsets.size();
    }

    public boolean needDefragment() {
        return (getRecordCnt() > 100 && canRetrieveSize() > getRecordCnt());
    }

    @Override
    public void defragment() throws IllegalAccessException {
        if (invalidOffsets.isEmpty()) {
            return;
        }
        writeLock.lock();

        List<R> tmpRecords = new ArrayList<>(getRecordCnt());
        int offset = -1;

        invalidOffsets.clear();
        List<IndexInfo<Object>> indexInfos = new ArrayList<>();
        for (Field field : indexFields) {
            IndexInfo indexInfo = indexTrees.get(field.getName().toUpperCase());
            indexInfo.clear();
            indexInfos.add(indexInfo);
        }

        for (R record : records) {
            if (record == null) {
                continue;
            }
            ++offset;
            tmpRecords.add(record);
            for (int i = 0; i < indexFields.size(); i++) {
                Object indexObj = indexFields.get(i).get(record);
                indexInfos.get(i).insert(indexObj, offset);
            }
        }
        records = tmpRecords;
        writeLock.unlock();
    }

    @Override
    public void insert(R record) throws IllegalAccessException, NoSuchMethodException, InvocationTargetException, InstantiationException {
        writeLock.lock();
        try {
            R clone = (R) (BeanUtils.cloneBean(record));
            Integer offset = invalidOffsets.poll();
            if (offset == null) {//没有可回收的空间
                records.add(clone);
                offset = records.size() - 1;
            } else {//有可回收的空间
                records.set(offset, clone);
            }

            for (Field field : indexFields) {
                Object indexObj = field.get(record);
                indexTrees.get(field.getName().toUpperCase()).insert(indexObj, offset);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public List<R> select(String indexes,
                          Object[] indexVals,
                          Predicate<R> condition) throws NoSuchMethodException, InstantiationException, IllegalAccessException, InvocationTargetException {
        String[] indexNames = TableUtils.convertColumnNames(indexes).getLeft();
        if (!((indexNames == null || indexVals == null) || (indexNames.length == indexVals.length))) {
            throw new RuntimeException("查找表[" + tableName + "]，属性名个数和属性值的个数不等[" + indexNames.length + "≠" + indexVals.length + "]");
        }
        Field[] fieldArry = null;
        if (indexNames != null) {
            fieldArry = TableUtils.getReflectField(this, indexNames);
        }
        readLock.lock();
        ColumnDiveGroupInfo columnDiveGroupInfo = TableUtils.columnsDivideGroup(this, fieldArry, indexVals);

        try {
            if (columnDiveGroupInfo == null) {
                return TableSearchUtils.fullScanSearch(this, indexNames, indexVals, fieldArry, condition);
            } else {
                return TableSearchUtils.fastSearch(this, columnDiveGroupInfo, condition);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public ResultSet select(String indexes, Object[] indexVals, Predicate<R> condition, String rsColumns) throws IllegalAccessException {
        String[] indexNames = TableUtils.convertColumnNames(indexes).getLeft();
        Pair<String[], String[]> pair = TableUtils.convertColumnNames(rsColumns);
        String[] rsColumnNames = pair.getLeft();
        String[] rsAliasNames = pair.getRight();
        if (!((indexNames == null || indexVals == null) || (indexNames.length == indexVals.length))) {
            throw new RuntimeException("查找表[" + tableName + "]，属性名个数和属性值的个数不等[" + indexNames.length + "≠" + indexVals.length + "]");
        }

        Field[] fieldArry = null;
        if (indexNames != null) {
            fieldArry = TableUtils.getReflectField(this, indexNames);
        }

        Field[] rsFieldArry;
        if (rsColumnNames != null) {
            rsFieldArry = TableUtils.getReflectField(this, rsColumnNames);
        } else {
            rsColumnNames = new String[columnFieldMap.size()];
            rsAliasNames = new String[columnFieldMap.size()];
            rsFieldArry = new Field[columnFieldMap.size()];
            Set<Map.Entry<String, Field>> entries = columnFieldMap.entrySet();
            int i = 0;
            for (Map.Entry<String, Field> entry : entries) {
                rsColumnNames[i] = entry.getKey();
                rsAliasNames[i] = entry.getKey();
                rsFieldArry[i] = entry.getValue();
                i++;
            }
        }

        readLock.lock();
        ColumnDiveGroupInfo columnDiveGroupInfo = TableUtils.columnsDivideGroup(this, fieldArry, indexVals);

        try {
            if (columnDiveGroupInfo == null) {
                return TableSearchUtils.fullScanSearch(this, indexNames, indexVals, fieldArry, condition, rsAliasNames, rsFieldArry);
            } else {
                return TableSearchUtils.fastSearch(this, columnDiveGroupInfo, condition, rsAliasNames, rsFieldArry);
            }
        } finally {
            readLock.unlock();
        }
    }

    @Override
    public int delete(String indexes,
                      Object[] indexVals,
                      Predicate<R> condition) throws IllegalAccessException, NoSuchMethodException, InstantiationException, InvocationTargetException {

        writeLock.lock();
        try {
            if ((indexes == null || indexVals == null) && condition == null) {

                Set<Map.Entry<String, IndexInfo>> entries = indexTrees.entrySet();
                for (Map.Entry<String, IndexInfo> entry : entries) {
                    entry.getValue().clear();
                }

                int delCnt = records.size() - invalidOffsets.size();
                records.clear();
                invalidOffsets.clear();
                return delCnt;
            }
        } finally {
            writeLock.unlock();
        }

        return update(indexes, indexVals, condition, null);
    }

    @Override
    public int update(String indexes,
                      Object[] indexVals,
                      Predicate<R> condition,
                      Consumer<R> update) throws InvocationTargetException, NoSuchMethodException, InstantiationException, IllegalAccessException {
        String[] indexNames = TableUtils.convertColumnNames(indexes).getLeft();
        if (!((indexNames == null || indexVals == null) || (indexNames.length == indexVals.length))) {
            throw new RuntimeException("查找表[" + tableName + "]，属性名个数和属性值的个数不等[" + indexNames.length + "≠" + indexVals.length + "]");
        }

        Field[] fieldArry = null;
        if (indexNames != null) {
            fieldArry = TableUtils.getReflectField(this, indexNames);
        }

        writeLock.lock();
        ColumnDiveGroupInfo columnDiveGroupInfo = TableUtils.columnsDivideGroup(this, fieldArry, indexVals);

        try {
            if (columnDiveGroupInfo == null) {
                return TableModifyUtils.fullScanUpdate(this, indexNames, indexVals, fieldArry, condition, update);
            } else {
                return TableModifyUtils.fastUpdate(this, columnDiveGroupInfo, condition, update);
            }
        } finally {
            writeLock.unlock();
        }
    }

    @Override
    public String toString() {
        StringBuilder sb = new StringBuilder();
        sb.append("表名：").append(tableName).append("\n");
        sb.append("字段名：");
        for (String columnName : columnFieldMap.keySet()) {
            sb.append(columnName).append("，");
        }
        sb.append("\n");

        sb.append("记录数据：").append("\n");
        readLock.lock();
        try {
            for (R record : records) {
                if (record == null) {
                    continue;
                }
                sb.append(record);
                sb.append("\n");
            }
            return sb.toString();
        } finally {
            readLock.unlock();
        }
    }
}
