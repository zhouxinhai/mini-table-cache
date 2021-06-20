package indi.zhouxh.cache.table;

import indi.zhouxh.cache.table.columnDef.Demo1TableColumnDef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;
import java.util.Random;

/**
 * Create by zhouxinhai on 2021/2/1
 */
public class TestInfoPerformanceTest {

    @After
    public void destroy() {
        TableFactory.unregisteAll();
    }

    static ITableInfo<Demo1TableColumnDef> tableBatchInsert(int maxInsertCnt, String indexes) throws Exception {

        ITableInfo<Demo1TableColumnDef> aTable = TableFactory.registe("ATable", Demo1TableColumnDef.class, indexes);
        tableBatchInsert(aTable, maxInsertCnt);
        return aTable;
    }

    static void tableBatchInsert(ITableInfo<Demo1TableColumnDef> aTable, int maxInsertCnt) throws Exception {
        List<Demo1TableColumnDef> list = new ArrayList<>(maxInsertCnt);
        for (int i = 0; i < maxInsertCnt; i++) {
            list.add(new Demo1TableColumnDef("a" + i, i, "a" + i + "_"));
        }

        long start = System.currentTimeMillis();
        for (Demo1TableColumnDef demo1TableColumnDef : list) {
            aTable.insert(demo1TableColumnDef);
        }
        long end = System.currentTimeMillis();
        System.out.println("完成" + maxInsertCnt + "条" + aTable.getTableName() + "记录的插入，耗时" + (end - start) + "毫秒");
    }

    private void performanceAddUpdate(String indexes) throws Exception {
        int MAX_INSERT_CNT = 100000;
        int MAX_UPDATE_CNT = 1000;
        ITableInfo<Demo1TableColumnDef> aTable = tableBatchInsert(MAX_INSERT_CNT, indexes);

        long start = System.currentTimeMillis();
        int updateCnt = 0;
        int missCnt = 0;
        Random random = new Random();
        for (int i = 0; i < MAX_UPDATE_CNT; i++) {
            int index = random.nextInt(MAX_INSERT_CNT);
            String columns = "c1,c2";
            Object[] columnsVals = new Object[]{"a" + index, index};
            int update = aTable.update(columns, columnsVals, null, r -> {
                r.setC1("a" + index);
                r.setC2(index);
                r.setC3("zhouxinhai");
            });
            Assert.assertEquals(1, update);
            if (update == 0) {
                missCnt++;
                System.out.println("在表[" + aTable.getTableName() + "]中，无法找到字段名字列表为[" + columns + "]," + "字段值列表为[" + columnsVals + "]");
            } else {
                updateCnt += update;
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("完成" + MAX_UPDATE_CNT + "条" + aTable.getTableName() + "记录的更新，更新" + updateCnt + "条，没有找到" + missCnt + "条，耗时" + (end - start) + "毫秒");
    }

    @Test
    public void performanceAddUpdateNoIndex() throws Exception {
        System.out.println("------------------performanceAddUpdateNoIndex");
        performanceAddUpdate(null);
    }

    @Test
    public void performanceAddUpdateByIndex() throws Exception {
        System.out.println("------------------performanceAddUpdateByIndex");
        performanceAddUpdate("c1,c2");
    }

    private void performanceAddSelect(String indexes, boolean result) throws Exception {
        int MAX_INSERT_CNT = 100000;
        int MAX_QUERY_CNT = 1000;
        ITableInfo<Demo1TableColumnDef> aTable = tableBatchInsert(MAX_INSERT_CNT, indexes);

        long start = System.currentTimeMillis();
        int findCnt = 0;
        int missCnt = 0;
        Random random = new Random();
        for (int i = 0; i < MAX_QUERY_CNT; i++) {
            int index = random.nextInt(MAX_INSERT_CNT);
            String columns = "c1,c2,c3";
            String[] columnNames = new String[]{"c1", "c2", "c3"};
            Object[] columnsVals = new Object[]{"a" + index, index, "a" + index + "_"};

            if (!result) {
                List<Demo1TableColumnDef> select = aTable.select(columns, columnsVals, null);
                Assert.assertEquals(1, select.size());
                if (select.isEmpty()) {
                    missCnt++;
                    System.out.println("在表[" + aTable.getTableName() + "]中，无法找到字段名字列表为[" + columnNames + "]," + "字段值列表为[" + columnsVals + "]");
                } else {
                    findCnt++;
                }
            } else {
                ResultSet select = aTable.select(columns, columnsVals, null, null);
                Assert.assertEquals(1, select.getResultCnt());
                if (select.getResultCnt() == 0) {
                    missCnt++;
                    System.out.println("在表[" + aTable.getTableName() + "]中，无法找到字段名字列表为[" + columnNames + "]," + "字段值列表为[" + columnsVals + "]");
                } else {
                    findCnt++;
                }
            }
        }
        long end = System.currentTimeMillis();
        System.out.println("完成" + MAX_QUERY_CNT + "条" + aTable.getTableName() + "记录的查找，找到" + findCnt + "条，没有找到" + missCnt + "条，耗时" + (end - start) + "毫秒");
    }

    @Test
    public void performanceAddSelectNoIndex1() throws Exception {
        System.out.println("------------------performanceAddSelectNoIndex 返回ResultSet");
        performanceAddSelect(null, true);
    }

    @Test
    public void performanceAddSelectNoIndex2() throws Exception {
        System.out.println("------------------performanceAddSelectNoIndex 返回Bean对象");
        performanceAddSelect(null, false);
    }

    @Test
    public void performanceAddSelectByIndex1() throws Exception {
        System.out.println("------------------performanceAddSelectByIndex 返回ResultSet");
        performanceAddSelect("c1, c2", true);
    }

    @Test
    public void performanceAddSelectByIndex2() throws Exception {
        System.out.println("------------------performanceAddSelectByIndex 返回Bean对象");
        performanceAddSelect("c1, c2", false);
    }
}
