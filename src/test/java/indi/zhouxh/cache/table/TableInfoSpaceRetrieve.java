package indi.zhouxh.cache.table;

import indi.zhouxh.cache.table.columnDef.Demo1TableColumnDef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Create by zhouxinhai on 2021/2/2
 */
public class TableInfoSpaceRetrieve {

    @After
    public void destroy(){
        TableFactory.unregisteAll();
    }

    @Test
    public void test() throws Exception {
        int MAX_INSERT_CNT = 100;
        ITableInfo<Demo1TableColumnDef> aTable = TestInfoPerformanceTest.tableBatchInsert(MAX_INSERT_CNT, "c1, c2");
        String columns = "c1,c2";
        int MAX_DELETE_CNT = 10;
        for (int i = 0; i < MAX_DELETE_CNT; i++) {
            Object[] columnsVals = new Object[]{"a" + i, i};
            int delete = aTable.delete(columns, columnsVals, null);
            Assert.assertEquals(1, delete);
        }
        Assert.assertEquals(MAX_INSERT_CNT - MAX_DELETE_CNT, aTable.getRecordCnt());
        Assert.assertEquals(MAX_DELETE_CNT, aTable.getInvalidOffsets().size());

        TestInfoPerformanceTest.tableBatchInsert(aTable, MAX_DELETE_CNT);
        Assert.assertEquals(MAX_INSERT_CNT, aTable.getRecordCnt());
        Assert.assertEquals(0, aTable.getInvalidOffsets().size());

        TestInfoPerformanceTest.tableBatchInsert(aTable, MAX_DELETE_CNT);
        Assert.assertEquals(MAX_INSERT_CNT + MAX_DELETE_CNT, aTable.getRecordCnt());
        Assert.assertEquals(0, aTable.getInvalidOffsets().size());

        for (int i = 0; i < MAX_DELETE_CNT; i++) {
            Object[] columnsVals = new Object[]{"a" + i, i};
            int delete = aTable.delete(columns, columnsVals, null);
            Assert.assertEquals(2, delete);
        }
        Assert.assertEquals(MAX_INSERT_CNT - MAX_DELETE_CNT, aTable.getRecordCnt());
        Assert.assertEquals(MAX_DELETE_CNT * 2, aTable.getInvalidOffsets().size());

        int recordCnt = aTable.getRecordCnt();
        int delete = aTable.delete(null, null, null);
        Assert.assertEquals(recordCnt, delete);
        Assert.assertEquals(0, aTable.getInvalidOffsets().size());
    }
}
