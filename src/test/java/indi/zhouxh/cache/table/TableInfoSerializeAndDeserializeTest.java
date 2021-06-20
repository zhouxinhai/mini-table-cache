package indi.zhouxh.cache.table;


import indi.zhouxh.cache.table.columnDef.Demo1TableColumnDef;
import indi.zhouxh.cache.table.utils.TableSerializeAndDeserializeUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Create by zhouxinhai on 2021/2/2
 */
public class TableInfoSerializeAndDeserializeTest {

    @After
    public void destroy() {
        TableFactory.unregisteAll();
    }

    @Test
    public void test() throws Exception {
        int MAX_INSERT_CNT = 10;
        ITableInfo<Demo1TableColumnDef> aTable1 = TestInfoPerformanceTest.tableBatchInsert(MAX_INSERT_CNT, "c1, c2");
        int MAX_DELETE_CNT = 3;
        String columns = "c1,c2";
        for (int i = 0; i < MAX_DELETE_CNT; i++) {
            Object[] columnsVals = new Object[]{"a" + i, i};
            int delete = aTable1.delete(columns, columnsVals, null);
            Assert.assertEquals(1, delete);
        }
        Assert.assertEquals(3, aTable1.getInvalidOffsets().size());
        Assert.assertEquals(7, aTable1.getRecordCnt());
        byte[] serialize = TableSerializeAndDeserializeUtils.serialize(aTable1);

        TableInfo<Demo1TableColumnDef> aTable2 = (TableInfo<Demo1TableColumnDef>) TableSerializeAndDeserializeUtils.deserialize(serialize,
                                                                                                                                Demo1TableColumnDef.class);
        Assert.assertEquals(0, aTable2.getInvalidOffsets().size());
        Assert.assertEquals(7, aTable2.getRecordCnt());

        TestInfoPerformanceTest.tableBatchInsert(aTable2, 3);
        Assert.assertEquals(0, aTable2.getInvalidOffsets().size());
        Assert.assertEquals(10, aTable2.getRecordCnt());
        for (int i = 0; i < MAX_DELETE_CNT; i++) {
            Object[] columnsVals = new Object[]{"a" + i, i};
            int delete = aTable2.delete(columns, columnsVals, null);
            Assert.assertEquals(1, delete);
        }
        Assert.assertEquals(3, aTable2.getInvalidOffsets().size());
        Assert.assertEquals(7, aTable2.getRecordCnt());
    }
}
