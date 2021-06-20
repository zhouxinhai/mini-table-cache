package indi.zhouxh.cache.table;

import indi.zhouxh.cache.table.columnDef.Demo1TableColumnDef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Create by zhouxinhai on 2021/1/31
 */
public class TableInfoStdTest {
    @After
    public void destroy() {
        TableFactory.unregisteAll();
    }

    @Test
    public void testCRUD1() throws Exception {

        ITableInfo<Demo1TableColumnDef> aTable = TableFactory.registe("ATable", Demo1TableColumnDef.class, null);

        aTable.insert(new Demo1TableColumnDef("a1", 1, "a3"));
        aTable.insert(new Demo1TableColumnDef("a11", 11, "a33"));
        aTable.insert(new Demo1TableColumnDef("a111", 111, "a333"));

        System.out.println("插入3条记录后------------------------------------------------");
        System.out.println(aTable);

        List<Demo1TableColumnDef> select = aTable.select(null, null, record -> {
            return (record.getC2() > 10);
        });
        Assert.assertEquals(2, select.size());

        select = aTable.select(null, null, record -> {
            return (record.getC2() > 10 && record.getC1().equals("a111"));
        });
        Assert.assertEquals(1, select.size());

        select = aTable.select("C1,c2, C3", new Object[]{"a11", 11, "a33"}, null);
        Assert.assertEquals(1, select.size());
        System.out.println("------------------------------------------------");

        ResultSet rs = aTable.select("C1,c2, C3", new Object[]{"a11", 11, "a33"}, null, "C1,c2");
        Assert.assertEquals(1, rs.getResultCnt());
        rs = aTable.select("C1,c2, C3", new Object[]{"a11", 11, "a33"}, record -> {
            return record.getC2() < 10;
        }, "C1,c2");
        Assert.assertEquals(0, rs.getResultCnt());

        rs = aTable.select("C1,c2, C3", new Object[]{"a11", 11, "a33"}, null, null);
        Assert.assertEquals(1, rs.getResultCnt());
        rs = aTable.select("C1,c2, C3", new Object[]{"a11", 11, "a33"}, record -> {
            return record.getC3().length() == 3;
        }, null);
        Assert.assertEquals(1, rs.getResultCnt());

        int delete = aTable.delete("c1,c2,c3", new Object[]{"a11", 11, "a33"}, null);
        Assert.assertEquals(1, delete);
        System.out.println("删除" + select.size() + "条记录");
        System.out.println("------------------------------------------------");
        System.out.println(aTable);

        aTable.insert(new Demo1TableColumnDef("a1", 1, "a3"));
        System.out.println("插入1条记录后------------------------------------------------");
        System.out.println(aTable);
        System.out.println("------------------------------------------------");

        delete = aTable.delete("c1,c2,c3", new Object[]{"a1", 1, "a3"}, null);
        Assert.assertEquals(2, delete);
        System.out.println("删除" + delete + "条记录");
        System.out.println("------------------------------------------------");
        System.out.println(aTable);

        delete = aTable.delete(null, null, null);
        System.out.println("删除" + delete + "条记录");
        System.out.println("------------------------------------------------");
        System.out.println(aTable);
    }

    @Test
    public void testCRUD2() throws Exception {

        ITableInfo<Demo1TableColumnDef> aTable = TableFactory.registe("ATable", Demo1TableColumnDef.class, null);

        aTable.insert(new Demo1TableColumnDef("a1", 1, "a3"));
        aTable.insert(new Demo1TableColumnDef("a11", 11, "a33"));
        aTable.insert(new Demo1TableColumnDef("a111", 111, "a333"));
        System.out.println("插入3条记录后------------------------------------------------");
        System.out.println(aTable);
        System.out.println("------------------------------------------------");

        aTable.update("C1, c2, C3", new Object[]{"a1", 1, "a3"}, null, r -> {
            r.setC1("b1");
            r.setC2(1);
            r.setC3("b3");
        });
        System.out.println("更新数据后------------------------------------------------");
        System.out.println(aTable);
        System.out.println("------------------------------------------------");
    }

    @Test
    public void testIndex() throws Exception {
        ITableInfo<Demo1TableColumnDef> aTable = TableFactory.registe("ATable", Demo1TableColumnDef.class, "c1,c2");

        aTable.insert(new Demo1TableColumnDef("a1", 1, "a3"));
        aTable.insert(new Demo1TableColumnDef("a11", 11, "a33"));
        aTable.insert(new Demo1TableColumnDef("a111", 111, "a333"));
        aTable.insert(new Demo1TableColumnDef(null, 1111, "a3333"));

        System.out.println("插入4条记录后------------------------------------------------");
        System.out.println(aTable);

        List<Demo1TableColumnDef> select = aTable.select(null, null, null);
        Assert.assertEquals(4, select.size());

        select = aTable.select("C1, c2, C3", new Object[]{"a1", 1, "a3"}, null);
        Assert.assertEquals(1, select.size());

        System.out.println("更新1条记录后------------------------------------------------");
        int update = aTable.update("C1, c2, C3", new Object[]{"a1", 1, "a3"}, null, r -> {
            r.setC1("b1");
            r.setC2(2);
            r.setC3("b3");
        });
        Assert.assertEquals(1, update);
        System.out.println(aTable);
        System.out.println("删除1条记录后------------------------------------------------");
        int delete = aTable.delete("c1,c2,c3", new Object[]{null, 1111, "a3333"}, null);
        Assert.assertEquals(1, delete);
        System.out.println(aTable);

        select = aTable.select("C1, c2, C3", new Object[]{null, 1111, "a3333"}, null);
        Assert.assertEquals(0, select.size());

        int recordCnt = aTable.getRecordCnt();
        delete = aTable.delete(null, null, null);
        Assert.assertEquals(recordCnt, delete);
        System.out.println("删除所有后------------------------------------------------");
        System.out.println(aTable);
        System.out.println("------------------------------------------------");
    }
}
