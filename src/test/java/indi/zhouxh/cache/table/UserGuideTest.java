package indi.zhouxh.cache.table;

import indi.zhouxh.cache.table.columnDef.Demo1TableColumnDef;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.List;

/**
 * Create by zhouxinhai on 2021/2/9
 */
public class UserGuideTest {

    @After
    public void destroy() {
        TableFactory.unregisteAll();
    }

    @Test
    public void crudTest() throws Exception {

        /*
         * create table demo1tbl
         * (
         * c1 string,
         * c2 int,
         * c3 string
         * );
         *
         * CREATE index on demo1tbl(c1);
         * CREATE index on demo1tbl(c2);
         */
        ITableInfo<Demo1TableColumnDef> aTable = TableFactory.registe("demo1tbl", Demo1TableColumnDef.class, "c1,c2");

        /*
         * insert into demo1tbl values('a1_1', 1, 'a1_3');
         * insert into demo1tbl values('a2_1', 2, 'a2_3');
         * insert into demo1tbl values('a3_1', 3, 'a3_3');
         * insert into demo1tbl values('a1_1', 5, 'a5_3');
         */
        aTable.insert(new Demo1TableColumnDef("a1_1", 1, "a1_3"));
        aTable.insert(new Demo1TableColumnDef("a2_1", 2, "a2_3"));
        aTable.insert(new Demo1TableColumnDef("a3_1", 3, "a3_3"));
        aTable.insert(new Demo1TableColumnDef("a1_1", 5, "a5_3"));
        Assert.assertEquals(4, aTable.getRecordCnt());

        /*
         * select * from demo1tbl;
         */
        List<Demo1TableColumnDef> select = aTable.select(null, null, null);
        Assert.assertEquals(4, select.size());


        /*
         * select * from demo1tbl where c1 = 'a1_1' and c2 = 1;
         */
        select = aTable.select("c1,c2", new Object[]{"a1_1", 1}, null);
        Assert.assertEquals(1, select.size());

        /*
         * select * from demo1tbl where c1 in ('a1_1','a2_1');
         */
        select = aTable.select("c1", new Object[]{new String[]{"a1_1", "a2_1"}}, null);
        Assert.assertEquals(3, select.size());

        /*
         * select * from demo1tbl where c1 = 'a1_1'  and c2 > length(c3) ;
         */
        select = aTable.select("c1", new Object[]{"a1_1"}, r -> {
            return r.getC2() > r.getC3().length();
        });
        Assert.assertEquals(1, select.size());

        /*
         * select c1,c2 from demo1tbl where c1 = 'a1_1'  and c2 > length(c3) ;
         */
        ResultSet rs = aTable.select("c1", new Object[]{"a1_1"}, r -> {
            return r.getC2() > r.getC3().length();
        }, "c1 as h1,c2 as h2");
        Assert.assertEquals(1, rs.getResultCnt());
        Assert.assertArrayEquals(new String[]{"h1", "h2"}, rs.getRsColumnNames());

        /*
         * update demo1tbl set c3 =c1 where c1 = 'a3_1' and c1 <> c3;
         */
        int update = aTable.update("c1", new Object[]{"a3_1"}, r -> {
            return !r.getC1().equals(r.getC3());
        }, r -> {
            r.setC3(r.getC1());
        });
        Assert.assertEquals(1, update);
        /*
         * select * from demo1tbl where c1 = c3  and c1 = 'a3_1'
         */
        select = aTable.select("c1", new Object[]{"a3_1"}, r -> {
            return r.getC1().equals(r.getC3());
        });
        Assert.assertEquals(1, select.size());

        /*
         * update demo1tbl set c2 =100 where c1 in ('a1_1' ,'a2_1',a3_1')
         */
        update = aTable.update("c1", new Object[]{new String[]{"a1_1", "a2_1", "a3_1"}}, null, r -> {
            r.setC2(100);
        });
        Assert.assertEquals(4, update);
        /*
         * select * from demo1tbl where c2=100
         */
        select = aTable.select("c2", new Object[]{100}, null);
        Assert.assertEquals(4, select.size());

        /*
         * update demo1tbl c2=200 where c3 in ('a5_3' ,'a2_3')
         */
        update = aTable.update("c3", new Object[]{new String[]{"a5_3", "a2_3"}}, null, r -> {
            r.setC2(200);
        });
        Assert.assertEquals(2, update);
        /*
         * select * from demo1tbl where c2=200
         */
        select = aTable.select("c2", new Object[]{200}, null);
        Assert.assertEquals(2, select.size());

        /*
         * delete from demo1tbl where where c1 = c3  and c1 = 'a3_1'
         */
        int delete = aTable.delete("c1", new Object[]{"a3_1"}, r -> {
            return r.getC1().equals(r.getC3());
        });
        Assert.assertEquals(1, delete);
    }
}
