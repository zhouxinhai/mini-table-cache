package indi.zhouxh.cache.table;


import indi.zhouxh.cache.table.columnDef.Demo1TableColumnDef;
import indi.zhouxh.cache.table.columnDef.Demo2TableColumnDef;
import indi.zhouxh.cache.table.utils.TableJoinUtils;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

import java.util.ArrayList;
import java.util.List;

/**
 * Create by zhouxinhai on 2021/2/8
 */
public class TableJoinTest {
    @After
    public void destroy() {
        TableFactory.unregisteAll();
    }

    private ITableInfo<Demo1TableColumnDef> initDemo1Table(int maxInsertCnt, String indexes) throws Exception {
        ITableInfo<Demo1TableColumnDef> demo1Table = TableFactory.registe("demo1Table", Demo1TableColumnDef.class, indexes);
        List<Demo1TableColumnDef> list = new ArrayList<>(maxInsertCnt);
        for (int i = 0; i < maxInsertCnt; i++) {
            list.add(new Demo1TableColumnDef("a" + i, i, "a" + i + "_"));
        }

        long start = System.currentTimeMillis();
        for (Demo1TableColumnDef demo1TableColumnDef : list) {
            demo1Table.insert(demo1TableColumnDef);
        }
        long end = System.currentTimeMillis();
        System.out.println("完成" + maxInsertCnt + "条" + demo1Table.getTableName() + "记录的插入，耗时" + (end - start) + "毫秒");
        return demo1Table;
    }

    private ITableInfo<Demo2TableColumnDef> initDemo2Table(int maxInsertCnt, String indexes) throws Exception {
        ITableInfo<Demo2TableColumnDef> demo2Table = TableFactory.registe("demo2Table", Demo2TableColumnDef.class, indexes);
        List<Demo2TableColumnDef> list = new ArrayList<>(maxInsertCnt);
        for (int i = 0; i < maxInsertCnt; i++) {

            list.add(new Demo2TableColumnDef("a" + i % 100, i, "a" + i % 100 + "_"));
        }

        long start = System.currentTimeMillis();
        for (Demo2TableColumnDef demo2TableColumnDef : list) {
            demo2Table.insert(demo2TableColumnDef);
        }
        long end = System.currentTimeMillis();
        System.out.println("完成" + maxInsertCnt + "条" + demo2Table.getTableName() + "记录的插入，耗时" + (end - start) + "毫秒");
        return demo2Table;
    }

    @Test
    public void innerJoinTest() throws Exception {
        ITableInfo<Demo1TableColumnDef> demo1Tbl = initDemo1Table(1000, "c1,c3");
        ITableInfo<Demo2TableColumnDef> demo2Tbl = initDemo2Table(900, "col1, col3");

        /*
         * select
         *   t1.c1, t1.c2,t1.c3,
         *   t2.col1,t2.col2,t2.col3
         * from
         *  demo1Table t1 inner join demo2Table t2
         *  on t1.c1 = t2.col1
         *  and t1.c3 = t2.col3
         * where t1.c1 = 'a1'
         *  and t2.col1 = 'a1';
         */
        ResultSet leftTblRs = demo1Tbl.select("c1", new Object[]{"a1"}, null, null);
        ResultSet rightTblRs = demo2Tbl.select("col1", new Object[]{"a1"}, null, null);

        ResultSet innerjoinRs1 = TableJoinUtils.innerJoin(leftTblRs, "c1,c2,c3", "c1,c3", rightTblRs, "col1,col2,col3", "col1,col3");
        Assert.assertEquals(9, innerjoinRs1.getResultCnt());

        ResultSet innerjoinRs2 = TableJoinUtils.innerJoin(leftTblRs, "c1,c2,c3", "c1,c2", rightTblRs, "col1,col2", "col1,col2");
        Assert.assertEquals(1, innerjoinRs2.getResultCnt());
    }

    @Test
    public void leftJoinTest() throws Exception {
        ITableInfo<Demo1TableColumnDef> demo1Tbl = initDemo1Table(1000, "c1, c3");
        ITableInfo<Demo2TableColumnDef> demo2Tbl = initDemo2Table(900, "col1, col3");

        /*
         * select
         *   t1.c1 as h1,
         *   t1.c2 as h2,
         *   t1.c3 as h3,
         *   t2.col1 as h4,
         *   t2.col2 as h5,
         *   t2.col3 as h6
         * from
         *  demo1Table t1 left join demo2Table t2
         *  on t1.c1 = t2.col1
         *  and t1.c3 = t2.col3
         * where t1.c1 = 'a1'
         *  and t2.col1 = 'a1';
         */
        ResultSet leftTblRs = demo1Tbl.select("c1", new Object[]{"a1"}, null, null);
        ResultSet rightTblRs = demo2Tbl.select("col1", new Object[]{"a1"}, null, null);

        ResultSet leftJoinRs1 = TableJoinUtils.leftJoin(leftTblRs,
                                                        "c1 as h1,c2 as h2,c3 as h3",
                                                        "c1,c3",
                                                        rightTblRs,
                                                        "col1 as h4,col2 as h5,col3 as h6",
                                                        "col1,col2");
        Assert.assertEquals(1, leftJoinRs1.getResultCnt());
        Assert.assertArrayEquals(new String[]{"h1", "h2", "h3", "h4", "h5", "h6"}, leftJoinRs1.getRsColumnNames());



        /*
         * select
         *   t1.c1,
         *   t1.c2,
         *   t1.c3,
         *   t2.col1,
         *   t2.col2,
         *   t2.col3
         * from
         *  demo1Table t1 right join demo2Table t2
         *  on t1.c1 = t2.col1
         *  and t1.c3 = t2.col3
         * where t1.c1 = 'a1'
         *  and t2.col1 = 'a1';
         */
        ResultSet leftJoinRs2 = TableJoinUtils.leftJoin(rightTblRs, "col1,col2,col3", "col1,col3", leftTblRs, "c1,c2,c3", "c1,c2");
        Assert.assertEquals(9, leftJoinRs2.getResultCnt());
    }
}
