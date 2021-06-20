package indi.zhouxh.cache.table;

import indi.zhouxh.cache.table.utils.TableUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.junit.After;
import org.junit.Assert;
import org.junit.Test;

/**
 * Create by zhouxinhai on 2021/2/8
 */
public class TableUtilsTest {

    @After
    public void destroy() {
        TableFactory.unregisteAll();
    }

    @Test
    public void testConvertColumnNames() {
        String columns = "c1  as  a1 ,c2, c3   a3  ,c4 as a4,c5 a5,c6 ";
        Pair<String[], String[]> pair = TableUtils.convertColumnNames(columns);
        Assert.assertArrayEquals(new String[]{"c1", "c2", "c3", "c4", "c5", "c6"}, pair.getLeft());
        Assert.assertArrayEquals(new String[]{"a1", "c2", "a3", "a4", "a5", "c6"}, pair.getRight());
    }
}
