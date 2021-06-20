package indi.zhouxh.cache.table.utils;

import indi.zhouxh.cache.table.AbsTableColumnDef;
import indi.zhouxh.cache.table.ITableInfo;

import java.io.ByteArrayInputStream;
import java.io.ByteArrayOutputStream;
import java.io.ObjectInputStream;
import java.io.ObjectOutputStream;

/**
 * Create by zhouxinhai on 2021/2/2
 */
public class TableSerializeAndDeserializeUtils {

    public static <R extends AbsTableColumnDef> byte[] serialize(ITableInfo<R> tableInfo) throws Exception {
        tableInfo.defragment();
        ByteArrayOutputStream bos = new ByteArrayOutputStream();
        ObjectOutputStream oos = new ObjectOutputStream(bos);
        oos.writeObject(tableInfo);
        return bos.toByteArray();
    }

    public static <R extends AbsTableColumnDef> ITableInfo<R> deserialize(byte[] bytes, Class<R> cla) throws Exception {
        ByteArrayInputStream bais = new ByteArrayInputStream(bytes);
        ObjectInputStream ois = new ObjectInputStream(bais);
        Object obj = ois.readObject();
        ITableInfo<R> tabinfo = (ITableInfo<R>) obj;
        tabinfo.init();
        return tabinfo;
    }
}
