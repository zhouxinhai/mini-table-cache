package indi.zhouxh.cache.table;

import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Create by zhouxinhai on 2021/2/9
 */
public class TableFactory {
    private static Map<String, ITableInfo> tablekInfos = new ConcurrentHashMap<>();

    private TableFactory() {
    }

    public static <R extends AbsTableColumnDef> ITableInfo<R> registe(String tableName, Class<R> cla, String indexes) throws IllegalAccessException {
        ITableInfo tbl = tablekInfos.get(tableName);
        if (tbl != null) {
            throw new RuntimeException(tableName + "表已经注册过，请先注销后重新注册");
        }


        tbl = new TableInfo<R>(tableName, cla, indexes);
        tablekInfos.put(tableName, tbl);
        return tbl;
    }

    public static void unregiste(String tableName) {
        tablekInfos.remove(tableName);
    }

    public static void unregisteAll(){
        tablekInfos.clear();
    }
}
