package indi.zhouxh.cache.table;

import lombok.Getter;

import java.util.LinkedList;
import java.util.List;

/**
 * Create by zhouxinhai on 2021/2/5
 */
public class ResultSet {

    public ResultSet(String[] rsColumnNames) {
        if (rsColumnNames != null) {
            this.rsColumnNames = new String[rsColumnNames.length];
            System.arraycopy(rsColumnNames, 0, this.rsColumnNames, 0, rsColumnNames.length);
        }
    }

    @Getter
    private String[] rsColumnNames;
    @Getter
    private List<Object[]> results = new LinkedList<>();

    public int getRsColumnsCnt() {
        if (rsColumnNames == null) {
            return 0;
        }
        return rsColumnNames.length;
    }

    public int getResultCnt() {
        return results.size();
    }
}
