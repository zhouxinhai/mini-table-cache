package indi.zhouxh.cache.table.columnDef;

import indi.zhouxh.cache.table.AbsTableColumnDef;
import lombok.AllArgsConstructor;
import lombok.Data;
import lombok.NoArgsConstructor;

/**
 * Create by zhouxinhai on 2021/1/31
 */
@Data
@AllArgsConstructor
@NoArgsConstructor
public class Demo1TableColumnDef extends AbsTableColumnDef {
    private static final long serialVersionUID = 1;

    private String c1;
    private int c2;
    private String c3;
}
