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
public class Demo2TableColumnDef extends AbsTableColumnDef {
    private static final long serialVersionUID = 1;

    private String col1;
    private Integer col2;
    private String col3;
}
