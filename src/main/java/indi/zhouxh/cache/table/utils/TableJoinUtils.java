package indi.zhouxh.cache.table.utils;

import indi.zhouxh.cache.table.ResultSet;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.tuple.Pair;

import java.util.Arrays;
import java.util.Optional;

/**
 * Create by zhouxinhai on 2021/2/8
 */
public class TableJoinUtils {
    enum JoinType {
        INNER, OUTER
    }

    private static ResultSet join(JoinType joinType,
                                  ResultSet leftTblRs,
                                  String leftRsColumns,
                                  String leftJoinColumns,
                                  ResultSet rightTblRs,
                                  String rightRsColumns,
                                  String rightJoinColumns) {

        String[] leftJoinColumnNames = TableUtils.convertColumnNames(leftJoinColumns).getLeft();
        Pair<String[], String[]> leftPair = TableUtils.convertColumnNames(leftRsColumns);
        String[] leftRsColumnNames = leftPair.getLeft();
        String[] leftRsAliasNames = leftPair.getRight();
        String[] rightJoinColumnNames = TableUtils.convertColumnNames(rightJoinColumns).getLeft();
        Pair<String[], String[]> rightPair = TableUtils.convertColumnNames(rightRsColumns);
        String[] rightRsColumnNames = rightPair.getLeft();
        String[] rightRsAliasNames = rightPair.getRight();


        leftTblRs = Optional.ofNullable(leftTblRs).orElseThrow(() -> new RuntimeException("leftTblRs不能为空"));
        int[] leftJoinColumnsOffsets;
        try {
            leftJoinColumnsOffsets = TableUtils.getOffsets(leftTblRs, leftJoinColumnNames);
        } catch (Exception e) {
            throw new RuntimeException("关联查询的左表，关联字段" + Arrays.toString(leftJoinColumnNames) + "不在左表的字段集合内" + Arrays.toString(leftTblRs.getRsColumnNames()),
                                       e);
        }
        int[] leftRsColumnsOffsets;
        try {
            leftRsColumnsOffsets = TableUtils.getOffsets(leftTblRs, leftRsColumnNames);
        } catch (Exception e) {
            throw new RuntimeException("关联查询的左表，左表的返回字段" + Arrays.toString(leftRsColumnNames) + "不在左表的字段集合内" + Arrays.toString(leftTblRs.getRsColumnNames()),
                                       e);
        }

        rightTblRs = Optional.ofNullable(rightTblRs).orElseThrow(() -> new RuntimeException("rightTblRs不能为空"));
        int[] rightJoinColumnsOffsets;
        try {
            rightJoinColumnsOffsets = TableUtils.getOffsets(rightTblRs, rightJoinColumnNames);
        } catch (Exception e) {
            throw new RuntimeException("关联查询的右表，关联字段" + Arrays.toString(rightJoinColumnNames) + "不在右表的字段集合内" + Arrays.toString(rightTblRs.getRsColumnNames()),
                                       e);
        }

        int[] rightRsColumnsOffsets;
        try {
            rightRsColumnsOffsets = TableUtils.getOffsets(rightTblRs, rightRsColumnNames);
        } catch (Exception e) {
            throw new RuntimeException("关联查询的右表，右表的返回字段" + Arrays.toString(rightRsColumnNames) + "不在右表的字段集合内" + Arrays.toString(rightTblRs.getRsColumnNames()),
                                       e);
        }

        if (leftJoinColumnNames.length != rightJoinColumnNames.length) {
            throw new RuntimeException("连接左表关联字段" + Arrays.toString(leftJoinColumnNames) + "和右表的关联字段个数" + Arrays.toString(rightJoinColumnNames) + "不匹配[" + leftJoinColumnNames.length + "!=" + rightJoinColumnNames.length + "]");
        }

        ResultSet joinRs = new ResultSet(ArrayUtils.addAll(leftRsAliasNames, rightRsAliasNames));

        for (Object[] leftRs : leftTblRs.getResults()) {
            boolean joinSuccess = false;
            for (Object[] rigthRs : rightTblRs.getResults()) {
                int i = 0;
                for (; i < leftJoinColumnNames.length; i++) {
                    if (!leftRs[leftJoinColumnsOffsets[i]].equals(rigthRs[rightJoinColumnsOffsets[i]])) {
                        break;
                    }
                }
                if (i == leftJoinColumnNames.length) {
                    Object[] rs = new Object[leftRsColumnNames.length + rightRsColumnNames.length];
                    for (int j = 0; j < leftRsColumnNames.length; j++) {
                        rs[j] = leftRs[leftRsColumnsOffsets[j]];
                    }
                    for (int j = 0; j < rightRsColumnNames.length; j++) {
                        rs[j + leftRsColumnNames.length] = rigthRs[rightRsColumnsOffsets[j]];
                    }
                    joinRs.getResults().add(rs);
                    joinSuccess = true;
                }
            }

            if (!joinSuccess && joinType == JoinType.OUTER) {
                Object[] rs = new Object[leftRsColumnNames.length + rightRsColumnNames.length];
                for (int j = 0; j < leftRsColumnNames.length; j++) {
                    rs[j] = leftRs[leftRsColumnsOffsets[j]];
                }
                for (int j = 0; j < rightRsColumnNames.length; j++) {
                    rs[j + leftRsColumnNames.length] = null;
                }
                joinRs.getResults().add(rs);
            }
        }

        return joinRs;
    }

    public static ResultSet innerJoin(ResultSet leftTblRs,
                                      String leftRsColumns,
                                      String leftJoinColumns,
                                      ResultSet rightTblRs,
                                      String rightRsColumns,
                                      String rightJoinColumns) {

        return join(JoinType.INNER, leftTblRs, leftRsColumns, leftJoinColumns, rightTblRs, rightRsColumns, rightJoinColumns);
    }

    public static ResultSet leftJoin(ResultSet leftTblRs,
                                     String leftRsColumns,
                                     String leftJoinColumns,
                                     ResultSet rightTblRs,
                                     String rightRsColumns,
                                     String rightJoinColumns) {

        return join(JoinType.OUTER, leftTblRs, leftRsColumns, leftJoinColumns, rightTblRs, rightRsColumns, rightJoinColumns);
    }
}
