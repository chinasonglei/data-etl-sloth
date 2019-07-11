package com.beadwallet.utils.common;

import com.beadwallet.cons.Constants;
import java.io.File;
import java.util.Comparator;

public class PrepareFlowComparator implements Comparator<File> {

    /**
     * 调度任务排序工具
     * 比较谁大谁小，将小的放在前面，大的放在后面。
     * 例如当返回负数的时候，表明第一个数应该排在第二个数的上面。
     *
     * @param o1 File
     * @param o2 File
     * @return -1：左值 < 右值，左值排在上面
     *          0 ：左值 = 右值，左值排在上面
     *          1 ：左值 > 右值，左值排在下面
     */
    @Override
    public int compare(File o1, File o2) {
        if (o1 == null || o2 == null) {
            return 0;
        }

        boolean isCreate1 = o1.getName().startsWith(Constants.FLOW_FILE_PREFIX_CREATE);
        boolean isCreate2 = o2.getName().startsWith(Constants.FLOW_FILE_PREFIX_CREATE);

        if (isCreate1 != isCreate2) {
            if (isCreate1) {
                return -1;
            } else {
                return 1;
            }
        }

        return 0;
    }
}