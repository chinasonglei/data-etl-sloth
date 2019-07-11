package com.beadwallet.utils.common;

import com.beadwallet.cons.Constants;
import com.beadwallet.dao.entity.DispatchFlowInfoEntity;
import java.util.Comparator;

public class EtlFlowComparator implements Comparator<DispatchFlowInfoEntity> {

    /**
     * 调度任务排序工具
     *
     * @param o1 DispatchFlowInfoEntity
     * @param o2 DispatchFlowInfoEntity
     * @return -1：左值 < 右值，排在上面
     *          0 ：左值 = 右值，排在上面
     *          1 ：左值 > 右值，排在下面
     */
    @Override
    public int compare(DispatchFlowInfoEntity o1, DispatchFlowInfoEntity o2) {
        //升序
        if (o1.getTime_offset() > o2.getTime_offset()) {
            return 1;
        } else if (o1.getTime_offset() < o2.getTime_offset()) {
            return -1;
        }

        //ODS排前、RDS排后
        boolean isOds1 = o1.getFlow_name().startsWith(Constants.FLOW_FILE_PREFIX_ODS);
        boolean isOds2 = o2.getFlow_name().startsWith(Constants.FLOW_FILE_PREFIX_ODS);
        if (isOds1 != isOds2) {
            if (isOds1) {
                return -1;
            } else {
                return 1;
            }
        }

        //降序
        if (o1.getLevel() > o2.getLevel()) {
            return -1;
        } else if (o1.getLevel() < o2.getLevel()) {
            return 1;
        }

        //升序
        if (o1.getData_length() > o2.getData_length()) {
            return 1;
        } else if (o1.getData_length() < o2.getData_length()) {
            return -1;
        }

        //升序
        if (o1.getBusiness_source().compareTo(o2.getBusiness_source()) > 0) {
            return 1;
        } else if (o1.getBusiness_source().compareTo(o2.getBusiness_source()) < 0) {
            return -1;
        }

        //升序
        if (o1.getTable().compareTo(o2.getTable()) > 0) {
            return 1;
        } else if (o1.getTable().compareTo(o2.getTable()) < 0) {
            return -1;
        }

        return 0;
    }
}