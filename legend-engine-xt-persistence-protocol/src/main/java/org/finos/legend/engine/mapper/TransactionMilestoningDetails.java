package org.finos.legend.engine.mapper;

import com.gs.alloy.components.milestoning.model.MilestoningMode;

public class TransactionMilestoningDetails {
    public MilestoningMode milestoningMode;
    public String batchIdInName;
    public String batchIdOutName;
    public String batchTimeInName;
    public String batchTimeOutName;

    public TransactionMilestoningDetails(MilestoningMode milestoningMode, String batchIdInName,
                                         String batchIdOutName, String batchTimeInName, String batchTimeOutName) {
        this.milestoningMode = milestoningMode;
        this.batchIdInName = batchIdInName;
        this.batchIdOutName = batchIdOutName;
        this.batchTimeInName = batchTimeInName;
        this.batchTimeOutName = batchTimeOutName;
    }

}
