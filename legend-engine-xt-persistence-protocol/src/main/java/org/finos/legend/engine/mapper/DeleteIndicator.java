package org.finos.legend.engine.mapper;

public class DeleteIndicator {
    public boolean isEnabled;
    public String deleteIndicatorField;
    public String[] deleteIndicatorValues;

    public DeleteIndicator(boolean isEnabled, String deleteIndicatorField, String[] deleteIndicatorValues) {
        this.isEnabled = isEnabled;
        this.deleteIndicatorField = deleteIndicatorField;
        this.deleteIndicatorValues = deleteIndicatorValues;
    }
}
