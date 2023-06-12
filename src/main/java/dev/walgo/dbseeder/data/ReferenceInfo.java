package dev.walgo.dbseeder.data;

import java.util.ArrayList;
import java.util.List;

public class ReferenceInfo {

    private String fieldName;
    private String tableName;
    private List<String> tableColumn;
    private String tableKeyColumn;
    private int fieldIdx;

    public String getFieldName() {
        return fieldName;
    }

    public void setFieldName(String fieldName) {
        this.fieldName = fieldName;
    }

    public String getTableName() {
        return tableName;
    }

    public void setTableName(String tableName) {
        this.tableName = tableName;
    }

    public List<String> getTableColumn() {
        return tableColumn;
    }

    public void setTableColumn(String tableColumn) {
        this.tableColumn = List.of(tableColumn);
    }

    public void setTableColumn(List<String> tableColumns) {
        this.tableColumn = tableColumns;
    }

    public void addTableColumn(String tableColumn) {
        if (this.tableColumn == null) {
            this.tableColumn = new ArrayList<>();
        }
        this.tableColumn.add(tableColumn);
    }

    public int getFieldIdx() {
        return fieldIdx;
    }

    public void setFieldIdx(int fieldIdx) {
        this.fieldIdx = fieldIdx;
    }

    public String getTableKeyColumn() {
        return tableKeyColumn;
    }

    public void setTableKeyColumn(String tableKeyColumn) {
        this.tableKeyColumn = tableKeyColumn;
    }

    @Override
    public String toString() {
        return "ReferenceInfo{"
                + "fieldName=" + fieldName
                + ", tableName=" + tableName
                + ", tableColumn=" + tableColumn
                + ", tableKeyColumn=" + tableKeyColumn
                + ", fieldIdx=" + fieldIdx
                + '}';
    }

}
