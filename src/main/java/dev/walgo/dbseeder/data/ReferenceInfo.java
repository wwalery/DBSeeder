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

    public ReferenceInfo setFieldName(String fieldName) {
        this.fieldName = fieldName;
        return this;
    }

    public String getTableName() {
        return tableName;
    }

    public ReferenceInfo setTableName(String tableName) {
        this.tableName = tableName;
        return this;
    }

    public List<String> getTableColumn() {
        return tableColumn;
    }

    public ReferenceInfo setTableColumn(String... tableColumn) {
        this.tableColumn = List.of(tableColumn);
        return this;
    }
    
    public ReferenceInfo setTableColumn(List<String> tableColumns) {
        this.tableColumn = tableColumns;
        return this;
    }

    public ReferenceInfo addTableColumn(String tableColumn) {
        if (this.tableColumn == null) {
            this.tableColumn = new ArrayList<>();
        }
        this.tableColumn.add(tableColumn);
        return this;
    }

    public int getFieldIdx() {
        return fieldIdx;
    }

    public ReferenceInfo setFieldIdx(int fieldIdx) {
        this.fieldIdx = fieldIdx;
        return this;
    }

    public String getTableKeyColumn() {
        return tableKeyColumn;
    }

    public ReferenceInfo setTableKeyColumn(String tableKeyColumn) {
        this.tableKeyColumn = tableKeyColumn;
        return this;
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
