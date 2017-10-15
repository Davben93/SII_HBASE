package hBase_es_1;

import org.apache.hadoop.hbase.client.HTable;

public class RowBean {
	
	private HTable table;
	private Object family;
	private Object qualifier;
	private Object value;
	
	public RowBean(HTable table,Object family,Object qualifier, Object value){
		this.setTable(table);
		this.setFamily(family);
		this.setQualifier(qualifier);
		this.setValue(value);
	}

	public HTable getTable() {
		return table;
	}

	public void setTable(HTable table) {
		this.table = table;
	}

	public Object getFamily() {
		return family;
	}

	public void setFamily(Object family) {
		this.family = family;
	}

	public Object getValue() {
		return value;
	}

	public void setValue(Object value) {
		this.value = value;
	}

	public Object getQualifier() {
		return qualifier;
	}

	public void setQualifier(Object qualifier) {
		this.qualifier = qualifier;
	}
}

