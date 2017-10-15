package hBase_es_1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Delete;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

public class WrapperHBase {

	public void addRecord(String tableName, String rowKey, String family, String qualifier, String value)
			throws Exception {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tableName);
		Put riga = new Put(Bytes.toBytes(rowKey));
		riga.add(Bytes.toBytes(family), Bytes.toBytes(qualifier), Bytes.toBytes(value));
		List<Put> righe = new ArrayList<Put>();
		righe.add(riga);
		table.put(righe);
		table.close();
	}

	public void delRecord(String tableName, String rowKey) throws IOException {

		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tableName);
		Delete delete = new Delete(Bytes.toBytes(rowKey));
		table.delete(delete);
		table.close();
	}

	public RowBean getOneRecord(String tableName, String rowKey) throws IOException {
		RowBean rb;
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, tableName);
		Get get = new Get(Bytes.toBytes(rowKey));
		Result rs = table.get(get);
		KeyValue kv = rs.raw()[0];
		Object family = Bytes.toString(kv.getFamily());
		Object qualifier = Bytes.toString(kv.getQualifier());
		Object value = Bytes.toString(kv.getValue());
		rb = new RowBean(table, family, qualifier, value);
		return rb;
	}
}