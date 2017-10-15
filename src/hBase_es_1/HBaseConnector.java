package hBase_es_1;

import java.io.IOException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.KeyValue;

public class HBaseConnector {
	
	public static void main(String[] argv) throws IOException {
		Configuration config = HBaseConfiguration.create();
		HTable table = new HTable(config, "blogposts");
		Get get = new Get("post1".getBytes());
		Result rs = table.get(get);

		for (KeyValue kv : rs.raw()) {
			System.out.print(new String(kv.getRow()) + " ");
			System.out.print(new String(kv.getFamily()) + ":");
			System.out.print(new String(kv.getQualifier()) + " ");
			System.out.println(new String(kv.getValue()));
		}
		table.close();
	}
}