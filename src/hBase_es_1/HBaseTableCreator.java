package hBase_es_1;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hbase.HBaseConfiguration;
import org.apache.hadoop.hbase.KeyValue;
import org.apache.hadoop.hbase.client.Get;
import org.apache.hadoop.hbase.client.HTable;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.util.Bytes;

//put Nome tabella        riga       family:qualifier    valore  

//put 'dia',           'informatica', 'person:name',   Davide'
//


public class HBaseTableCreator {
	public HTable creaTabella(String[] famiglie) throws IOException
	  {
		Configuration conf = HBaseConfiguration.create();
		HTable table = new HTable(conf, "dia");
		Put riga1 = new Put(Bytes.toBytes("riga1"));//riga
		Put riga2 = new Put(Bytes.toBytes("riga2"));//riga
		
//		riga.add(family, qualifier, value)
		
		riga1.add(Bytes.toBytes("informatica"), Bytes.toBytes("c1"), Bytes.toBytes("Davide"));
		riga1.add(Bytes.toBytes("informatica"), Bytes.toBytes("c2"), Bytes.toBytes("Andrea"));
		riga1.add(Bytes.toBytes("informatica"), Bytes.toBytes("c3"), Bytes.toBytes("Gian"));
		riga1.add(Bytes.toBytes("informatica"), Bytes.toBytes("c4"), Bytes.toBytes("Vin"));
		riga1.add(Bytes.toBytes("informatica"), Bytes.toBytes("c5"), Bytes.toBytes("Maur"));
		
		riga2.add(Bytes.toBytes("automazione"), Bytes.toBytes("c1"), Bytes.toBytes("Giovinco"));
		riga2.add(Bytes.toBytes("automazione"), Bytes.toBytes("c2"), Bytes.toBytes("Marini"));
		riga2.add(Bytes.toBytes("automazione"), Bytes.toBytes("c3"), Bytes.toBytes("Ulivi"));
		riga2.add(Bytes.toBytes("automazione"), Bytes.toBytes("c4"), Bytes.toBytes("Pacciarelli"));
		riga2.add(Bytes.toBytes("automazione"), Bytes.toBytes("c5"), Bytes.toBytes("Adacher"));
		
		List<Put> righe = new ArrayList<Put>(); 
		righe.add(riga1);
		righe.add(riga2);
		table.put(righe);
		table.close();
		return table;
	  }
	public String personeInformatica() throws IOException{
		HTable table=this.creaTabella(null);
		Get get=new Get("riga1".getBytes());
		String persone = null;
		Result rs = table.get(get);
		for(KeyValue kv : rs.raw()){
			if(kv.getFamily().equals(Bytes.toBytes("informatica")))
				persone+=new String(kv.getValue());
		}
		return persone;
		
	}
	
	public String personeInfoCol1(String gruppoDiRicerca) throws IOException{
		HTable table=this.creaTabella(null);
		Get get=new Get("riga1".getBytes());
		Result rs = table.get(get);
		byte [] value = rs.getValue(Bytes.toBytes("informatica"),Bytes.toBytes("gruppoDiRicerca"));
		String valueStr = Bytes.toString(value);
		return valueStr;
	}
	  }







































//Get g1 = new Get(Bytes.toBytes("riga1"));//prendo la riga1
//Get g2 = new Get(Bytes.toBytes("riga2"));//prendo la riga2
//  Result r1 = table.get(g1);
//  Result r2 = table.get(g2);
//
//  byte [] value1 = r1.getValue(Bytes.toBytes("informatica"),Bytes.toBytes("col1"));
//  byte [] value2 = r2.getValue(Bytes.toBytes("automazione"),Bytes.toBytes("col1"));
//
//  String valueStr1 = Bytes.toString(value1);//valore del primo elemento nella riga di colonna informatica
//  String valueStr2 = Bytes.toString(value2);//valore del primo elemento nella riga di colonna automazione
//  System.out.println("GET: " +"informatica1: " +valueStr1+ "automazione1: "+valueStr2);
//  Scan s = new Scan();
//  s.addColumn(Bytes.toBytes("informatica"), Bytes.toBytes("col1"));
//  s.addColumn(Bytes.toBytes("automazione"), Bytes.toBytes("col1"));
//  ResultScanner scanner = table.getScanner(s);
//
//  try
//  {
//     for (Result rnext = scanner.next(); rnext != null; rnext = scanner.next())
//     {
//        System.out.println("Found row : " + rnext);
//     }
//  } finally
//    {
//       scanner.close();
//    }
//  }

