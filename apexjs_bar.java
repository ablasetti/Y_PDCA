import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Set;

import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.Table;



public class apexjs_bar {

	List<String> js = new ArrayList<String>();


	public List<String> get_js(){
		return js;
	}


	public apexjs_bar(Table mytable, String mydiv, String title,String xaxis, String kpi, String bar, double lcl, double ucl){
		List<String> mylist = mytable.stringColumn(xaxis).asList();
		List<String> mylist2 = new ArrayList<>();
		for(String str: mylist)
			mylist2.add("'"+ str+ "'");

		List<Double> lcldata = new ArrayList<Double>();
		List<Double> ucldata = new ArrayList<Double>();
		for(int i = 0; i<mylist.size();i++) {
			lcldata.add(lcl);
			ucldata.add(ucl);
		}

		// TO SOLVE THE ISSUE OF DIFFERENT COLUMNS TYPE... double or int...
		List<String> myb = mytable.column(bar).asStringColumn().asList();
		List<Double> il = new ArrayList<>();	
		for(String ob : myb) {
			il.add(Double.valueOf(ob));
		}

		// y-axis scale ro accomodate graph and bars
		double minval = Collections.min(mytable.doubleColumn(kpi).asList());
		double minkpi = minval-minval*0.1;
		if(minval<10)
			minkpi =0;
		
		double maxbar = Collections.max(il)*4;
		
		
		// START WRITING JS
		js.add("<script src=\"https://cdn.jsdelivr.net/npm/apexcharts\"></script> "); 				// remote
		//		js.add("<script src= \"" + dir +"/lib/apexcharts-bundle/dist/apexcharts.js\"></script> "); 	// local 
		js.add(" <script>");

		js.add("var options = {\r\n" + 
				"          series: [{\r\n" + 
				"          name: 'YIELD',\r\n" + 
				"          type: 'line',\r\n" + 
				"          data:"
				+  mytable.column(kpi).asList().toString()  + 
				"        ,}, {\r\n" + 
				"          name: 'Best',\r\n" + 
				"          type: 'line',\r\n" + 
				"          data:"
				+  ucldata.toString() + 
				"        ,}, {\r\n" + 
				"          name: 'Avg',\r\n" + 
				"          type: 'line',\r\n" + 
				"          data:"
				+  lcldata.toString() + 
				"        ,}, {\r\n" + 
				"          name: 'Wfs#',\r\n" + 
				"          type: 'column',\r\n" + 

				"          data: "
				+  mytable.column(bar).asList().toString() + 
				"        ,}],\r\n" + 
				"          chart: {\r\n" + 
				"          height: 350,\r\n" + 
				"          type: 'line',\r\n" + 
				"          stacked: false\r\n" + 
				"        },\r\n" + 
				"        dataLabels: {\r\n" + 
				"          enabled: false\r\n" + 
				"        },\r\n" + 
				"        stroke: {\r\n" + 
				"          width: [3, 1,1,1]\r\n" + 
				"        },\r\n" + 
				"        title: {\r\n" + 
				"          text: '"+ title +"',\r\n" + 
				"          align: 'center',\r\n" + 
				"  \r\n" + 
				"        },\r\n" + 
				"        xaxis: {\r\n" + 
				"          categories: "
				+ mylist2.toString() + 
				"        ,},\r\n" + 
				"\r\n" + 

 				" yaxis: ["+
 				"{ seriesName: 'YIELD', decimalsInFloat:1, min:"+ minkpi+", title:{text: \"\"}},\r\n" + 
 				"{ seriesName: 'YIELD', decimalsInFloat:1,show: false, min:"+ minkpi+"},\r\n" + // same name for 2nd var to use the same scale
 				"{ seriesName: 'YIELD', decimalsInFloat:1,show: false, min:"+ minkpi+"},\r\n" + // same name for 3rd var to use the same scale
 				"{ seriesName: 'Wfs#', decimalsInFloat:0, opposite: true, max:"+maxbar+",title:{text: \"Wfs#\"}"+",}"     +
 				"],"+

				"};" // END...
				);




		js.add("var chart = new ApexCharts(document.querySelector(\"#"+mydiv+"\"), options);");
		js.add("chart.render();");

		js.add("</script>");
	}


}
