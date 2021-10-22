import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.FileReader;
import java.io.FileWriter;
import java.io.IOException;
import java.io.PrintStream;
import java.sql.Connection;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.LocalDate;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Date;
import java.util.List;

import tech.tablesaw.api.BooleanColumn;
import tech.tablesaw.api.ColumnType;
import tech.tablesaw.api.DateTimeColumn;
import tech.tablesaw.api.DoubleColumn;
import tech.tablesaw.api.IntColumn;
import tech.tablesaw.api.Row;
import tech.tablesaw.api.StringColumn;
import tech.tablesaw.api.Table;
import tech.tablesaw.columns.numbers.NumberColumnFormatter;
import tech.tablesaw.io.xlsx.XlsxReadOptions;
import tech.tablesaw.io.xlsx.XlsxReader;
import tech.tablesaw.joining.DataFrameJoiner;
import tech.tablesaw.plotly.components.Figure;
import tech.tablesaw.plotly.components.Layout;
import tech.tablesaw.plotly.components.Marker;
import tech.tablesaw.plotly.traces.BarTrace;
import tech.tablesaw.plotly.traces.HistogramTrace;
import tech.tablesaw.plotly.traces.Trace;

public class yield_pdca {


	static db probe ;
	static Connection p_con;




	// costrutture
	public yield_pdca() { 
		System.out.println("Starting Yield PDCA..." );

		// Turn on the thread
		runsched sched =  new runsched();
		new Thread(sched).start();
	}




	class runsched implements Runnable
	{
		public void run() {
			for(;;){ // never end loop
				try {
					if(itstime()){
						System.setErr(new PrintStream(new File("err.txt"))); 
						System.out.println("START ANALYSIS...");
						yield_anal();
						System.out.println("DONE...");
					}

					Thread.sleep(60000);
				} catch (InterruptedException | FileNotFoundException e) {
					// TODO Auto-generated catch block
					e.printStackTrace();
				}
			}
		}
	}







	public static void main(String[] args) {
		yield_pdca ypdca = new yield_pdca();
	}






	public static void notify(String body) {
		BufferedReader brin;
		try {
			brin = new BufferedReader(new FileReader("notify.txt"));

			String topeople = brin.readLine();
		
		
		// Title, body, to
		new msgsend(
				"YIP products status",  // TITLE

				"Ciao, <br>"+      // BODY...
				"al seguente link troverai lo stato della resa per i main devices che afferiscono al programma di yield incentive: (use Chrome or Firefox)<br>"
				+ body
				+"<br><br>" + "Regards, A."
				,
				topeople
//				"ablasetti@lfoundry.com" // TO
//				"ablasetti@lfoundry.com;Antonio.Venezia@lfoundry.com;Piero.Testa@lfoundry.com;Giuseppe.Caldarola@lfoundry.com" // TO
//				"ablasetti@lfoundry.com;Antonio.Venezia@lfoundry.com;Gianluca.Testa@lfoundry.com" // TO
				).send();
		
		brin.close();
		} catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}
		



	static String headhtml = 
			" <!DOCTYPE html>"+
					" <html lang=\"en-US\">" +
					"<head>"+
					" <meta charset=\"UTF-8\">"+
					//					"<title>Average retail price for champagnes by year and rating</title>"+
					"<script src=\"https://cdn.plot.ly/plotly-latest.min.js\"></script>"+
					"<link rel=\"stylesheet\" href=\"https://www.w3schools.com/w3css/4/w3.css\">"+
					"<link rel=\"stylesheet\" href=\"../css/ale.css\">"+
					//					"<link rel=\"stylesheet\" href=\"C:/Users/ablasetti/myenv/workspace/CpK/css/ale.css\">"+
					"</head>"+
					"<body>";
	//
	//	dbo, TblLot, FldFab, varchar, 8, 0
	//	dbo, TblLot, FldStep, varchar, 8, 0
	//	dbo, TblLot, FldLot, varchar, 16, 0
	//	dbo, TblLot, FldDevice, varchar, 8, 1
	//	dbo, TblLot, FldPartType, varchar, 16, 1
	//	dbo, TblLot, FldStartWaferQty, int, 10, 1
	//	dbo, TblLot, FldStepInWaferQty, int, 10, 1
	//	dbo, TblLot, FldStepOutWaferQty, int, 10, 1
	//	dbo, TblLot, FldStepInDateTime, datetime, 23, 1
	//	dbo, TblLot, FldStepOutDateTime, datetime, 23, 1
	//	dbo, TblLot, FldStepWaferQtyState, varchar, 16, 1
	//	dbo, TblLot, FldStepState, varchar, 16, 1
	//	dbo, TblLot, FldYieldMeanDpw, float, 53, 1
	//	dbo, TblLot, FldYieldMedianDpw, float, 53, 1
	//	dbo, TblLot, FldYieldStdDevDpw, float, 53, 1
	//	dbo, TblLot, FldYieldPct10Dpw, float, 53, 1
	//	dbo, TblLot, FldYieldPct25Dpw, float, 53, 1
	//	dbo, TblLot, FldYieldPct75Dpw, float, 53, 1
	//	dbo, TblLot, FldYieldPct90Dpw, float, 53, 1
	//	dbo, TblLot, FldYieldMinDpw, int, 10, 1
	//	dbo, TblLot, FldYieldMaxDpw, int, 10, 1
	//	dbo, TblLot, FldYieldRangeDpw, int, 10, 1
	//	dbo, TblLot, FldNote, varchar, 256, 1



	public static void write_pdca_head(FileWriter final_report) throws IOException {
		final_report.write("  <div class=\"w3-row-padding w3-padding-1 w3-center\" id=\"food\">\r\n");
		final_report.write("<div  class=\"w3-half\">");	 
		final_report.write("<h3> Status - Graph/Chart </h3>");
		final_report.write("</div>");
		final_report.write("<div class=\"w3-half\"  w3-dark-grey >");	    
		final_report.write("<h3> Pareto of the Gaps</h3>");
		final_report.write("</div>");
		final_report.write("</div>"); //end row	
	}


	public static void write_pdca_tail(FileWriter final_report) throws IOException {
		final_report.write("  <div class=\"w3-row-padding w3-padding-1 w3-center\" id=\"food\">\r\n");
		final_report.write("<div  class=\"w3-half\">");	 
		final_report.write("<h3> Gap Analysis</h3>");
		final_report.write("</div>");
		final_report.write("<div class=\"w3-half\"  w3-dark-grey >");	    
		final_report.write("<h3> Actions</h3>");
		final_report.write("</div>");
		final_report.write("</div>"); //end row
	}



	public static void write_pdca_title(FileWriter final_report, String title) throws IOException {
		final_report.write("<br>");
		final_report.write("  <div class=\"w3-row-padding w3-padding-16 w3-center w3-red\" id=\"pdca\">\r\n");

		final_report.write("<div  class=\"w3-quarter\">");	 
		final_report.write("<img src=\" ../logo2.png \"  alt=\"cpk\" style=\"width:100%\">");
		final_report.write("</div>");

		final_report.write("<div  class=\"w3-quarter\">");	 
		final_report.write("Objective: "+ title );
		final_report.write("</div>");

		final_report.write("<div  class=\"w3-quarter\">");	 
		final_report.write("Responsible: Antonio Venezia \n");
		final_report.write("Accountable: Alessandro Blasetti \n");

		final_report.write("</div>");

		final_report.write("<div  class=\"w3-quarter\">");	 
		final_report.write("Update: " +  LocalDate.now());
		final_report.write("</div>");

		final_report.write("</div>"); //end row
	}




	private static Table getTable(int sheet, String dev, String location)throws IOException{
		//		String location = "./ACTIONS/C24K.xlsx";

//		String location = "./ACTIONS/"+ dev.substring(0,4) +".xlsx";

		System.out.println("TAB#: " + sheet);

		XlsxReader reader = new XlsxReader();
		//		XlsxReadOptions options = XlsxReadOptions.builderFromUrl(location).sheetIndex(sheet).build();
		XlsxReadOptions options = XlsxReadOptions.builderFromFile(location).sheetIndex(sheet).build();
		Table tab = reader.read(options);

		tab
		.columnsOfType(ColumnType.LOCAL_DATE_TIME)
		.forEach(x -> ((DateTimeColumn)x).setPrintFormatter(DateTimeFormatter.ISO_LOCAL_DATE));

		//		tab.removeColumns("GAIN","QTR","WW");

		//		tab = tab.where(tab.stringColumn("STATUS").isNotEqualTo("CLOSED"));
		System.out.println(tab.print());
		System.out.println("OK...");
		//				manage_expiration(tab);

		return tab;
	}










	// Sembra che in alcuni casi passino i valori die level del bin ma non il bin..???
	public static Table get_Lot_list_2(db probe, Connection p_con, String step, String dev,String yearmonth) throws IOException {

		// FOR CHECK REMOVE... TODO
//		Table LFW = probe.get_lots_by_month_2(p_con, step, dev, yearmonth);
		Table LFW = probe.get_lots_by_period(p_con, step, dev, yearmonth);

		IntColumn dy = IntColumn.create("dyield");;
		for (Row r : LFW ) {
			String mylfr = r.getText("lwr");

			String yd = probe.get_die_yield(p_con, step, dev, mylfr);
			int mydy = Integer.valueOf(yd);
			dy.append(mydy);
			//			System.out.println(r.getInt("PDPW") + " --- "+mydy );
		}
		LFW.addColumns(dy);
		LFW.write().csv("raw/"+dev+step+yearmonth+"_dieyield.csv");

		//		Lot	wf	Run	PDPW	lwr	Year	QTR	Month	WW	YearMonth	dyield
		//		9171339.009	19	0	324	9171339.009-19-0	2021	3	7	26	20217	324
		//		9171339.009	10	0	326	9171339.009-10-0	2021	3	7	26	20217	326

		return LFW;
	}



	public static String get_lotList_from_table(Table LFW) {
		List<String> all_lots = LFW.stringColumn("Lot").unique().asList();;
		String sql_lots="(";
		for(String mylot:all_lots)
			sql_lots = sql_lots+ "'" + mylot +"'"+"," ;
		sql_lots = sql_lots.substring(0, sql_lots.length() - 1)+")";
		
		if(all_lots.isEmpty())
			sql_lots = "('')";

		return sql_lots;
	}




	public static Integer get_wafer_count(Table LFW) {
		int wcount = LFW.where(LFW.intColumn("dyield").isNotEqualTo(-1)).rowCount();
		return wcount;
	}





	public  boolean itstime(){
		boolean val = false;
		String DATE_FORMAT_NOW = "yyyy/MM/dd";
		String DATE_FORMAT_H = "HH";
		String DATE_FORMAT_M = "mm";

		Calendar cal;
		SimpleDateFormat sdf = new SimpleDateFormat(DATE_FORMAT_NOW);
		SimpleDateFormat sdf_h = new SimpleDateFormat(DATE_FORMAT_H);
		SimpleDateFormat sdf_m = new SimpleDateFormat(DATE_FORMAT_M);
		cal = Calendar.getInstance();

		String hours = sdf_h.format(cal.getTime());
		String minutes = sdf_m.format(cal.getTime());


		BufferedReader brin;
		try {
			brin = new BufferedReader(new FileReader("timeup.txt"));

			String time = brin.readLine();
			String[] hm = time.split("-");
			int day = Integer.parseInt(hm[2]);
//			if(cal.get(Calendar.DAY_OF_WEEK) == day && hours.equals(hm[0]) && minutes.equals(hm[1])){
			if(hours.equals(hm[0]) && minutes.equals(hm[1])){
				val = true;
				System.out.println("Time to wake up...");
			}

		} catch ( IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return val;
	}












	public static void yield_anal() {
		try {
			probe = new db();
			p_con = probe.get_connection();
//			String file = "report/Yield-report_"+LocalDate.now()+".html";
//			String dir = System.getProperty("user.dir")+"\\report\\"+file;
			
			String file = System.getProperty("user.dir")+"/report/Yield-report_"+LocalDate.now()+".html";
			
			FileWriter final_report = new FileWriter(new File(file));
			final_report.write(headhtml);


			BufferedReader brin = new BufferedReader(new FileReader("dev.txt"));
			String adddev = "";


			//			probe.get_bin_pareto(p_con, "'PPP'", "C24AQ09AAA");


			int count = 0;
			while( (adddev = brin.readLine()) != null) {
				String dev = adddev.split("-")[0];
				String step = adddev.split("-")[1];
				analyze( step,  dev,  final_report,count);
				count++;
				final_report.flush();
			}
			brin.close();


			final_report.write("<HR>");
			DateFormat dateFormat = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");
			final_report.write("<p> A. Blasetti " + dateFormat.format(new Date()) + "</p>");
			final_report.write("\n");

			final_report.flush();
			final_report.close();
			p_con.close();
			
			
			notify(file);
			
		} catch (IOException | SQLException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

	}



	
	
	
	public static Table get_bin_pareto(String step, String dev, String tgt, int maxdpw) throws IOException {
		
		Table lots_t = get_Lot_list_2(probe,p_con, step, dev, tgt);
		String lots = get_lotList_from_table(lots_t);
		Table binpareto = probe.get_bin_pareto_2(p_con, step, dev,lots);
		DoubleColumn pct = (binpareto.intColumn("bincount")
				.divide(get_wafer_count(lots_t))
				.divide(maxdpw).multiply(100)
				);

		pct.setName(tgt);
		binpareto.addColumns(pct);
		binpareto.removeColumns("bincount");
		
		return binpareto;
	}
	
	
	
	
	


	public static void analyze(String step, String dev, FileWriter final_report, int count) {

		Table yield = null; 

		try {

			int maxdpw = probe.getmaxdpw(p_con, dev, step); // TODO...
			
			write_pdca_title(final_report,dev + " "+step +" Yield PDCA");

			Table yield_qtr = probe.get_yield_by_quarter(p_con, step, dev);
//			Table yield_month = probe.get_yield_by_month(p_con, step, dev);
			
			StringColumn qtr  = yield_qtr.stringColumn("YearQTR").setName("QTR");
			StringColumn Yield = yield_qtr.doubleColumn("Yield")
					.divide(maxdpw)
					.multiply(100)
					.asFloatColumn()
					.asStringColumn()
					.setName("Yield");
			
			Table yt  = Table.create("YT");
			yt.addColumns(qtr,Yield);

			Table bestqtr = yt.sortDescendingOn("Yield").first(1); //getString(0, 0);
			String best_quarter = bestqtr.getString(0, 0);
			double best_qtr_val = Double.valueOf(bestqtr.getString(0, 1));

			yield = probe.get_yield_by_week(p_con, step, dev);
			DoubleColumn yieldpct = (yield.intColumn("Yield").divide(maxdpw).multiply(100)).setName("Yield%");
			yield.addColumns(yieldpct);

			double yield_mean = yield.doubleColumn("Yield%").mean();
//			double pct_90 = yield.doubleColumn("Yield%").percentile(90);
			
			final_report.write("  <div class=\"w3-row-padding w3-padding-16 w3-center\" id=\"food\">\r\n");

			write_pdca_head(final_report);

			String yield_id = dev+"yielddiv"+count;
			final_report.write("<div id= "+yield_id+" class=\"w3-half\">");

			final_report.write("<br><br>");
			List<String> jscpk2 = new apexjs_bar(yield,yield_id,dev+ " "+ step.replace("'", "")+" Yield trend by WW", "YearWorkWeek","Yield%","WfCount" ,yield_mean, best_qtr_val).get_js();

			for(String myjs : jscpk2)
				final_report.write(myjs);

			
			final_report.write(yt.transpose(true,true).write().toString("html"));
			
//			final_report.write(((Table) yield_month.removeColumns("Year","Month")).write().toString("html"));
			
			final_report.write("</div>");



			/******************************************************************* 
			 * BIN PARETO (die level data)
			 *******************************************************************/
//			int y = LocalDate.now().getYear();
//			int m = LocalDate.now().getMonthValue();
//			int pm = m-1; // TODO across years...
//			String cur_month = "20217";
//			String prev_month = "20216";
//			String cur_month = Integer.toString(y) + Integer.toString(m);
//			String prev_month = Integer.toString(y) + Integer.toString(pm);
			
			String cur_month = "C30";
			String prev_month = "P30";
			
			String cur_week = "C7";
			String prev_week = "P7";
			
	
			String pareto_id =dev+"pareto"+count;
			final_report.write("<div id= "+pareto_id+" class=\"w3-half\">");
			
			
			Table binpareto_pm = get_bin_pareto(step, dev, prev_month,maxdpw);
			Table binpareto_cm = get_bin_pareto(step, dev, cur_month,maxdpw);
			
			Table binpareto_cw = get_bin_pareto(step, dev, cur_week,maxdpw);
			Table binpareto_pw = get_bin_pareto(step, dev, prev_week,maxdpw);
			

			Table binpareto = binpareto_pm.joinOn("bin").fullOuter(binpareto_cm);
			binpareto = binpareto.joinOn("bin").fullOuter(binpareto_pw);
			binpareto = binpareto.joinOn("bin").fullOuter(binpareto_cw);
			
			
			
			binpareto.columnsOfType(ColumnType.DOUBLE).forEach(x -> ((DoubleColumn)x).setMissingTo(0.0)); // zero to missing...
			Table origpareto = binpareto;

			binpareto=	binpareto.sortDescendingOn(cur_month).first(25);
//			binpareto = binpareto.where(binpareto.stringColumn("bin").isNotEqualTo(".. (1472)"));
			binpareto = binpareto.where(binpareto.stringColumn("bin").isNotIn(".. (1472)", ".* (1468)"));
			

			System.out.println(binpareto.print());

			
			Layout layout =
					Layout.builder()
					.title("Bin Pareto by Month")
					.barMode(Layout.BarMode.GROUP)
					.height(450)
					.width(640) //620
					.showLegend(true)
					.build();

			int dimension = 4;
			
			Trace[] traces = new Trace[dimension];
			String[] numberColNames = {prev_month, cur_month, prev_week, cur_week};
			String[] colors = {"#85144b", "#FF4136","#0000FF", "#008000"};
			for (int i = 0; i < dimension; i++) {
				String name = numberColNames[i];
				BarTrace trace =
						BarTrace.builder(binpareto.categoricalColumn("bin"), binpareto.numberColumn(name))
						.orientation(BarTrace.Orientation.VERTICAL)
						.marker(Marker.builder().color(colors[i]).build())
						.showLegend(true)
						.name(name)
						.build();
				traces[i] = trace;
			}

			
			

			String jsbareaccp= (new Figure(layout, traces)).asJavascript(pareto_id);
			final_report.write(jsbareaccp);
			
			final_report.write(jsbareaccp);

			final_report.write("</div>");
			final_report.write("</div>");



			/******************************************************************
			 *  PDCA Analysis
			 ****************************************************************/
			final_report.write("  <div class=\"w3-row-padding w3-padding-16 w3-center\" id=\"food\">\r\n");

			write_pdca_tail(final_report);

			String anal_id = dev+"Analysis" +count;
			final_report.write("<div id= "+anal_id+" class=\"w3-half\">");

			String cmys = "0";
			if(! probe.get_yield_by_period(p_con, step, dev,cur_month).isEmpty())
			  cmys = probe.get_yield_by_period(p_con, step, dev,cur_month).getString(0, 0);
			
			
			String pmys = "0";
			if(! probe.get_yield_by_period(p_con, step, dev,prev_month).isEmpty())
				pmys = probe.get_yield_by_period(p_con, step, dev,prev_month).getString(0, 0);
			
//			String pmys = probe.get_yield_by_month(p_con, step, dev,prev_month).getString(0, 0);

			if(cmys.isEmpty()) cmys = "0";
			if(pmys.isEmpty()) pmys = "0";
			
			double cmy = Double.valueOf(cmys)/maxdpw*100;
			double pmy = Double.valueOf(pmys)/maxdpw*100;
			double bestqtrpct = Double.valueOf(bestqtr.getString(0, 1));
			
			double delta_yield_m = cmy - pmy;
			
			String month_asses = (delta_yield_m > 0) ? "improving" : "worsening" ;

			double delta_yield_bq = cmy - bestqtrpct;
			String bq_asses = (delta_yield_bq >= 0) ? " so we are at the best yield ever (+" +  String.format("%.1f",Double.valueOf(delta_yield_bq))+"%)." : 
														"so we still need to recover " +  String.format("%.1f",Double.valueOf(delta_yield_bq))+"%.";
			

			final_report.write("<p style=\"text-align:left\" >");

			final_report.write("Yield is " + month_asses + " by "+ String.format("%.1f",delta_yield_m) + "%."+
					" Last 30 days yield is " + String.format("%.1f",Double.valueOf(cmy)) + "%" + 
					" vs previous  30 days yield at " + String.format("%.1f",Double.valueOf(pmy)) + "%. " + 
					" Best QTR performance was "  + String.format("%.1f",bestqtrpct) + "% (in "  + best_quarter+") " + 
					bq_asses
					);
			
			final_report.write("</p>");
			
			origpareto = origpareto.where(origpareto.stringColumn("bin").isNotEqualTo(".. (1472)"));

			DoubleColumn deltaM = origpareto.doubleColumn(cur_month)
					.subtract(origpareto.doubleColumn(prev_month))
					.setName("Delta Month");
			origpareto.addColumns(deltaM);
			
			
			DoubleColumn deltaW = origpareto.doubleColumn(cur_week)
					.subtract(origpareto.doubleColumn(prev_week))
					.setName("Delta Week");
			origpareto.addColumns(deltaW);

			

			origpareto
			.columnsOfType(ColumnType.DOUBLE)
			.forEach(x -> ((DoubleColumn)x).setPrintFormatter(NumberColumnFormatter.fixedWithGrouping(2)));


			Table monthpareto = origpareto.select("bin",prev_month, cur_month,"Delta Month");
			Table weekpareto = origpareto.select("bin",prev_week, cur_week,"Delta Week");
			
			final_report.write("  <div class=\"w3-row-padding w3-padding-4 w3-center\" id=\"food\">\r\n");
			final_report.write("<div id= "+anal_id+" class=\"w3-half\">");
			final_report.write("Month");
			final_report.write(monthpareto.sortDescendingOn("Delta Month").first(5).write().toString("html"));
			final_report.write("<br>");
			final_report.write(monthpareto.sortAscendingOn("Delta Month").first(5).write().toString("html"));
			final_report.write("</div>");

			final_report.write("<div id= "+anal_id+" class=\"w3-half\">");
			final_report.write("Week");
			final_report.write(weekpareto.sortDescendingOn("Delta Week").first(5).write().toString("html"));
			final_report.write("<br>");
			final_report.write(weekpareto.sortAscendingOn("Delta Week").first(5).write().toString("html"));
			final_report.write("</div>");
			final_report.write("</div>");

			//      Last sentence   //////////////////////////////////////////////////
			final_report.write("<p style=\"text-align:left\" >");
			Table last7high = weekpareto.where(weekpareto.doubleColumn("Delta Week").isGreaterThanOrEqualTo(0.5));
			Table lastMhigh = monthpareto.where(monthpareto.doubleColumn("Delta Month").isGreaterThanOrEqualTo(0.5));
			
			
			String lastmonthsentence = "";
			if(! lastMhigh.isEmpty()) 
				lastmonthsentence = "For the Month. What's going on for bins: " + lastMhigh.sortDescendingOn("Delta Month").stringColumn("bin").asList() +"? <br>";
			
			final_report.write(lastmonthsentence);
			
			
			
			String lastsentence = "";
			if(! last7high.isEmpty())
				lastsentence = "For the week. What's going on for bins: " + last7high.sortDescendingOn("Delta Week").stringColumn("bin").asList() +"?"; 
			final_report.write(lastsentence);
			
			final_report.write("Go deeper here: "+get_tex_html_link("../../YV4/"+dev+"_" +step+".html", "link"));
			
			final_report.write("</p>");
			////////////////////////////////////////////////////////////////
			
			
			final_report.write("</div>");


			
			/******************************************************************
			 *  PDCA  Actions...
			 ****************************************************************/
			String Actions_id = dev+"Actions" +count;
			final_report.write("<div id= "+Actions_id+" class=\"w3-half\">");
			String location = System.getProperty("user.dir")+"/ACTIONS/"+ dev.substring(0,4) +".xlsx";
			Table yield_ai = getTable(0, dev, location); //5
			final_report.write("<br>");
			final_report.write(yield_ai.write().toString("html"));
			final_report.write(get_tex_html_link(location, "see table"));
			final_report.write("</div>");

			final_report.write("</div>");

		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	
	
	
	
	
	public static String get_tex_html_link(String link, String name){

		String out = "<a href=\""+
				link + "\"" +
				" target=\"_blank\">"+
				name + 
				"</a>"+ 
				"\n";

		return out;
	}
	
	
	
}
